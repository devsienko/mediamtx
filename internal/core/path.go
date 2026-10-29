package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/cameraevent"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/formatlabel"
	"github.com/bluenviron/mediamtx/internal/forward"
	"github.com/bluenviron/mediamtx/internal/hooks"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/recorder"
	"github.com/bluenviron/mediamtx/internal/staticsources"
	"github.com/bluenviron/mediamtx/internal/stream"
)

func emptyTimer() *time.Timer {
	t := time.NewTimer(0)
	<-t.C
	return t
}

type pathParent interface {
	logger.Writer
	setPathReady(*path)
	setPathNotReady(*path)
	closePathIfIdle(*path)
	removePath(*path)
	AddReader(req defs.PathAddReaderReq) (*defs.PathAddReaderRes, error)
}

type pathOnDemandState int

const (
	pathOnDemandStateInitial pathOnDemandState = iota
	pathOnDemandStateWaitingReady
	pathOnDemandStateReady
	pathOnDemandStateClosing
)

type pathAPIPathsListRes struct {
	data  *defs.APIPathList
	paths map[string]*path
}

type pathAPIPathsListReq struct {
	res chan pathAPIPathsListRes
}

type pathAPIPathsGetRes struct {
	path *path
	data *defs.APIPath
	err  error
}

type pathAPIPathsGetReq struct {
	name string
	res  chan pathAPIPathsGetRes
}

type pathAPIForwardDestsListRes struct {
	path *path
	err  error
}

type pathAPIForwardDestsListReq struct {
	name string
	res  chan pathAPIForwardDestsListRes
}

type pathAPIForwardDestsGetRes struct {
	path *path
	err  error
}

type pathAPIForwardDestsGetReq struct {
	name string
	id   uuid.UUID
	res  chan pathAPIForwardDestsGetRes
}

type pathAPIStaticSourcesGetRes struct {
	path *path
	data *defs.APIStaticSource
	err  error
}

type pathAPIStaticSourcesGetReq struct {
	name string
	res  chan pathAPIStaticSourcesGetRes
}

const (
	cameraEventQueueSize          = 256
	cameraEventMaxStatesPerSource = 256
)

type cameraEventRuntimeConfig struct {
	endpoint string
	uuid     uuid.UUID
	timeout  time.Duration
}

type cameraEventDelivery struct {
	config  *cameraEventRuntimeConfig
	message cameraevent.Message
	source  *cameraEventSourceState
	key     cameraEventStateKey
	version uint64
}

type cameraEventStateKey struct {
	eventType  string
	objectType string
}

type cameraEventObservedState struct {
	active    bool
	timestamp int64
	config    *cameraEventRuntimeConfig
	version   uint64
	pending   bool
	delivered bool
}

type cameraEventSourceState struct {
	ctx              context.Context
	cancel           context.CancelFunc
	mutex            sync.Mutex
	states           map[cameraEventStateKey]*cameraEventObservedState
	stateLimitLogged bool
}

type path struct {
	parentCtx         context.Context
	logLevel          conf.LogLevel
	dumpPackets       bool
	rtspAddress       string
	readTimeout       conf.Duration
	writeTimeout      conf.Duration
	writeQueueSize    int
	udpReadBufferSize uint
	udpMaxPayloadSize int
	rtpMaxPayloadSize int
	supportsIPv6      bool
	conf              *conf.Path
	name              string
	matches           []string
	wg                *sync.WaitGroup
	externalCmdPool   *externalcmd.Pool
	parent            pathParent

	// accessed by pathManager only
	ready    bool
	confName string

	ctx                            context.Context
	ctxCancel                      func()
	pendingRequests                atomic.Int64
	confMutex                      sync.RWMutex
	source                         defs.Source
	stream                         *stream.Stream
	recorder                       *recorder.Recorder
	cameraEventConfig              atomic.Pointer[cameraEventRuntimeConfig]
	cameraEventSource              atomic.Pointer[cameraEventSourceState]
	cameraEventQueue               chan cameraEventDelivery
	cameraEventHTTPClient          *http.Client
	forwardManager                 *forward.Manager
	availableTime                  time.Time
	onlineTime                     time.Time
	onUnDemandHook                 func(string)
	onUnavailableHook              func()
	onOfflineHook                  func()
	readers                        map[defs.Reader]struct{}
	describeRequestsOnHold         []defs.PathDescribeReq
	readerAddRequestsOnHold        []defs.PathAddReaderReq
	onDemandStaticSourceState      pathOnDemandState
	onDemandStaticSourceReadyTimer *time.Timer
	onDemandStaticSourceCloseTimer *time.Timer
	onDemandPublisherState         pathOnDemandState
	onDemandPublisherReadyTimer    *time.Timer
	onDemandPublisherCloseTimer    *time.Timer

	// in
	chReloadConf              chan *conf.Path
	chStaticSourceSetReady    chan defs.PathSourceStaticSetReadyReq
	chStaticSourceSetNotReady chan defs.PathSourceStaticSetNotReadyReq
	chDescribe                chan defs.PathDescribeReq
	chAddPublisher            chan defs.PathAddPublisherReq
	chRemovePublisher         chan defs.PathRemovePublisherReq
	chAddReader               chan defs.PathAddReaderReq
	chRemoveReader            chan defs.PathRemoveReaderReq
	chAPIPathsGet             chan pathAPIPathsGetReq
	chAPIStaticSourcesGet     chan pathAPIStaticSourcesGetReq

	// out
	done chan struct{}
}

func (pa *path) storeCameraEventConfig(pathConf *conf.Path) *cameraEventRuntimeConfig {
	parsedUUID, err := uuid.Parse(pathConf.CameraEventSEIUUID)
	if err != nil || parsedUUID == uuid.Nil {
		// Configurations are validated before reaching a path. Keep a safe
		// fallback for tests that construct a path directly.
		parsedUUID = uuid.MustParse(cameraevent.DefaultUUID)
	}

	endpoint := ""
	if pathConf.CameraEventServiceURL != "" {
		endpoint, _ = cameraevent.ServiceEndpoint(pathConf.CameraEventServiceURL)
	}

	previous := pa.cameraEventConfig.Load()
	if previous != nil &&
		previous.endpoint == endpoint &&
		previous.uuid == parsedUUID &&
		previous.timeout == time.Duration(pathConf.CameraEventRequestTimeout) {
		return previous
	}

	next := &cameraEventRuntimeConfig{
		endpoint: endpoint,
		uuid:     parsedUUID,
		timeout:  time.Duration(pathConf.CameraEventRequestTimeout),
	}
	pa.cameraEventConfig.Store(next)
	return next
}

func (pa *path) newCameraEventSourceState() *cameraEventSourceState {
	ctx, cancel := context.WithCancel(pa.ctx)
	return &cameraEventSourceState{
		ctx:    ctx,
		cancel: cancel,
		states: make(map[cameraEventStateKey]*cameraEventObservedState),
	}
}

func (pa *path) activateCameraEventSource(source *cameraEventSourceState) {
	previous := pa.cameraEventSource.Swap(source)
	if previous != nil && previous != source {
		previous.cancel()
	}
}

func (pa *path) deactivateCameraEventSource() {
	previous := pa.cameraEventSource.Swap(nil)
	if previous != nil {
		previous.cancel()
	}
}

func (pa *path) initialize() {
	ctx, ctxCancel := context.WithCancel(pa.parentCtx)

	pa.confName = pa.conf.Name
	pa.ctx = ctx
	pa.ctxCancel = ctxCancel
	pa.storeCameraEventConfig(pa.conf)
	pa.cameraEventQueue = make(chan cameraEventDelivery, cameraEventQueueSize)
	pa.cameraEventHTTPClient = &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	pa.readers = make(map[defs.Reader]struct{})
	pa.onDemandStaticSourceReadyTimer = emptyTimer()
	pa.onDemandStaticSourceCloseTimer = emptyTimer()
	pa.onDemandPublisherReadyTimer = emptyTimer()
	pa.onDemandPublisherCloseTimer = emptyTimer()
	pa.chReloadConf = make(chan *conf.Path)
	pa.chStaticSourceSetReady = make(chan defs.PathSourceStaticSetReadyReq)
	pa.chStaticSourceSetNotReady = make(chan defs.PathSourceStaticSetNotReadyReq)
	pa.chDescribe = make(chan defs.PathDescribeReq)
	pa.chAddPublisher = make(chan defs.PathAddPublisherReq)
	pa.chRemovePublisher = make(chan defs.PathRemovePublisherReq)
	pa.chAddReader = make(chan defs.PathAddReaderReq)
	pa.chRemoveReader = make(chan defs.PathRemoveReaderReq)
	pa.chAPIPathsGet = make(chan pathAPIPathsGetReq)
	pa.chAPIStaticSourcesGet = make(chan pathAPIStaticSourcesGetReq)
	pa.done = make(chan struct{})

	pa.forwardManager = &forward.Manager{
		ReadTimeout:       pa.readTimeout,
		WriteTimeout:      pa.writeTimeout,
		UDPMaxPayloadSize: pa.udpMaxPayloadSize,
		PathName:          pa.name,
		Matches:           pa.matches,
		Forward:           pa.conf.Forward,
		Parent:            pa,
	}
	pa.forwardManager.Initialize()

	pa.Log(logger.Debug, "created")

	pa.wg.Add(1)
	go pa.run()

	pa.wg.Add(1)
	go pa.runCameraEventWorker()
}

func (pa *path) close() {
	pa.ctxCancel()
}

func (pa *path) wait() {
	<-pa.done
}

// Log implements logger.Writer.
func (pa *path) Log(level logger.Level, format string, args ...any) {
	pa.parent.Log(level, "[path "+pa.name+"] "+format, args...)
}

func (pa *path) Name() string {
	return pa.name
}

func (pa *path) isAvailable() bool {
	return pa.stream != nil
}

func (pa *path) isOnline() bool {
	return pa.onOfflineHook != nil || (pa.source != nil && !pa.conf.AlwaysAvailable)
}

func (pa *path) run() {
	defer close(pa.done)
	defer pa.wg.Done()

	if pa.conf.AlwaysAvailable {
		err := pa.setAvailable(nil, "", nil, true)
		if err != nil {
			panic(err)
		}
	}

	if pa.conf.Source == "redirect" {
		pa.source = &sourceRedirect{}
	} else if pa.conf.HasStaticSource() {
		pa.source = &staticsources.Handler{
			Conf:              pa.conf,
			LogLevel:          pa.logLevel,
			DumpPackets:       pa.dumpPackets,
			ReadTimeout:       pa.readTimeout,
			WriteTimeout:      pa.writeTimeout,
			WriteQueueSize:    pa.writeQueueSize,
			UDPReadBufferSize: pa.udpReadBufferSize,
			RTPMaxPayloadSize: pa.rtpMaxPayloadSize,
			SupportsIPv6:      pa.supportsIPv6,
			Matches:           pa.matches,
			PathManager:       pa.parent,
			Parent:            pa,
		}
		pa.source.(*staticsources.Handler).Initialize()

		if !pa.conf.SourceOnDemand {
			pa.source.(*staticsources.Handler).Start(false, "")
		}
	}

	onUnInitHook := hooks.OnInit(hooks.OnInitParams{
		Logger:          pa,
		ExternalCmdPool: pa.externalCmdPool,
		Conf:            pa.conf,
		ExternalCmdEnv:  pa.ExternalCmdEnv(),
	})

	err := pa.runInner()

	// call before destroying context
	pa.parent.removePath(pa)

	pa.ctxCancel()

	pa.onDemandStaticSourceReadyTimer.Stop()
	pa.onDemandStaticSourceCloseTimer.Stop()
	pa.onDemandPublisherReadyTimer.Stop()
	pa.onDemandPublisherCloseTimer.Stop()

	onUnInitHook()

	for _, req := range pa.describeRequestsOnHold {
		req.Res <- defs.PathDescribeRes{Err: fmt.Errorf("terminated")}
	}

	for _, req := range pa.readerAddRequestsOnHold {
		req.Res <- defs.PathAddReaderRes{Err: fmt.Errorf("terminated")}
	}

	if pa.source != nil {
		if source, ok := pa.source.(*staticsources.Handler); ok {
			if !pa.conf.SourceOnDemand || pa.onDemandStaticSourceState != pathOnDemandStateInitial {
				source.Close("path is closing")
			}
		} else if source, ok2 := pa.source.(defs.Publisher); ok2 {
			source.Close()
		}
	}

	if pa.onUnDemandHook != nil {
		pa.onUnDemandHook("path destroyed")
	}

	if pa.stream != nil {
		pa.setNotAvailable()
	}

	pa.Log(logger.Debug, "destroyed: %v", err)
}

func (pa *path) runInner() error {
	for {
		select {
		case <-pa.onDemandStaticSourceReadyTimer.C:
			pa.doOnDemandStaticSourceReadyTimer()

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case <-pa.onDemandStaticSourceCloseTimer.C:
			pa.doOnDemandStaticSourceCloseTimer()

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case <-pa.onDemandPublisherReadyTimer.C:
			pa.doOnDemandPublisherReadyTimer()

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case <-pa.onDemandPublisherCloseTimer.C:
			pa.doOnDemandPublisherCloseTimer()

		case newConf := <-pa.chReloadConf:
			pa.doReloadConf(newConf)

		case req := <-pa.chStaticSourceSetReady:
			pa.doSourceStaticSetReady(req)

		case req := <-pa.chStaticSourceSetNotReady:
			pa.doSourceStaticSetNotReady(req)

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case req := <-pa.chDescribe:
			pa.doDescribe(req)

			pa.pendingRequests.Add(-1)

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case req := <-pa.chAddPublisher:
			pa.doAddPublisher(req)

			pa.pendingRequests.Add(-1)

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case req := <-pa.chRemovePublisher:
			pa.doRemovePublisher(req)

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case req := <-pa.chAddReader:
			pa.doAddReader(req)

			pa.pendingRequests.Add(-1)

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case req := <-pa.chRemoveReader:
			pa.doRemoveReader(req)

			if pa.shouldClose() {
				pa.parent.closePathIfIdle(pa)
			}

		case req := <-pa.chAPIPathsGet:
			pa.doAPIPathsGet(req)

		case req := <-pa.chAPIStaticSourcesGet:
			pa.doAPIStaticSourcesGet(req)

		case <-pa.ctx.Done():
			return fmt.Errorf("terminated")
		}
	}
}

func (pa *path) doOnDemandStaticSourceReadyTimer() {
	for _, req := range pa.describeRequestsOnHold {
		req.Res <- defs.PathDescribeRes{Err: fmt.Errorf("source of path '%s' has timed out", pa.name)}
	}
	pa.describeRequestsOnHold = nil

	for _, req := range pa.readerAddRequestsOnHold {
		req.Res <- defs.PathAddReaderRes{Err: fmt.Errorf("source of path '%s' has timed out", pa.name)}
	}
	pa.readerAddRequestsOnHold = nil

	pa.onDemandStaticSourceStop("timed out")
}

func (pa *path) doOnDemandStaticSourceCloseTimer() {
	if pa.conf.AlwaysAvailable {
		panic("should not happen")
	}
	pa.setNotAvailable()
	pa.onDemandStaticSourceStop("not needed by anyone")
}

func (pa *path) doOnDemandPublisherReadyTimer() {
	for _, req := range pa.describeRequestsOnHold {
		req.Res <- defs.PathDescribeRes{Err: fmt.Errorf("source of path '%s' has timed out", pa.name)}
	}
	pa.describeRequestsOnHold = nil

	for _, req := range pa.readerAddRequestsOnHold {
		req.Res <- defs.PathAddReaderRes{Err: fmt.Errorf("source of path '%s' has timed out", pa.name)}
	}
	pa.readerAddRequestsOnHold = nil

	pa.onDemandPublisherStop("timed out")
}

func (pa *path) doOnDemandPublisherCloseTimer() {
	pa.onDemandPublisherStop("not needed by anyone")
}

func (pa *path) doReloadConf(newConf *conf.Path) {
	recordSettingsChanged :=
		newConf.RecordPath != pa.conf.RecordPath ||
			newConf.RecordFormat != pa.conf.RecordFormat ||
			newConf.RecordPartDuration != pa.conf.RecordPartDuration ||
			newConf.RecordMaxPartSize != pa.conf.RecordMaxPartSize ||
			newConf.RecordSegmentDuration != pa.conf.RecordSegmentDuration ||
			newConf.RecordDeleteAfter != pa.conf.RecordDeleteAfter

	pa.confMutex.Lock()
	pa.conf = newConf
	pa.confMutex.Unlock()
	pa.storeCameraEventConfig(newConf)

	if pa.conf.HasStaticSource() {
		pa.source.(*staticsources.Handler).ReloadConf(newConf)
	}

	pa.forwardManager.ReloadConf(newConf.Forward)

	pa.reconcileRecording(recordSettingsChanged, true)
}

func (pa *path) doSourceStaticSetReady(req defs.PathSourceStaticSetReadyReq) {
	if !pa.conf.AlwaysAvailable {
		err := pa.setAvailable(pa.source, "", req.Desc, req.ReplaceNTP)
		if err != nil {
			req.Res <- defs.PathSourceStaticSetReadyRes{Err: err}
			return
		}
	}

	eventSource := pa.newCameraEventSourceState()
	subStream := &stream.SubStream{
		Stream:        pa.stream,
		UseRTPPackets: req.UseRTPPackets,
		OnDataH264: func(au [][]byte) ([][]byte, func()) {
			return pa.onDataH264(eventSource, au)
		},
	}

	if pa.conf.AlwaysAvailable {
		subStream.InDesc = req.Desc
	}

	err := subStream.Initialize()
	if err != nil {
		eventSource.cancel()
		req.Res <- defs.PathSourceStaticSetReadyRes{Err: err}
		return
	}

	if pa.conf.AlwaysAvailable {
		pa.setOnline(pa.source.APISourceDescribe(), "")
	}
	if pa.conf.HasOnDemandStaticSource() {
		pa.onDemandStaticSourceReadyTimer.Stop()
		pa.onDemandStaticSourceReadyTimer = emptyTimer()
		pa.onDemandStaticSourceScheduleClose()
	}

	pa.activateCameraEventSource(eventSource)
	pa.consumeOnHoldRequests()

	req.Res <- defs.PathSourceStaticSetReadyRes{SubStream: subStream}
}

func (pa *path) doSourceStaticSetNotReady(req defs.PathSourceStaticSetNotReadyReq) {
	if !pa.conf.AlwaysAvailable {
		pa.setNotAvailable()
	} else {
		pa.setOffline()

		err := pa.stream.StartOfflineSubStream()
		if err != nil {
			panic("should not happen")
		}
	}

	// send response before calling onDemandStaticSourceStop()
	// in order to avoid a deadlock due to staticsources.Handler.stop()
	close(req.Res)

	if pa.conf.HasOnDemandStaticSource() && pa.onDemandStaticSourceState != pathOnDemandStateInitial {
		pa.onDemandStaticSourceStop("an error occurred")
	}
}

func (pa *path) doDescribe(req defs.PathDescribeReq) {
	if _, ok := pa.source.(*sourceRedirect); ok {
		req.Res <- defs.PathDescribeRes{
			Redirect: pa.conf.SourceRedirect,
		}
		return
	}

	if pa.stream != nil {
		req.Res <- defs.PathDescribeRes{
			Stream: pa.stream,
		}
		return
	}

	if pa.conf.HasOnDemandStaticSource() {
		if pa.onDemandStaticSourceState == pathOnDemandStateInitial {
			pa.onDemandStaticSourceStart(req.AccessRequest.Query)
		}
		pa.describeRequestsOnHold = append(pa.describeRequestsOnHold, req)
		return
	}

	if pa.conf.HasOnDemandPublisher() {
		if pa.onDemandPublisherState == pathOnDemandStateInitial {
			pa.onDemandPublisherStart(req.AccessRequest.Query)
		}
		pa.describeRequestsOnHold = append(pa.describeRequestsOnHold, req)
		return
	}

	if pa.conf.Fallback != nil {
		req.Res <- defs.PathDescribeRes{Redirect: *pa.conf.Fallback}
		return
	}

	req.Res <- defs.PathDescribeRes{Err: &defs.PathNoStreamAvailableError{PathName: pa.name}}
}

func (pa *path) doRemovePublisher(req defs.PathRemovePublisherReq) {
	if pa.source == req.Author {
		pa.executeRemovePublisher()
	}
	close(req.Res)
}

func (pa *path) doAddPublisher(req defs.PathAddPublisherReq) {
	if pa.conf.Source != "publisher" {
		req.Res <- defs.PathAddPublisherRes{
			Err: fmt.Errorf("can't publish to path '%s' since 'source' is not 'publisher'", pa.name),
		}
		return
	}

	if pa.source != nil {
		if !pa.conf.OverridePublisher {
			req.Res <- defs.PathAddPublisherRes{Err: fmt.Errorf("someone is already publishing to path '%s'", pa.name)}
			return
		}

		pa.Log(logger.Info, "closing existing publisher")
		pa.source.(defs.Publisher).Close()
		pa.executeRemovePublisher()
	}

	if !pa.conf.AlwaysAvailable {
		err := pa.setAvailable(req.Author, req.AccessRequest.Query, req.Desc, req.ReplaceNTP)
		if err != nil {
			req.Res <- defs.PathAddPublisherRes{Err: err}
			return
		}
	}

	eventSource := pa.newCameraEventSourceState()
	subStream := &stream.SubStream{
		Stream:        pa.stream,
		UseRTPPackets: req.UseRTPPackets,
		OnDataH264: func(au [][]byte) ([][]byte, func()) {
			return pa.onDataH264(eventSource, au)
		},
	}

	if pa.conf.AlwaysAvailable {
		subStream.InDesc = req.Desc
	}

	err := subStream.Initialize()
	if err != nil {
		eventSource.cancel()
		req.Res <- defs.PathAddPublisherRes{Err: err}
		return
	}

	pa.source = req.Author

	req.Author.Log(logger.Info, "is publishing to path '%s'",
		pa.name)

	if pa.conf.AlwaysAvailable {
		pa.setOnline(req.Author.APISourceDescribe(), req.AccessRequest.Query)
	}
	if pa.conf.HasOnDemandPublisher() && pa.onDemandPublisherState != pathOnDemandStateInitial {
		pa.onDemandPublisherReadyTimer.Stop()
		pa.onDemandPublisherReadyTimer = emptyTimer()
		pa.onDemandPublisherScheduleClose()
	}

	pa.activateCameraEventSource(eventSource)
	pa.consumeOnHoldRequests()

	req.Res <- defs.PathAddPublisherRes{SubStream: subStream}
}

func (pa *path) doAddReader(req defs.PathAddReaderReq) {
	if pa.stream != nil {
		pa.addReaderPost(req)
		return
	}

	if pa.conf.HasOnDemandStaticSource() {
		if pa.onDemandStaticSourceState == pathOnDemandStateInitial {
			pa.onDemandStaticSourceStart(req.AccessRequest.Query)
		}
		pa.readerAddRequestsOnHold = append(pa.readerAddRequestsOnHold, req)
		return
	}

	if pa.conf.HasOnDemandPublisher() {
		if pa.onDemandPublisherState == pathOnDemandStateInitial {
			pa.onDemandPublisherStart(req.AccessRequest.Query)
		}
		pa.readerAddRequestsOnHold = append(pa.readerAddRequestsOnHold, req)
		return
	}

	req.Res <- defs.PathAddReaderRes{Err: &defs.PathNoStreamAvailableError{PathName: pa.name}}
}

func (pa *path) doRemoveReader(req defs.PathRemoveReaderReq) {
	if _, ok := pa.readers[req.Author]; ok {
		pa.executeRemoveReader(req.Author)
	}
	close(req.Res)

	if len(pa.readers) == 0 {
		if pa.conf.HasOnDemandStaticSource() {
			if pa.onDemandStaticSourceState == pathOnDemandStateReady {
				pa.onDemandStaticSourceScheduleClose()
			}
		} else if pa.conf.HasOnDemandPublisher() {
			if pa.onDemandPublisherState == pathOnDemandStateReady {
				pa.onDemandPublisherScheduleClose()
			}
		}
	}
}

func (pa *path) doAPIStaticSourcesGet(req pathAPIStaticSourcesGetReq) {
	source, ok := pa.source.(*staticsources.Handler)
	if !ok {
		req.res <- pathAPIStaticSourcesGetRes{err: staticsources.ErrNoStaticSource}
		return
	}

	req.res <- pathAPIStaticSourcesGetRes{data: source.APIItem()}
}

func (pa *path) doAPIPathsGet(req pathAPIPathsGetReq) {
	var outDesc *description.Session
	if pa.isAvailable() {
		outDesc = pa.stream.OutDescCopy()
	}

	req.res <- pathAPIPathsGetRes{
		data: &defs.APIPath{
			Name:     pa.name,
			ConfName: pa.conf.Name,
			Ready:    pa.isAvailable(),
			ReadyTime: func() *time.Time {
				if !pa.isAvailable() {
					return nil
				}
				v := pa.availableTime
				return &v
			}(),
			Available: pa.isAvailable(),
			AvailableTime: func() *time.Time {
				if !pa.isAvailable() {
					return nil
				}
				v := pa.availableTime
				return &v
			}(),
			Online: pa.isOnline(),
			OnlineTime: func() *time.Time {
				if !pa.isOnline() {
					return nil
				}
				v := pa.onlineTime
				return &v
			}(),
			Source: func() *defs.APIPathSource {
				if pa.source == nil {
					return nil
				}
				v := pa.source.APISourceDescribe()
				return v
			}(),
			Tracks: func() []defs.APIPathTrackCodec {
				if !pa.isAvailable() {
					return []defs.APIPathTrackCodec{}
				}
				return formatlabel.MediasToLabels(outDesc.Medias)
			}(),
			Tracks2: func() []defs.APIPathTrack {
				if !pa.isAvailable() {
					return []defs.APIPathTrack{}
				}
				return defs.MediasToTracks(outDesc.Medias)
			}(),
			InboundBytes: func() uint64 {
				if !pa.isAvailable() {
					return 0
				}
				return pa.stream.InboundBytes()
			}(),
			OutboundBytes: func() uint64 {
				if !pa.isAvailable() {
					return 0
				}
				return pa.stream.OutboundBytes()
			}(),
			InboundFramesInError: func() uint64 {
				if !pa.isAvailable() {
					return 0
				}
				return pa.stream.InboundFramesInError()
			}(),
			BytesReceived: func() uint64 {
				if !pa.isAvailable() {
					return 0
				}
				return pa.stream.InboundBytes()
			}(),
			BytesSent: func() uint64 {
				if !pa.isAvailable() {
					return 0
				}
				return pa.stream.OutboundBytes()
			}(),
			Readers: func() []defs.APIPathReader {
				ret := make([]defs.APIPathReader, 0, len(pa.readers))

				for r := range pa.readers {
					desc := *r.APIReaderDescribe()
					if desc.Type != defs.APIPathReaderTypeHidden {
						ret = append(ret, desc)
					}
				}

				sort.Slice(ret, func(i, j int) bool {
					if ret[i].Type != ret[j].Type {
						return ret[i].Type < ret[j].Type
					}
					return ret[i].ID < ret[j].ID
				})

				return ret
			}(),
		},
	}
}

func (pa *path) SafeConf() *conf.Path {
	pa.confMutex.RLock()
	defer pa.confMutex.RUnlock()
	return pa.conf
}

func (pa *path) ExternalCmdEnv() externalcmd.Environment {
	_, port, _ := net.SplitHostPort(pa.rtspAddress)
	env := externalcmd.Environment{
		"MTX_PATH":  pa.name,
		"RTSP_PATH": pa.name, // deprecated
		"RTSP_PORT": port,
	}

	if len(pa.matches) > 1 {
		for i, ma := range pa.matches[1:] {
			env["G"+strconv.FormatInt(int64(i+1), 10)] = ma
		}
	}

	return env
}

func (pa *path) shouldClose() bool {
	return pa.conf.Regexp != nil &&
		pa.source == nil &&
		len(pa.readers) == 0 &&
		len(pa.describeRequestsOnHold) == 0 &&
		len(pa.readerAddRequestsOnHold) == 0
}

func (pa *path) onDemandStaticSourceStart(query string) {
	pa.source.(*staticsources.Handler).Start(true, query)

	pa.onDemandStaticSourceReadyTimer.Stop()
	pa.onDemandStaticSourceReadyTimer = time.NewTimer(time.Duration(pa.conf.SourceOnDemandStartTimeout))

	pa.onDemandStaticSourceState = pathOnDemandStateWaitingReady
}

func (pa *path) onDemandStaticSourceScheduleClose() {
	pa.onDemandStaticSourceCloseTimer.Stop()
	pa.onDemandStaticSourceCloseTimer = time.NewTimer(time.Duration(pa.conf.SourceOnDemandCloseAfter))

	pa.onDemandStaticSourceState = pathOnDemandStateClosing
}

func (pa *path) onDemandStaticSourceStop(reason string) {
	if pa.onDemandStaticSourceState == pathOnDemandStateClosing {
		pa.onDemandStaticSourceCloseTimer.Stop()
		pa.onDemandStaticSourceCloseTimer = emptyTimer()
	}

	pa.onDemandStaticSourceState = pathOnDemandStateInitial

	pa.source.(*staticsources.Handler).Stop(reason)
}

func (pa *path) onDemandPublisherStart(query string) {
	pa.onUnDemandHook = hooks.OnDemand(hooks.OnDemandParams{
		Logger:          pa,
		ExternalCmdPool: pa.externalCmdPool,
		Conf:            pa.conf,
		ExternalCmdEnv:  pa.ExternalCmdEnv(),
		Query:           query,
	})

	pa.onDemandPublisherReadyTimer.Stop()
	pa.onDemandPublisherReadyTimer = time.NewTimer(time.Duration(pa.conf.RunOnDemandStartTimeout))

	pa.onDemandPublisherState = pathOnDemandStateWaitingReady
}

func (pa *path) onDemandPublisherScheduleClose() {
	pa.onDemandPublisherCloseTimer.Stop()
	pa.onDemandPublisherCloseTimer = time.NewTimer(time.Duration(pa.conf.RunOnDemandCloseAfter))

	pa.onDemandPublisherState = pathOnDemandStateClosing
}

func (pa *path) onDemandPublisherStop(reason string) {
	if pa.onDemandPublisherState == pathOnDemandStateClosing {
		pa.onDemandPublisherCloseTimer.Stop()
		pa.onDemandPublisherCloseTimer = emptyTimer()
	}

	pa.onUnDemandHook(reason)
	pa.onUnDemandHook = nil

	pa.onDemandPublisherState = pathOnDemandStateInitial
}

func (pa *path) setOnline(sourceDesc *defs.APIPathSource, publisherQuery string) {
	pa.setOffline()

	pa.onOfflineHook = hooks.OnOnline(hooks.OnOnlineParams{
		Logger:          pa,
		ExternalCmdPool: pa.externalCmdPool,
		Conf:            pa.conf,
		ExternalCmdEnv:  pa.ExternalCmdEnv(),
		Desc:            sourceDesc,
		Query:           publisherQuery,
	})

	pa.onlineTime = time.Now()
	pa.reconcileRecording(false, true)
}

func (pa *path) setOffline() {
	pa.deactivateCameraEventSource()

	if pa.onOfflineHook != nil {
		pa.onOfflineHook()
		pa.onOfflineHook = nil
	}

	pa.reconcileRecording(false, true)
}

func (pa *path) setAvailable(
	source defs.Source,
	publisherQuery string,
	desc *description.Session,
	replaceNTP bool,
) error {
	pa.stream = &stream.Stream{
		OrigDesc:              desc,
		AlwaysAvailable:       pa.conf.AlwaysAvailable,
		AlwaysAvailableTracks: pa.conf.AlwaysAvailableTracks,
		AlwaysAvailableFile:   pa.conf.AlwaysAvailableFile,
		WriteQueueSize:        pa.writeQueueSize,
		RTPMaxPayloadSize:     pa.rtpMaxPayloadSize,
		ReplaceNTP:            replaceNTP,
		Parent:                pa,
	}
	err := pa.stream.Initialize()
	if err != nil {
		return err
	}

	pa.availableTime = time.Now()

	var sourceDesc *defs.APIPathSource
	if source != nil {
		sourceDesc = source.APISourceDescribe()
	}

	pa.onUnavailableHook = hooks.OnAvailable(hooks.OnAvailableParams{
		Logger:          pa,
		ExternalCmdPool: pa.externalCmdPool,
		Conf:            pa.conf,
		ExternalCmdEnv:  pa.ExternalCmdEnv(),
		Desc:            sourceDesc,
		Query:           publisherQuery,
	})

	if !pa.conf.AlwaysAvailable {
		pa.setOnline(sourceDesc, publisherQuery)
	}

	pa.forwardManager.Start(pa.stream)

	pa.reconcileRecording(false, true)

	if pa.conf.AlwaysAvailable {
		pa.Log(logger.Info, "stream is available, %s", defs.MediasInfo(pa.stream.OrigDesc.Medias))
	} else {
		pa.Log(logger.Info, "stream is available and online, %s", defs.MediasInfo(pa.stream.OrigDesc.Medias))
	}

	pa.parent.setPathReady(pa)

	return nil
}

func (pa *path) consumeOnHoldRequests() {
	for _, req := range pa.describeRequestsOnHold {
		req.Res <- defs.PathDescribeRes{
			Stream: pa.stream,
		}
	}
	pa.describeRequestsOnHold = nil

	for _, req := range pa.readerAddRequestsOnHold {
		pa.addReaderPost(req)
	}
	pa.readerAddRequestsOnHold = nil
}

func (pa *path) setNotAvailable() {
	pa.parent.setPathNotReady(pa)
	pa.setOffline()

	for r := range pa.readers {
		pa.executeRemoveReader(r)
		r.Close()
	}

	pa.onUnavailableHook()

	pa.stopRecording(true)

	pa.forwardManager.Stop()

	if pa.stream != nil {
		pa.stream.Close()
		pa.stream = nil
	}
}

func (pa *path) configWantsRecording() bool {
	return pa.conf.Record && pa.stream != nil &&
		(!pa.conf.AlwaysAvailable || pa.conf.AlwaysAvailableRecorded || pa.isOnline())
}

func (pa *path) reconcileRecording(forceRestart bool, graceful bool) {
	wantRecording := pa.configWantsRecording()

	if forceRestart && pa.recorder != nil {
		pa.stopRecording(graceful)
	}

	if !wantRecording {
		pa.stopRecording(graceful)
		return
	}

	if pa.recorder == nil {
		pa.startRecording()
	}
}

func (pa *path) stopRecording(graceful bool) {
	if pa.recorder == nil {
		return
	}

	if graceful {
		pa.recorder.CloseGracefully()
	} else {
		pa.recorder.Close()
	}
	pa.recorder = nil
}

func (pa *path) startRecording() {
	if pa.recorder != nil || pa.stream == nil {
		return
	}

	pa.recorder = &recorder.Recorder{
		PathFormat:      pa.conf.RecordPath,
		Format:          pa.conf.RecordFormat,
		PartDuration:    time.Duration(pa.conf.RecordPartDuration),
		MaxPartSize:     pa.conf.RecordMaxPartSize,
		SegmentDuration: time.Duration(pa.conf.RecordSegmentDuration),
		PathName:        pa.name,
		Stream:          pa.stream,
		OnSegmentCreate: func(segmentPath string) {
			pathConf := pa.SafeConf()
			if pathConf.RunOnRecordSegmentCreate != "" {
				env := pa.ExternalCmdEnv()
				env["MTX_SEGMENT_PATH"] = segmentPath

				pa.Log(logger.Info, "runOnRecordSegmentCreate command launched")
				cmd := &externalcmd.Cmd{
					Pool:    pa.externalCmdPool,
					Cmdstr:  pathConf.RunOnRecordSegmentCreate,
					Restart: false,
					Env:     env,
				}
				cmd.Start()
			}
		},
		OnSegmentComplete: func(segmentPath string, segmentDuration time.Duration) {
			pathConf := pa.SafeConf()
			if pathConf.RunOnRecordSegmentComplete != "" {
				env := pa.ExternalCmdEnv()
				env["MTX_SEGMENT_PATH"] = segmentPath
				env["MTX_SEGMENT_DURATION"] = strconv.FormatFloat(segmentDuration.Seconds(), 'f', -1, 64)

				pa.Log(logger.Info, "runOnRecordSegmentComplete command launched")
				cmd := &externalcmd.Cmd{
					Pool:    pa.externalCmdPool,
					Cmdstr:  pathConf.RunOnRecordSegmentComplete,
					Restart: false,
					Env:     env,
				}
				cmd.Start()
			}
		},
		Parent: pa,
	}
	pa.recorder.Initialize()
}

func (pa *path) onDataH264(source *cameraEventSourceState, au [][]byte) ([][]byte, func()) {
	config := pa.cameraEventConfig.Load()
	if config == nil || config.endpoint == "" {
		return au, nil
	}

	message, filteredAU, ok := cameraevent.ParseAndRemoveH264(au, config.uuid)
	if !ok {
		return au, nil
	}

	key := cameraEventStateKey{
		eventType:  message.Event.EventType,
		objectType: message.Event.ObjectType,
	}
	active := message.Event.EventPhase == "Start"

	source.mutex.Lock()
	defer source.mutex.Unlock()

	if pa.cameraEventSource.Load() != source || source.ctx.Err() != nil {
		return filteredAU, nil
	}

	state, exists := source.states[key]
	if exists && message.Event.Timestamp < state.timestamp {
		pa.Log(logger.Debug,
			"outdated camera event ignored: type=%s object=%s timestamp=%d current=%d",
			key.eventType, key.objectType, message.Event.Timestamp, state.timestamp)
		return filteredAU, nil
	}

	if !exists {
		if len(source.states) >= cameraEventMaxStatesPerSource {
			if !source.stateLimitLogged {
				pa.Log(logger.Warn, "camera event state limit reached; additional event types are ignored")
				source.stateLimitLogged = true
			}
			return filteredAU, nil
		}

		state = &cameraEventObservedState{
			active:    active,
			timestamp: message.Event.Timestamp,
			config:    config,
			version:   1,
		}
		source.states[key] = state
	} else if state.active != active || state.config != config {
		state.active = active
		state.timestamp = message.Event.Timestamp
		state.config = config
		state.version++
		state.pending = false
		state.delivered = false
	} else if message.Event.Timestamp > state.timestamp {
		state.timestamp = message.Event.Timestamp
	}

	if state.pending || state.delivered {
		return filteredAU, nil
	}

	delivery := cameraEventDelivery{
		config:  config,
		message: message,
		source:  source,
		key:     key,
		version: state.version,
	}
	select {
	case pa.cameraEventQueue <- delivery:
		state.pending = true
		pa.Log(logger.Debug, "camera event queued: id=%s phase=%s",
			message.Event.EventID, message.Event.EventPhase)
	default:
		pa.Log(logger.Warn, "camera event dropped: delivery queue is full")
	}

	return filteredAU, nil
}

func (delivery *cameraEventDelivery) complete(success bool) {
	delivery.source.mutex.Lock()
	defer delivery.source.mutex.Unlock()

	state, ok := delivery.source.states[delivery.key]
	if !ok || state.version != delivery.version {
		return
	}

	state.pending = false
	if success {
		state.delivered = true
	}
}

func (pa *path) runCameraEventWorker() {
	defer pa.wg.Done()

	for {
		select {
		case delivery := <-pa.cameraEventQueue:
			if delivery.source.ctx.Err() != nil {
				delivery.complete(false)
				continue
			}

			err := pa.deliverCameraEvent(delivery)
			delivery.complete(err == nil)
			if err != nil && delivery.source.ctx.Err() == nil {
				pa.Log(logger.Error, "camera event delivery failed: %v", err)
			}

		case <-pa.ctx.Done():
			return
		}
	}
}

func (pa *path) deliverCameraEvent(delivery cameraEventDelivery) error {
	payload, err := json.Marshal(delivery.message)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		requestCtx, cancel := context.WithTimeout(delivery.source.ctx, delivery.config.timeout)
		req, err := http.NewRequestWithContext(
			requestCtx,
			http.MethodPost,
			delivery.config.endpoint,
			bytes.NewReader(payload),
		)
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "MediaMTX-camera-event/1")

		res, err := pa.cameraEventHTTPClient.Do(req)
		if err != nil {
			lastErr = err
		} else {
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()

			if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices {
				cancel()
				pa.Log(logger.Debug, "camera event delivered: id=%s phase=%s",
					delivery.message.Event.EventID, delivery.message.Event.EventPhase)
				return nil
			}

			lastErr = fmt.Errorf("service returned %s", res.Status)
			if res.StatusCode < http.StatusInternalServerError &&
				res.StatusCode != http.StatusRequestTimeout &&
				res.StatusCode != http.StatusTooManyRequests {
				cancel()
				return lastErr
			}
		}
		cancel()

		if attempt < 2 {
			retryDelay := time.Duration(attempt+1) * 250 * time.Millisecond
			select {
			case <-time.After(retryDelay):
			case <-delivery.source.ctx.Done():
				return delivery.source.ctx.Err()
			}
		}
	}

	return lastErr
}

func (pa *path) executeRemoveReader(r defs.Reader) {
	delete(pa.readers, r)
}

func (pa *path) executeRemovePublisher() {
	if !pa.conf.AlwaysAvailable {
		pa.setNotAvailable()
	} else {
		pa.setOffline()

		err := pa.stream.StartOfflineSubStream()
		if err != nil {
			panic("should not happen")
		}
	}
	pa.source = nil
}

func (pa *path) addReaderPost(req defs.PathAddReaderReq) {
	if _, ok := pa.readers[req.Author]; ok {
		req.Res <- defs.PathAddReaderRes{Stream: pa.stream}
		return
	}

	if pa.conf.MaxReaders != 0 && len(pa.readers) >= pa.conf.MaxReaders {
		req.Res <- defs.PathAddReaderRes{Err: fmt.Errorf("maximum reader count reached")}
		return
	}

	pa.readers[req.Author] = struct{}{}

	if pa.conf.HasOnDemandStaticSource() {
		if pa.onDemandStaticSourceState == pathOnDemandStateClosing {
			pa.onDemandStaticSourceState = pathOnDemandStateReady
			pa.onDemandStaticSourceCloseTimer.Stop()
			pa.onDemandStaticSourceCloseTimer = emptyTimer()
		}
	} else if pa.conf.HasOnDemandPublisher() {
		if pa.onDemandPublisherState == pathOnDemandStateClosing {
			pa.onDemandPublisherState = pathOnDemandStateReady
			pa.onDemandPublisherCloseTimer.Stop()
			pa.onDemandPublisherCloseTimer = emptyTimer()
		}
	}

	req.Res <- defs.PathAddReaderRes{Stream: pa.stream}
}

// reloadConf is called by pathManager.
func (pa *path) reloadConf(newConf *conf.Path) {
	select {
	case pa.chReloadConf <- newConf:
	case <-pa.ctx.Done():
	}
}

// StaticSourceHandlerSetReady is called by staticsources.Handler.
func (pa *path) StaticSourceHandlerSetReady(
	ctx context.Context, req defs.PathSourceStaticSetReadyReq,
) {
	select {
	case pa.chStaticSourceSetReady <- req:

	case <-pa.ctx.Done():
		req.Res <- defs.PathSourceStaticSetReadyRes{Err: fmt.Errorf("terminated")}

	// this avoids:
	// - invalid requests sent after the source has been terminated
	// - deadlocks caused by <-done inside stop()
	case <-ctx.Done():
		req.Res <- defs.PathSourceStaticSetReadyRes{Err: fmt.Errorf("terminated")}
	}
}

// StaticSourceHandlerSetNotReady is called by staticsources.Handler.
func (pa *path) StaticSourceHandlerSetNotReady(
	ctx context.Context, req defs.PathSourceStaticSetNotReadyReq,
) {
	select {
	case pa.chStaticSourceSetNotReady <- req:

	case <-pa.ctx.Done():
		close(req.Res)

	// this avoids:
	// - invalid requests sent after the source has been terminated
	// - deadlocks caused by <-done inside stop()
	case <-ctx.Done():
		close(req.Res)
	}
}

// describe is called by a reader or publisher through pathManager.
func (pa *path) describe(req defs.PathDescribeReq) (*defs.PathDescribeRes, error) {
	select {
	case pa.chDescribe <- req:
		res := <-req.Res
		return &res, res.Err

	case <-pa.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// addPublisher is called by a publisher through pathManager.
func (pa *path) addPublisher(req defs.PathAddPublisherReq) (*defs.PathAddPublisherRes, error) {
	select {
	case pa.chAddPublisher <- req:
		res := <-req.Res
		return &res, res.Err
	case <-pa.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// RemovePublisher is called by a publisher.
func (pa *path) RemovePublisher(req defs.PathRemovePublisherReq) {
	req.Res = make(chan struct{})
	select {
	case pa.chRemovePublisher <- req:
		<-req.Res
	case <-pa.ctx.Done():
	}
}

// addReader is called by a reader through pathManager.
func (pa *path) addReader(req defs.PathAddReaderReq) (*defs.PathAddReaderRes, error) {
	select {
	case pa.chAddReader <- req:
		res := <-req.Res
		return &res, res.Err
	case <-pa.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// RemoveReader is called by a reader.
func (pa *path) RemoveReader(req defs.PathRemoveReaderReq) {
	req.Res = make(chan struct{})
	select {
	case pa.chRemoveReader <- req:
		<-req.Res
	case <-pa.ctx.Done():
	}
}

// APIPathsGet implements defs.APIPathManager.
func (pa *path) APIPathsGet(req pathAPIPathsGetReq) (*defs.APIPath, error) {
	req.res = make(chan pathAPIPathsGetRes)
	select {
	case pa.chAPIPathsGet <- req:
		res := <-req.res
		return res.data, res.err

	case <-pa.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

func (pa *path) APIForwardDestsList() *defs.APIForwardDestList {
	return pa.forwardManager.APIList()
}

func (pa *path) APIForwardDestsGet(destID uuid.UUID) (*defs.APIForwardDest, error) {
	return pa.forwardManager.APIGet(destID)
}

func (pa *path) APIStaticSourcesGet(req pathAPIStaticSourcesGetReq) (*defs.APIStaticSource, error) {
	req.res = make(chan pathAPIStaticSourcesGetRes)
	select {
	case pa.chAPIStaticSourcesGet <- req:
		res := <-req.res
		return res.data, res.err

	case <-pa.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}
