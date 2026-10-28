package core

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/archivecontrol"
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

type archiveControlRuntimeConfig struct {
	enabled            bool
	uuid               uuid.UUID
	maxAge             time.Duration
	minRestartInterval time.Duration
}

type archiveControlSource struct {
	generation uint64
	done       chan struct{}
}

type archiveControlRequestPhase int

const (
	archiveControlRequestStart archiveControlRequestPhase = iota
	archiveControlRequestStopPrepare
	archiveControlRequestStopCommit
)

type archiveControlRequest struct {
	phase      archiveControlRequestPhase
	source     *archiveControlSource
	config     *archiveControlRuntimeConfig
	metadata   archivecontrol.Metadata
	receivedAt time.Time
	res        chan bool
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
	archiveControlConfig           atomic.Pointer[archiveControlRuntimeConfig]
	archiveControlGeneration       uint64
	archiveControlSource           *archiveControlSource
	archiveControlLastSequence     uint64
	archiveControlRecordGeneration uint64
	archiveControlPendingStop      uint64
	archiveControlNextStart        time.Time
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
	chArchiveControl          chan archiveControlRequest

	// out
	done chan struct{}
}

func (pa *path) storeArchiveControlConfig(pathConf *conf.Path) *archiveControlRuntimeConfig {
	parsedUUID, err := uuid.Parse(pathConf.ArchiveControlUUID)
	if err != nil || parsedUUID == uuid.Nil {
		// Configurations are validated before reaching a path. Keep a safe
		// fallback for tests that construct a path directly.
		parsedUUID = uuid.MustParse(archivecontrol.DefaultUUID)
	}

	previous := pa.archiveControlConfig.Load()
	if previous != nil &&
		previous.enabled == pathConf.ArchiveControl &&
		previous.uuid == parsedUUID &&
		previous.maxAge == time.Duration(pathConf.ArchiveControlMaxAge) &&
		previous.minRestartInterval == time.Duration(pathConf.ArchiveControlMinRestartInterval) {
		return previous
	}

	next := &archiveControlRuntimeConfig{
		enabled:            pathConf.ArchiveControl,
		uuid:               parsedUUID,
		maxAge:             time.Duration(pathConf.ArchiveControlMaxAge),
		minRestartInterval: time.Duration(pathConf.ArchiveControlMinRestartInterval),
	}
	pa.archiveControlConfig.Store(next)
	return next
}

func (pa *path) initialize() {
	ctx, ctxCancel := context.WithCancel(pa.parentCtx)

	pa.confName = pa.conf.Name
	pa.ctx = ctx
	pa.ctxCancel = ctxCancel
	pa.storeArchiveControlConfig(pa.conf)
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
	pa.chArchiveControl = make(chan archiveControlRequest)
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
	pa.deactivateArchiveControlSource()

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

		case req := <-pa.chArchiveControl:
			pa.doArchiveControl(req)

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
	oldArchiveControlConfig := pa.archiveControlConfig.Load()
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
	newArchiveControlConfig := pa.storeArchiveControlConfig(newConf)

	if oldArchiveControlConfig != newArchiveControlConfig {
		pa.archiveControlLastSequence = 0
		pa.archiveControlNextStart = time.Time{}

		// A UUID change or disabling the protocol invalidates the previous
		// controller. This prevents a SEI-only recording from becoming
		// impossible to stop.
		if !newArchiveControlConfig.enabled ||
			(oldArchiveControlConfig != nil && oldArchiveControlConfig.uuid != newArchiveControlConfig.uuid) {
			pa.archiveControlPendingStop = 0
			pa.archiveControlRecordGeneration = 0
		}
	}

	if pa.conf.HasStaticSource() {
		pa.source.(*staticsources.Handler).ReloadConf(newConf)
	}

	pa.forwardManager.ReloadConf(newConf.Forward)

	pa.reconcileRecording(recordSettingsChanged, true)
}

func (pa *path) newArchiveControlSource() *archiveControlSource {
	pa.archiveControlGeneration++
	return &archiveControlSource{
		generation: pa.archiveControlGeneration,
		done:       make(chan struct{}),
	}
}

func (pa *path) activateArchiveControlSource(source *archiveControlSource) {
	pa.deactivateArchiveControlSource()
	pa.archiveControlSource = source
	pa.archiveControlLastSequence = 0
	pa.archiveControlPendingStop = 0
	pa.archiveControlNextStart = time.Time{}
}

func (pa *path) deactivateArchiveControlSource() {
	if pa.archiveControlSource != nil {
		close(pa.archiveControlSource.done)
		pa.archiveControlSource = nil
	}

	pa.archiveControlLastSequence = 0
	pa.archiveControlPendingStop = 0
	pa.archiveControlRecordGeneration = 0
	pa.archiveControlNextStart = time.Time{}
	pa.reconcileRecording(false, true)
}

func (pa *path) doSourceStaticSetReady(req defs.PathSourceStaticSetReadyReq) {
	if !pa.conf.AlwaysAvailable {
		err := pa.setAvailable(pa.source, "", req.Desc, req.ReplaceNTP)
		if err != nil {
			req.Res <- defs.PathSourceStaticSetReadyRes{Err: err}
			return
		}
	}

	controlSource := pa.newArchiveControlSource()
	subStream := &stream.SubStream{
		Stream:        pa.stream,
		UseRTPPackets: req.UseRTPPackets,
		OnDataH264: func(au [][]byte) ([][]byte, func()) {
			return pa.onDataH264(controlSource, au)
		},
	}

	if pa.conf.AlwaysAvailable {
		subStream.InDesc = req.Desc
	}

	err := subStream.Initialize()
	if err != nil {
		close(controlSource.done)
		req.Res <- defs.PathSourceStaticSetReadyRes{Err: err}
		return
	}

	if pa.conf.AlwaysAvailable {
		pa.setOnline(pa.source.APISourceDescribe(), "")
	}
	pa.activateArchiveControlSource(controlSource)

	if pa.conf.HasOnDemandStaticSource() {
		pa.onDemandStaticSourceReadyTimer.Stop()
		pa.onDemandStaticSourceReadyTimer = emptyTimer()
		pa.onDemandStaticSourceScheduleClose()
	}

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
		pa.deactivateArchiveControlSource()
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

	controlSource := pa.newArchiveControlSource()
	subStream := &stream.SubStream{
		Stream:        pa.stream,
		UseRTPPackets: req.UseRTPPackets,
		OnDataH264: func(au [][]byte) ([][]byte, func()) {
			return pa.onDataH264(controlSource, au)
		},
	}

	if pa.conf.AlwaysAvailable {
		subStream.InDesc = req.Desc
	}

	err := subStream.Initialize()
	if err != nil {
		close(controlSource.done)
		req.Res <- defs.PathAddPublisherRes{Err: err}
		return
	}

	pa.source = req.Author

	req.Author.Log(logger.Info, "is publishing to path '%s'",
		pa.name)

	if pa.conf.AlwaysAvailable {
		pa.setOnline(req.Author.APISourceDescribe(), req.AccessRequest.Query)
	}
	pa.activateArchiveControlSource(controlSource)

	if pa.conf.HasOnDemandPublisher() && pa.onDemandPublisherState != pathOnDemandStateInitial {
		pa.onDemandPublisherReadyTimer.Stop()
		pa.onDemandPublisherReadyTimer = emptyTimer()
		pa.onDemandPublisherScheduleClose()
	}

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

	pa.deactivateArchiveControlSource()
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
	pa.deactivateArchiveControlSource()

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

func (pa *path) archiveControlWantsRecording() bool {
	config := pa.archiveControlConfig.Load()
	return config != nil && config.enabled &&
		pa.stream != nil && pa.isOnline() &&
		pa.archiveControlSource != nil &&
		pa.archiveControlRecordGeneration == pa.archiveControlSource.generation
}

func (pa *path) reconcileRecording(forceRestart bool, graceful bool) {
	wantRecording := pa.configWantsRecording() || pa.archiveControlWantsRecording()

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

func (pa *path) sendArchiveControlRequest(req archiveControlRequest) bool {
	select {
	case pa.chArchiveControl <- req:
	case <-req.source.done:
		return false
	case <-pa.ctx.Done():
		return false
	}

	select {
	case accepted := <-req.res:
		return accepted
	case <-req.source.done:
		return false
	case <-pa.ctx.Done():
		return false
	}
}

func (pa *path) onDataH264(source *archiveControlSource, au [][]byte) ([][]byte, func()) {
	config := pa.archiveControlConfig.Load()
	if config == nil || !config.enabled {
		return au, nil
	}

	metadata, filteredAU, ok := archivecontrol.ParseAndRemoveH264(au, config.uuid)
	if !ok {
		return au, nil
	}

	req := archiveControlRequest{
		source:     source,
		config:     config,
		metadata:   metadata,
		receivedAt: time.Now(),
		res:        make(chan bool, 1),
	}

	switch metadata.Command {
	case archivecontrol.CommandStart:
		req.phase = archiveControlRequestStart
		pa.sendArchiveControlRequest(req)
		return filteredAU, nil

	case archivecontrol.CommandStop:
		req.phase = archiveControlRequestStopPrepare
		if !pa.sendArchiveControlRequest(req) {
			return filteredAU, nil
		}

		return filteredAU, func() {
			commitReq := req
			commitReq.phase = archiveControlRequestStopCommit
			commitReq.res = make(chan bool, 1)
			pa.sendArchiveControlRequest(commitReq)
		}

	default:
		return filteredAU, nil
	}
}

func (pa *path) doArchiveControl(req archiveControlRequest) {
	accepted := false
	defer func() {
		req.res <- accepted
	}()

	if req.phase == archiveControlRequestStopCommit {
		if req.source != pa.archiveControlSource ||
			pa.archiveControlPendingStop != req.metadata.Sequence {
			return
		}

		pa.archiveControlPendingStop = 0
		pa.archiveControlRecordGeneration = 0
		config := pa.archiveControlConfig.Load()
		if config != nil {
			pa.archiveControlNextStart = time.Now().Add(config.minRestartInterval)
		}
		pa.reconcileRecording(false, true)
		pa.Log(logger.Info, "recording stopped by H264 SEI command")
		accepted = true
		return
	}

	currentConfig := pa.archiveControlConfig.Load()
	if req.config != currentConfig || currentConfig == nil || !currentConfig.enabled ||
		req.source != pa.archiveControlSource || pa.stream == nil || !pa.isOnline() {
		return
	}

	if req.metadata.Stream != pa.name || req.metadata.Sequence == 0 ||
		req.metadata.Sequence <= pa.archiveControlLastSequence {
		return
	}

	commandTime, err := time.Parse(time.RFC3339Nano, req.metadata.Timestamp)
	if err != nil {
		return
	}

	if currentConfig.maxAge > 0 {
		age := req.receivedAt.Sub(commandTime)
		if age < 0 {
			age = -age
		}
		if age > currentConfig.maxAge {
			pa.Log(logger.Warn, "archive control command ignored: timestamp is outside the allowed window")
			return
		}
	}

	expectedCommand := archivecontrol.CommandStart
	if req.phase == archiveControlRequestStopPrepare {
		expectedCommand = archivecontrol.CommandStop
	}
	if req.metadata.Command != expectedCommand {
		return
	}

	pa.archiveControlLastSequence = req.metadata.Sequence

	switch req.phase {
	case archiveControlRequestStart:
		if pa.archiveControlRecordGeneration == req.source.generation {
			accepted = true
			return
		}
		if req.receivedAt.Before(pa.archiveControlNextStart) {
			pa.Log(logger.Warn, "archive.start SEI ignored: start rate limit exceeded")
			return
		}

		pa.archiveControlRecordGeneration = req.source.generation
		pa.archiveControlPendingStop = 0
		pa.reconcileRecording(false, true)
		pa.Log(logger.Info, "recording started by H264 SEI command")
		accepted = true

	case archiveControlRequestStopPrepare:
		if pa.archiveControlRecordGeneration != req.source.generation {
			return
		}

		pa.archiveControlPendingStop = req.metadata.Sequence
		if pa.recorder != nil && !pa.configWantsRecording() {
			if pa.conf.RecordFormat == conf.RecordFormatMPEGTS {
				// MPEG-TS writes each access unit immediately, therefore the
				// recorder must be detached before the exclusive stop boundary.
				pa.stopRecording(true)
			} else {
				// fMP4 needs the stop access unit as lookahead in order to derive
				// the duration of the preceding sample.
				pa.recorder.Flush()
			}
		}
		accepted = true
	}
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
