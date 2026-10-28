// Package recorder contains the recorder.
package recorder

import (
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/stream"
)

const (
	ntpDriftTolerance = 5 * time.Second
)

// OnSegmentCreateFunc is the prototype of the function passed as OnSegmentCreate
type OnSegmentCreateFunc = func(path string)

// OnSegmentCompleteFunc is the prototype of the function passed as OnSegmentComplete
type OnSegmentCompleteFunc = func(path string, duration time.Duration)

// Recorder writes recordings to disk.
type Recorder struct {
	PathFormat        string
	Format            conf.RecordFormat
	PartDuration      time.Duration
	MaxPartSize       conf.StringSize
	SegmentDuration   time.Duration
	PathName          string
	Stream            *stream.Stream
	OnSegmentCreate   OnSegmentCreateFunc
	OnSegmentComplete OnSegmentCompleteFunc
	Parent            logger.Writer

	restartPause time.Duration
	location     *time.Location

	currentInstance *recorderInstance

	terminate chan bool
	flush     chan chan struct{}
	done      chan struct{}
}

// Initialize initializes Recorder.
func (r *Recorder) Initialize() {
	if r.OnSegmentCreate == nil {
		r.OnSegmentCreate = func(string) {
		}
	}
	if r.OnSegmentComplete == nil {
		r.OnSegmentComplete = func(string, time.Duration) {
		}
	}
	if r.restartPause == 0 {
		r.restartPause = 2 * time.Second
	}
	if r.location == nil {
		r.location = time.Local
	}

	r.terminate = make(chan bool)
	r.flush = make(chan chan struct{})
	r.done = make(chan struct{})

	r.currentInstance = &recorderInstance{
		pathFormat:        r.PathFormat,
		format:            r.Format,
		partDuration:      r.PartDuration,
		maxPartSize:       r.MaxPartSize,
		segmentDuration:   r.SegmentDuration,
		pathName:          r.PathName,
		stream:            r.Stream,
		onSegmentCreate:   r.OnSegmentCreate,
		onSegmentComplete: r.OnSegmentComplete,
		location:          r.location,
		parent:            r,
	}
	r.currentInstance.initialize()

	go r.run()
}

// Log implements logger.Writer.
func (r *Recorder) Log(level logger.Level, format string, args ...any) {
	r.Parent.Log(level, "[recorder] "+format, args...)
}

// Close closes the agent.
func (r *Recorder) Close() {
	r.Log(logger.Info, "recording stopped")
	select {
	case r.terminate <- false:
	case <-r.done:
	}
	<-r.done
}

// CloseGracefully closes the recorder after processing all queued units.
func (r *Recorder) CloseGracefully() {
	r.Log(logger.Info, "recording stopped")
	select {
	case r.terminate <- true:
	case <-r.done:
	}
	<-r.done
}

// Flush waits until all units currently queued by the stream have been processed.
func (r *Recorder) Flush() {
	done := make(chan struct{})
	select {
	case r.flush <- done:
	case <-r.done:
		return
	}

	select {
	case <-done:
	case <-r.done:
	}
}

func (r *Recorder) run() {
	defer close(r.done)

	for {
		select {
		case <-r.currentInstance.done:
			r.currentInstance.close(false)
		case gracefully := <-r.terminate:
			r.currentInstance.close(gracefully)
			return
		case done := <-r.flush:
			r.currentInstance.flush()
			close(done)
			continue
		}

		timer := time.NewTimer(r.restartPause)
	waitRestart:
		for {
			select {
			case <-timer.C:
				break waitRestart
			case <-r.terminate:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case done := <-r.flush:
				close(done)
			}
		}

		r.currentInstance = &recorderInstance{
			pathFormat:        r.PathFormat,
			format:            r.Format,
			partDuration:      r.PartDuration,
			maxPartSize:       r.MaxPartSize,
			segmentDuration:   r.SegmentDuration,
			pathName:          r.PathName,
			stream:            r.Stream,
			onSegmentCreate:   r.OnSegmentCreate,
			onSegmentComplete: r.OnSegmentComplete,
			location:          r.location,
			parent:            r,
		}
		r.currentInstance.initialize()
	}
}
