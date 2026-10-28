package stream

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReaderWaitUntilEmpty(t *testing.T) {
	r := &Reader{
		Parent:    nilLogger{},
		queueSize: 1,
	}
	r.start()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondProcessed := make(chan struct{})

	r.push(func() error {
		close(firstStarted)
		<-releaseFirst
		return nil
	})
	<-firstStarted

	// Fill the queue while the first callback is blocked. WaitUntilEmpty must
	// wait for room for its barrier, and then for both callbacks preceding it.
	r.push(func() error {
		close(secondProcessed)
		return nil
	})

	waitResult := make(chan bool)
	go func() {
		waitResult <- r.WaitUntilEmpty()
	}()

	select {
	case <-waitResult:
		t.Fatal("reader reported an empty queue while a callback was blocked")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	<-secondProcessed
	require.True(t, <-waitResult)
	r.stop()
}

func TestReaderWaitUntilEmptyAfterError(t *testing.T) {
	r := &Reader{
		Parent:    nilLogger{},
		queueSize: 1,
	}
	r.start()

	r.push(func() error {
		return errors.New("test error")
	})

	<-r.runDone
	require.False(t, r.WaitUntilEmpty())
	r.stop()
}
