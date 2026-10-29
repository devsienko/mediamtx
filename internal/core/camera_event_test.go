package core

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/cameraevent"
	"github.com/bluenviron/mediamtx/internal/test"
)

func encodeCameraEventSEIValue(v int) []byte {
	var out []byte
	for v >= 255 {
		out = append(out, 0xff)
		v -= 255
	}
	return append(out, byte(v))
}

func addCameraEventEmulationPrevention(data []byte) []byte {
	out := make([]byte, 0, len(data))
	zeroCount := 0
	for _, b := range data {
		if zeroCount >= 2 && b <= 0x03 {
			out = append(out, 0x03)
			zeroCount = 0
		}
		out = append(out, b)
		if b == 0 {
			zeroCount++
		} else {
			zeroCount = 0
		}
	}
	return out
}

func cameraEventSEI(t *testing.T, payload any) []byte {
	t.Helper()

	metadata, err := json.Marshal(payload)
	require.NoError(t, err)

	id := uuid.MustParse(cameraevent.DefaultUUID)
	data := append(append([]byte(nil), id[:]...), metadata...)
	rbsp := append([]byte{5}, encodeCameraEventSEIValue(len(data))...)
	rbsp = append(rbsp, data...)
	rbsp = append(rbsp, 0x80)
	return append([]byte{0x06}, addCameraEventEmulationPrevention(rbsp)...)
}

func cameraEventSTAPA(nalus ...[]byte) []byte {
	out := []byte{24}
	for _, nalu := range nalus {
		out = append(out, byte(len(nalu)>>8), byte(len(nalu)))
		out = append(out, nalu...)
	}
	return out
}

func newCameraEventPublisher(t *testing.T) (*gortsplib.Client, func([]byte)) {
	t.Helper()

	medi := test.UniqueMediaH264()
	client := &gortsplib.Client{}
	err := client.StartRecording(
		"rtsp://localhost:8554/test",
		&description.Session{Medias: []*description.Media{medi}})
	require.NoError(t, err)

	sequence := uint16(1000)
	timestamp := uint32(90000)
	return client, func(payload []byte) {
		err = client.WritePacketRTP(medi, &rtp.Packet{
			Version:        2,
			Marker:         true,
			PayloadType:    96,
			SequenceNumber: sequence,
			Timestamp:      timestamp,
			SSRC:           123456,
			Payload:        payload,
		})
		require.NoError(t, err)
		sequence++
		timestamp += 3000
	}
}

func TestCameraEventForwarding(t *testing.T) {
	requests := make(chan []byte, 1)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/camera-event", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		requests <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"paths:\n"+
			"  test:\n"+
			"    cameraEventServiceURL: "+server.URL+"\n"+
			"    cameraEventRequestTimeout: 1s\n")
	require.Equal(t, true, ok)
	defer p.Close()

	client, writeRTP := newCameraEventPublisher(t)
	defer client.Close()

	// The legacy envelope must not be forwarded anymore.
	writeRTP(cameraEventSTAPA(
		[]byte{0x09, 0xf0},
		cameraEventSEI(t, map[string]any{
			"schema": "faceter.video-metadata.v1",
			"type":   "camera.event",
		}),
		[]byte{0x65, 0x01},
	))

	expected := cameraevent.Message{Event: cameraevent.Event{
		EventID:    uuid.MustParse("8e86225f-6644-4daf-b150-a5fda0ba72d0"),
		Timestamp:  1793145600,
		EventPhase: "Start",
		EventType:  "Motion",
		ObjectType: "Human",
	}}
	writeRTP(cameraEventSTAPA(
		[]byte{0x09, 0xf0},
		cameraEventSEI(t, expected),
		[]byte{0x65, 0x02},
	))

	select {
	case body := <-requests:
		var actual cameraevent.Message
		require.NoError(t, json.Unmarshal(body, &actual))
		require.Equal(t, expected, actual)
	case <-time.After(5 * time.Second):
		t.Fatal("camera event was not delivered")
	}
	require.Equal(t, int32(2), attempts.Load(), "5xx responses must be retried")
}

func TestCameraEventDisabledWithoutServiceURL(t *testing.T) {
	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"paths:\n"+
			"  test:\n")
	require.Equal(t, true, ok)
	defer p.Close()

	client, writeRTP := newCameraEventPublisher(t)
	defer client.Close()
	writeRTP(cameraEventSTAPA(
		[]byte{0x09, 0xf0},
		cameraEventSEI(t, cameraevent.Message{Event: cameraevent.Event{
			EventID:    uuid.New(),
			Timestamp:  time.Now().Unix(),
			EventPhase: "Start",
			EventType:  "Motion",
			ObjectType: "Human",
		}}),
		[]byte{0x65, 0x01},
	))
}
