package core

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4"
	"github.com/google/uuid"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/archivecontrol"
	"github.com/bluenviron/mediamtx/internal/test"
)

func encodeSEIValueForTest(v int) []byte {
	var out []byte
	for v >= 255 {
		out = append(out, 0xff)
		v -= 255
	}
	return append(out, byte(v))
}

func addH264EmulationPreventionForTest(data []byte) []byte {
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

func archiveControlSEIForTest(t *testing.T, command string, sequence uint64, streamName string) []byte {
	return archiveControlSEIAtForTest(
		t,
		command,
		sequence,
		streamName,
		time.Now().UTC().Format(time.RFC3339Nano))
}

func archiveControlSEIAtForTest(
	t *testing.T,
	command string,
	sequence uint64,
	streamName string,
	timestamp string,
) []byte {
	t.Helper()

	metadata, err := json.Marshal(map[string]any{
		"schema":    archivecontrol.Schema,
		"command":   command,
		"timestamp": timestamp,
		"sequence":  sequence,
		"stream":    streamName,
	})
	require.NoError(t, err)

	id := uuid.MustParse(archivecontrol.DefaultUUID)
	payload := append(append([]byte{}, id[:]...), metadata...)
	rbsp := append([]byte{5}, encodeSEIValueForTest(len(payload))...)
	rbsp = append(rbsp, payload...)
	rbsp = append(rbsp, 0x80)

	return append([]byte{0x06}, addH264EmulationPreventionForTest(rbsp)...)
}

func archiveControlRecordedMarkers(t *testing.T, recDir string) map[byte]bool {
	t.Helper()

	var found map[byte]bool
	hasControlSEI := false
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(recDir)
		if err != nil || len(entries) == 0 {
			return false
		}

		candidate := make(map[byte]bool)
		sampleCount := 0
		candidateHasControlSEI := false
		for _, entry := range entries {
			byts, err := os.ReadFile(filepath.Join(recDir, entry.Name()))
			if err != nil {
				return false
			}

			var parts fmp4.Parts
			if parts.Unmarshal(byts) != nil {
				return false
			}
			for _, part := range parts {
				for _, track := range part.Tracks {
					for _, sample := range track.Samples {
						sampleCount++
						var avcc h264.AVCC
						if avcc.Unmarshal(sample.Payload) != nil {
							return false
						}
						for _, nalu := range avcc {
							if len(nalu) != 0 && nalu[0]&0x1f == 6 {
								candidateHasControlSEI = true
							}
							if len(nalu) == 2 && nalu[0] == 0x65 {
								candidate[nalu[1]] = true
							}
						}
					}
				}
			}
		}

		if sampleCount == 0 {
			return false
		}
		found = candidate
		hasControlSEI = candidateHasControlSEI
		return true
	}, 5*time.Second, 50*time.Millisecond)
	require.False(t, hasControlSEI, "control SEI must be consumed")
	return found
}

func h264STAPAForTest(nalus ...[]byte) []byte {
	out := []byte{24}
	for _, nalu := range nalus {
		out = append(out, byte(len(nalu)>>8), byte(len(nalu)))
		out = append(out, nalu...)
	}
	return out
}

func newArchiveControlPublisher(t *testing.T, pathName string) (*gortsplib.Client, func([]byte)) {
	t.Helper()

	medi := test.UniqueMediaH264()
	client := &gortsplib.Client{}
	err := client.StartRecording(
		"rtsp://localhost:8554/"+pathName,
		&description.Session{Medias: []*description.Media{medi}})
	require.NoError(t, err)

	seq := uint16(1000)
	ts := uint32(90000)
	return client, func(nalu []byte) {
		err = client.WritePacketRTP(medi, &rtp.Packet{
			Version:        2,
			Marker:         true,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      ts,
			SSRC:           123456,
			Payload:        nalu,
		})
		require.NoError(t, err)
		seq++
		ts += 90000
	}
}

func TestArchiveControlRecording(t *testing.T) {
	dir := t.TempDir()
	recDir := filepath.Join(dir, "test")

	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  recordPath: "+filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n"+
			"    archiveControl: true\n"+
			"    archiveControlMaxAge: 1m\n")
	require.True(t, ok)
	defer p.Close()

	medi := test.UniqueMediaH264()
	client := &gortsplib.Client{}
	err := client.StartRecording(
		"rtsp://localhost:8554/test",
		&description.Session{Medias: []*description.Media{medi}})
	require.NoError(t, err)
	defer client.Close()

	seq := uint16(1000)
	ts := uint32(90000)
	writeNAL := func(nalu []byte) {
		err = client.WritePacketRTP(medi, &rtp.Packet{
			Version:        2,
			Marker:         true,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      ts,
			SSRC:           123456,
			Payload:        nalu,
		})
		require.NoError(t, err)
		seq++
		ts += 90000
	}

	writeNAL([]byte{0x65, 0x10}) // before start: excluded
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1}, // start boundary: included
	))
	writeNAL([]byte{0x65, 0xb2}) // included
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.stop", 2, "test"),
		[]byte{0x65, 0xc3}, // stop boundary: lookahead, excluded
	))
	writeNAL([]byte{0x65, 0xd4}) // after stop: excluded

	found := archiveControlRecordedMarkers(t, recDir)

	require.True(t, found[0xa1], "start access unit must be included")
	require.True(t, found[0xb2], "access unit before stop must be included")
	require.False(t, found[0x10], "access unit before start must be excluded")
	require.False(t, found[0xc3], "stop access unit must be excluded")
	require.False(t, found[0xd4], "access unit after stop must be excluded")
}

func TestArchiveControlDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	recDir := filepath.Join(dir, "test")

	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  recordPath: "+filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n")
	require.True(t, ok)
	defer p.Close()

	client, writeNAL := newArchiveControlPublisher(t, "test")
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1},
	))
	writeNAL([]byte{0x65, 0xb2})
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.stop", 2, "test"),
		[]byte{0x65, 0xc3},
	))
	client.Close()

	time.Sleep(200 * time.Millisecond)
	count, _ := recordedFiles(t, recDir)
	require.Zero(t, count)
}

func TestArchiveControlValidationAndSequence(t *testing.T) {
	dir := t.TempDir()
	recDir := filepath.Join(dir, "test")

	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  recordPath: "+filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n"+
			"    archiveControl: true\n"+
			"    archiveControlMaxAge: 1m\n")
	require.True(t, ok)
	defer p.Close()

	client, writeNAL := newArchiveControlPublisher(t, "test")
	defer client.Close()

	writeNAL(h264STAPAForTest(
		archiveControlSEIAtForTest(t, "archive.start", 100, "test", time.Now().Add(-time.Hour).Format(time.RFC3339Nano)),
		[]byte{0x65, 0x11},
	))
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 99, "another-path"),
		[]byte{0x65, 0x12},
	))
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1},
	))
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 3, "test"),
		[]byte{0x65, 0xa3},
	))
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.stop", 2, "test"),
		[]byte{0x65, 0xc2},
	))
	writeNAL([]byte{0x65, 0xb2})
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.stop", 4, "test"),
		[]byte{0x65, 0xc4},
	))

	found := archiveControlRecordedMarkers(t, recDir)
	require.False(t, found[0x11], "stale timestamp must be rejected")
	require.False(t, found[0x12], "wrong stream must be rejected")
	require.True(t, found[0xa1])
	require.True(t, found[0xa3])
	require.True(t, found[0xc2], "out-of-order stop must not stop recording")
	require.True(t, found[0xb2], "recording must continue after an out-of-order stop")
	require.False(t, found[0xc4], "accepted stop is an exclusive boundary")
}

func TestArchiveControlSurvivesRecordPathReload(t *testing.T) {
	dir := t.TempDir()
	oldRoot := filepath.Join(dir, "old")
	newRoot := filepath.Join(dir, "new")

	p, ok := newInstance(t,
		"api: yes\n"+
			"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  recordPath: "+filepath.Join(oldRoot, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n"+
			"    archiveControl: true\n")
	require.True(t, ok)
	defer p.Close()

	client, writeNAL := newArchiveControlPublisher(t, "test")
	defer client.Close()
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1},
	))
	writeNAL([]byte{0x65, 0xb1})

	hc := &http.Client{}
	httpRequest(t, hc, http.MethodPatch, "http://localhost:9997/v3/config/paths/patch/test", map[string]any{
		"recordPath": filepath.Join(newRoot, "%path/%Y-%m-%d_%H-%M-%S-%f"),
	}, nil)
	require.True(t, archiveControlRecordedMarkers(t, filepath.Join(oldRoot, "test"))[0xa1])

	writeNAL([]byte{0x65, 0xa2})
	writeNAL([]byte{0x65, 0xb2})
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.stop", 2, "test"),
		[]byte{0x65, 0xc2},
	))

	found := archiveControlRecordedMarkers(t, filepath.Join(newRoot, "test"))
	require.True(t, found[0xa2], "SEI recording intent must survive a recorder settings reload")
	require.True(t, found[0xb2])
	require.False(t, found[0xc2])
}

func TestArchiveControlHotEnableAndDisable(t *testing.T) {
	dir := t.TempDir()
	recDir := filepath.Join(dir, "test")

	p, ok := newInstance(t,
		"api: yes\n"+
			"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  recordPath: "+filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n")
	require.True(t, ok)
	defer p.Close()

	client, writeNAL := newArchiveControlPublisher(t, "test")
	defer client.Close()
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 100, "test"),
		[]byte{0x65, 0x10},
	))

	hc := &http.Client{}
	httpRequest(t, hc, http.MethodPatch, "http://localhost:9997/v3/config/paths/patch/test", map[string]any{
		"archiveControl": true,
	}, nil)
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1},
	))
	writeNAL([]byte{0x65, 0xb1})

	httpRequest(t, hc, http.MethodPatch, "http://localhost:9997/v3/config/paths/patch/test", map[string]any{
		"archiveControl": false,
	}, nil)
	found := archiveControlRecordedMarkers(t, recDir)
	require.True(t, found[0xa1])
	require.False(t, found[0x10], "command sent while disabled must be ignored")

	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 2, "test"),
		[]byte{0x65, 0xa2},
	))
	writeNAL([]byte{0x65, 0xb2})
	time.Sleep(200 * time.Millisecond)
	count, _ := recordedFiles(t, recDir)
	require.Equal(t, 1, count, "commands sent after hot disable must be ignored")
}

func TestArchiveControlStopDoesNotDisableConfiguredRecording(t *testing.T) {
	dir := t.TempDir()
	recDir := filepath.Join(dir, "test")

	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  record: true\n"+
			"  recordPath: "+filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n"+
			"    archiveControl: true\n")
	require.True(t, ok)
	defer p.Close()

	client, writeNAL := newArchiveControlPublisher(t, "test")
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1},
	))
	writeNAL([]byte{0x65, 0xb1})
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.stop", 2, "test"),
		[]byte{0x65, 0xc2},
	))
	writeNAL([]byte{0x65, 0xb2})
	writeNAL([]byte{0x65, 0xb3})
	// Unlike a SEI-controlled stop, configured recording is finalized only
	// when the publisher disconnects. Let the RTSP server consume the writes
	// before closing the client transport.
	time.Sleep(500 * time.Millisecond)
	client.Close()

	found := archiveControlRecordedMarkers(t, recDir)
	require.True(t, found[0xc2], "configured recording must include frames at the SEI stop point")
	require.True(t, found[0xb2], "configured recording must continue after SEI stop")
}

func TestArchiveControlStopsWhenAlwaysAvailablePublisherLeaves(t *testing.T) {
	dir := t.TempDir()
	recDir := filepath.Join(dir, "test")

	p, ok := newInstance(t,
		"rtmp: no\n"+
			"hls: no\n"+
			"webrtc: no\n"+
			"srt: no\n"+
			"moq: no\n"+
			"pathDefaults:\n"+
			"  recordPath: "+filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f")+"\n"+
			"  recordDeleteAfter: 0s\n"+
			"paths:\n"+
			"  test:\n"+
			"    archiveControl: true\n"+
			"    alwaysAvailable: true\n"+
			"    alwaysAvailableRecorded: true\n"+
			"    alwaysAvailableTracks:\n"+
			"    - codec: H264\n")
	require.True(t, ok)
	defer p.Close()

	client, writeNAL := newArchiveControlPublisher(t, "test")
	writeNAL(h264STAPAForTest(
		archiveControlSEIForTest(t, "archive.start", 1, "test"),
		[]byte{0x65, 0xa1},
	))
	writeNAL([]byte{0x65, 0xb1})
	time.Sleep(100 * time.Millisecond)
	client.Close()

	found := archiveControlRecordedMarkers(t, recDir)
	require.True(t, found[0xa1])
	_, sizeBefore := recordedFiles(t, recDir)
	time.Sleep(300 * time.Millisecond)
	countAfter, sizeAfter := recordedFiles(t, recDir)
	require.Equal(t, 1, countAfter)
	require.Equal(t, sizeBefore, sizeAfter, "offline fallback must not keep the SEI recorder alive")
}
