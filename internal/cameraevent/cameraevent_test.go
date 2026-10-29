package cameraevent

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func encodeSEIValue(v int) []byte {
	var out []byte
	for v >= 0xff {
		out = append(out, 0xff)
		v -= 0xff
	}
	return append(out, byte(v))
}

func addEmulationPrevention(data []byte) []byte {
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

func seiMessage(payloadType int, payload []byte) []byte {
	out := encodeSEIValue(payloadType)
	out = append(out, encodeSEIValue(len(payload))...)
	return append(out, payload...)
}

func seiNAL(messages ...[]byte) []byte {
	var rbsp []byte
	for _, message := range messages {
		rbsp = append(rbsp, message...)
	}
	rbsp = append(rbsp, 0x80)
	return append([]byte{0x06}, addEmulationPrevention(rbsp)...)
}

func testMessage(phase string) Message {
	return Message{Event: Event{
		EventID:    uuid.MustParse("8e86225f-6644-4daf-b150-a5fda0ba72d0"),
		Timestamp:  1793145600,
		EventPhase: phase,
		EventType:  "Motion",
		ObjectType: "Human",
	}}
}

func eventSEIMessage(t testing.TB, id uuid.UUID, payload any) []byte {
	t.Helper()

	jsonPayload, err := json.Marshal(payload)
	require.NoError(t, err)
	data := append(append([]byte(nil), id[:]...), jsonPayload...)
	return seiMessage(5, data)
}

func eventSEI(t testing.TB, id uuid.UUID, payload any) []byte {
	t.Helper()

	return seiNAL(eventSEIMessage(t, id, payload))
}

func TestParseAndRemoveH264(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)

	for _, phase := range []string{"Start", "Stop"} {
		t.Run(phase, func(t *testing.T) {
			sei := eventSEI(t, id, testMessage(phase))
			au := [][]byte{{0x09, 0xf0}, sei, {0x65, 0x01}}

			message, filtered, ok := ParseAndRemoveH264(au, id)
			require.True(t, ok)
			require.Equal(t, testMessage(phase), message)
			require.Equal(t, [][]byte{{0x09, 0xf0}, {0x65, 0x01}}, filtered)
			require.Equal(t, sei, au[1], "the caller-owned access unit must not be mutated")
		})
	}
}

func TestParseAndRemoveH264PreservesOtherSEIMessages(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	otherID := uuid.MustParse("c83e2e42-a02f-49ac-9fbb-4e6db7679aa1")
	registeredData := seiMessage(4, []byte{0xb5, 0x00, 0x00, 0x01, 0x02})
	otherUserData := seiMessage(5, append(append([]byte(nil), otherID[:]...), []byte("custom metadata")...))
	eventData := eventSEIMessage(t, id, testMessage("Start"))
	sei := seiNAL(registeredData, eventData, otherUserData)
	au := [][]byte{{0x09, 0xf0}, sei, {0x65, 0x01}}

	message, filtered, ok := ParseAndRemoveH264(au, id)
	require.True(t, ok)
	require.Equal(t, testMessage("Start"), message)
	require.Equal(t, [][]byte{
		{0x09, 0xf0},
		seiNAL(registeredData, otherUserData),
		{0x65, 0x01},
	}, filtered)
	require.Equal(t, sei, au[1], "the caller-owned access unit must not be mutated")
}

func TestEmulationPreventionRoundTrip(t *testing.T) {
	for _, rbsp := range [][]byte{
		{0x00, 0x00, 0x00},
		{0x00, 0x00, 0x01},
		{0x00, 0x00, 0x02},
		{0x00, 0x00, 0x03},
		{0x00, 0x00, 0x03, 0x00},
	} {
		require.Equal(t, rbsp, removeEmulationPrevention(insertEmulationPrevention(rbsp)))
	}
}

func TestRejectsLegacyAndInvalidMessages(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	valid := testMessage("Start")

	for _, ca := range []struct {
		name    string
		payload any
	}{
		{"legacy envelope", map[string]any{"schema": "faceter.video-metadata.v1", "type": "camera.event"}},
		{"missing event", map[string]any{}},
		{"zero event ID", Message{Event: Event{Timestamp: 1, EventPhase: "Start", EventType: "Motion", ObjectType: "Human"}}},
		{"zero timestamp", Message{Event: Event{EventID: valid.Event.EventID, EventPhase: "Start", EventType: "Motion", ObjectType: "Human"}}},
		{"invalid phase", Message{Event: Event{EventID: valid.Event.EventID, Timestamp: 1, EventPhase: "Begin", EventType: "Motion", ObjectType: "Human"}}},
		{"missing event type", Message{Event: Event{EventID: valid.Event.EventID, Timestamp: 1, EventPhase: "Start", ObjectType: "Human"}}},
		{"missing object type", Message{Event: Event{EventID: valid.Event.EventID, Timestamp: 1, EventPhase: "Start", EventType: "Motion"}}},
	} {
		t.Run(ca.name, func(t *testing.T) {
			_, _, ok := ParseAndRemoveH264([][]byte{eventSEI(t, id, ca.payload)}, id)
			require.False(t, ok)
		})
	}
}

func TestRejectsMultipleAndOversizedEvents(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	one := eventSEI(t, id, testMessage("Start"))
	two := eventSEI(t, id, testMessage("Stop"))

	_, _, ok := ParseAndRemoveH264([][]byte{one, two}, id)
	require.False(t, ok)

	oversizedJSON := append([]byte(`{"event":{}}`), bytes.Repeat([]byte{' '}, maxMetadataSize)...)
	payload := append(append([]byte(nil), id[:]...), oversizedJSON...)
	_, _, ok = ParseAndRemoveH264([][]byte{seiNAL(seiMessage(5, payload))}, id)
	require.False(t, ok)
}

func TestServiceEndpoint(t *testing.T) {
	endpoint, err := ServiceEndpoint("https://events.example.test/api/")
	require.NoError(t, err)
	require.Equal(t, "https://events.example.test/api/camera-event", endpoint)

	for _, value := range []string{"", "events:8080", "ftp://events", "http://events?a=b"} {
		_, err = ServiceEndpoint(value)
		require.Error(t, err)
	}
}

func FuzzParseAndRemoveH264(f *testing.F) {
	id := uuid.MustParse(DefaultUUID)
	f.Add(eventSEI(f, id, testMessage("Start")))
	f.Add([]byte{0x06, 0x80})
	f.Add([]byte{0x65, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		first, firstFiltered, firstOK := ParseAndRemoveH264([][]byte{data}, id)
		second, secondFiltered, secondOK := ParseAndRemoveH264([][]byte{data}, id)
		require.Equal(t, firstOK, secondOK)
		require.Equal(t, first, second)
		require.Equal(t, firstFiltered, secondFiltered)
	})
}
