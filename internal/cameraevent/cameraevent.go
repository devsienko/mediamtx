// Package cameraevent parses camera events embedded into H264 SEI messages.
package cameraevent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/google/uuid"
)

const (
	// DefaultUUID identifies Faceter camera-event user_data_unregistered messages.
	DefaultUUID = "8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021"

	maxSEINALUSize  = 16 * 1024
	maxMetadataSize = 4 * 1024
)

// Event is the camera event sent to the external service.
type Event struct {
	EventID    uuid.UUID `json:"eventId"`
	Timestamp  int64     `json:"timestamp"`
	EventPhase string    `json:"eventPhase"`
	EventType  string    `json:"eventType"`
	ObjectType string    `json:"objectType"`
}

// Message is the wire envelope embedded into SEI and sent to the external service.
type Message struct {
	Event Event `json:"event"`
}

type wireMessage struct {
	Event *struct {
		EventID    string `json:"eventId"`
		Timestamp  int64  `json:"timestamp"`
		EventPhase string `json:"eventPhase"`
		EventType  string `json:"eventType"`
		ObjectType string `json:"objectType"`
	} `json:"event"`
}

// ServiceEndpoint validates a service base URL and appends /camera-event.
func ServiceEndpoint(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("must be a valid HTTP or HTTPS URL")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("must not contain a query string or fragment")
	}

	return url.JoinPath(u.String(), "camera-event")
}

func removeEmulationPrevention(data []byte) []byte {
	out := make([]byte, 0, len(data))
	zeroCount := 0

	for i, b := range data {
		if zeroCount >= 2 && b == 0x03 && i+1 < len(data) && data[i+1] <= 0x03 {
			zeroCount = 0
			continue
		}

		out = append(out, b)
		if b == 0x00 {
			zeroCount++
		} else {
			zeroCount = 0
		}
	}

	return out
}

func insertEmulationPrevention(data []byte) []byte {
	out := make([]byte, 0, len(data))
	zeroCount := 0

	for _, b := range data {
		if zeroCount >= 2 && b <= 0x03 {
			out = append(out, 0x03)
			zeroCount = 0
		}

		out = append(out, b)
		if b == 0x00 {
			zeroCount++
		} else {
			zeroCount = 0
		}
	}

	return out
}

func readSEIValue(buf []byte, pos *int) (int, bool) {
	value := 0
	for {
		if *pos >= len(buf) {
			return 0, false
		}

		part := int(buf[*pos])
		(*pos)++
		value += part
		if part != 0xff {
			return value, true
		}
	}
}

func parseJSON(payload []byte) (Message, bool) {
	var wire wireMessage
	if json.Unmarshal(payload, &wire) != nil || wire.Event == nil {
		return Message{}, false
	}

	eventID, err := uuid.Parse(wire.Event.EventID)
	if err != nil || eventID == uuid.Nil || wire.Event.Timestamp <= 0 ||
		(wire.Event.EventPhase != "Start" && wire.Event.EventPhase != "Stop") ||
		wire.Event.EventType == "" || wire.Event.ObjectType == "" {
		return Message{}, false
	}

	return Message{Event: Event{
		EventID:    eventID,
		Timestamp:  wire.Event.Timestamp,
		EventPhase: wire.Event.EventPhase,
		EventType:  wire.Event.EventType,
		ObjectType: wire.Event.ObjectType,
	}}, true
}

func parseSEI(nalu []byte, expectedUUID uuid.UUID) (Message, []byte, bool, int, bool) {
	if len(nalu) == 0 || nalu[0]&0x1f != 6 {
		return Message{}, nil, false, 0, true
	}
	if len(nalu) < 2 || len(nalu) > maxSEINALUSize || nalu[0]&0x80 != 0 {
		return Message{}, nil, false, 0, false
	}

	rbsp := removeEmulationPrevention(nalu[1:])
	if len(rbsp) == 0 || rbsp[len(rbsp)-1] != 0x80 {
		return Message{}, nil, false, 0, false
	}
	rbsp = rbsp[:len(rbsp)-1]

	var found Message
	foundEvent := false
	targetCount := 0
	filteredRBSP := make([]byte, 0, len(rbsp))

	for pos := 0; pos < len(rbsp); {
		messageStart := pos
		payloadType, ok := readSEIValue(rbsp, &pos)
		if !ok {
			return Message{}, nil, false, targetCount, false
		}

		payloadSize, ok := readSEIValue(rbsp, &pos)
		if !ok || payloadSize > len(rbsp)-pos {
			return Message{}, nil, false, targetCount, false
		}

		payload := rbsp[pos : pos+payloadSize]
		pos += payloadSize

		if payloadType != 5 || len(payload) < len(expectedUUID) ||
			!bytes.Equal(payload[:len(expectedUUID)], expectedUUID[:]) {
			filteredRBSP = append(filteredRBSP, rbsp[messageStart:pos]...)
			continue
		}
		targetCount++
		if targetCount > 1 {
			return Message{}, nil, false, targetCount, false
		}

		jsonPayload := payload[len(expectedUUID):]
		if len(jsonPayload) == 0 || len(jsonPayload) > maxMetadataSize {
			return Message{}, nil, false, targetCount, false
		}

		found, foundEvent = parseJSON(jsonPayload)
		if !foundEvent {
			return Message{}, nil, false, targetCount, false
		}
	}

	if !foundEvent || len(filteredRBSP) == 0 {
		return found, nil, foundEvent, targetCount, true
	}

	filteredRBSP = append(filteredRBSP, 0x80)
	filteredNALU := make([]byte, 1, 1+len(filteredRBSP))
	filteredNALU[0] = nalu[0]
	filteredNALU = append(filteredNALU, insertEmulationPrevention(filteredRBSP)...)

	return found, filteredNALU, true, targetCount, true
}

// ParseAndRemoveH264 extracts one camera event and removes only its matching
// user_data_unregistered payload. Other messages in the same SEI NALU are
// preserved; the NALU itself is removed when no messages remain. Consuming the
// event prevents it from being replayed from a recording and forwarded to
// downstream readers.
func ParseAndRemoveH264(au [][]byte, expectedUUID uuid.UUID) (Message, [][]byte, bool) {
	var found Message
	foundCount := 0
	targetCount := 0
	foundIndex := -1
	var replacement []byte

	for i, nalu := range au {
		message, filteredNALU, ok, naluTargetCount, valid := parseSEI(nalu, expectedUUID)
		if !valid {
			return Message{}, au, false
		}
		targetCount += naluTargetCount
		if targetCount > 1 {
			return Message{}, au, false
		}
		if ok {
			found = message
			foundCount++
			foundIndex = i
			replacement = filteredNALU
			if foundCount > 1 {
				return Message{}, au, false
			}
		}
	}

	if foundCount != 1 {
		return Message{}, au, false
	}

	filtered := make([][]byte, 0, len(au))
	filtered = append(filtered, au[:foundIndex]...)
	if replacement != nil {
		filtered = append(filtered, replacement)
	}
	filtered = append(filtered, au[foundIndex+1:]...)
	return found, filtered, true
}
