// Package archivecontrol parses commands embedded into H264 SEI messages.
package archivecontrol

import (
	"bytes"
	"encoding/json"

	"github.com/google/uuid"
)

const (
	// Schema is the archive-control metadata schema supported by MediaMTX.
	Schema = "faceter.archive-control.v1"

	// DefaultUUID identifies archive-control user_data_unregistered messages.
	DefaultUUID = "8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021"

	maxSEINALUSize  = 16 * 1024
	maxMetadataSize = 4 * 1024
)

// Command is an archive-control command.
type Command int

const (
	// CommandStart starts recording.
	CommandStart Command = iota

	// CommandStop stops recording.
	CommandStop
)

// Metadata is the validated envelope of an archive-control command.
type Metadata struct {
	Command   Command
	Timestamp string
	Sequence  uint64
	Stream    string
}

type wireMetadata struct {
	Schema    string `json:"schema"`
	Command   string `json:"command"`
	Timestamp string `json:"timestamp"`
	Sequence  uint64 `json:"sequence"`
	Stream    string `json:"stream"`
}

func removeEmulationPrevention(data []byte) []byte {
	out := make([]byte, 0, len(data))
	zeroCount := 0

	for i, b := range data {
		if zeroCount >= 2 && b == 0x03 && i+1 < len(data) && data[i+1] <= 0x03 {
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

func parseSEI(nalu []byte, expectedUUID uuid.UUID) (Metadata, bool, int, bool) {
	if len(nalu) == 0 || nalu[0]&0x1f != 6 {
		return Metadata{}, false, 0, true
	}
	if len(nalu) < 2 || len(nalu) > maxSEINALUSize || nalu[0]&0x80 != 0 {
		return Metadata{}, false, 0, false
	}

	rbsp := removeEmulationPrevention(nalu[1:])
	if len(rbsp) == 0 || rbsp[len(rbsp)-1] != 0x80 {
		return Metadata{}, false, 0, false
	}
	rbsp = rbsp[:len(rbsp)-1]

	var found Metadata
	foundCommand := false
	targetCount := 0

	for pos := 0; pos < len(rbsp); {
		payloadType, ok := readSEIValue(rbsp, &pos)
		if !ok {
			return Metadata{}, false, targetCount, false
		}

		payloadSize, ok := readSEIValue(rbsp, &pos)
		if !ok || payloadSize > len(rbsp)-pos {
			return Metadata{}, false, targetCount, false
		}

		payload := rbsp[pos : pos+payloadSize]
		pos += payloadSize

		if payloadType != 5 || len(payload) < len(expectedUUID) ||
			!bytes.Equal(payload[:len(expectedUUID)], expectedUUID[:]) {
			continue
		}
		targetCount++
		if targetCount > 1 {
			return Metadata{}, false, targetCount, false
		}

		jsonPayload := payload[len(expectedUUID):]
		if len(jsonPayload) == 0 || len(jsonPayload) > maxMetadataSize {
			return Metadata{}, false, targetCount, false
		}

		var wire wireMetadata
		if json.Unmarshal(jsonPayload, &wire) != nil {
			return Metadata{}, false, targetCount, false
		}
		if wire.Schema != Schema {
			continue
		}

		var command Command
		switch wire.Command {
		case "archive.start":
			command = CommandStart

		case "archive.stop":
			command = CommandStop

		default:
			continue
		}

		found = Metadata{
			Command:   command,
			Timestamp: wire.Timestamp,
			Sequence:  wire.Sequence,
			Stream:    wire.Stream,
		}
		foundCommand = true
	}

	return found, foundCommand, targetCount, true
}

// ParseH264 extracts a single archive-control command from an H264 access unit.
// Access units that contain multiple commands or malformed SEI messages are rejected.
func ParseH264(au [][]byte, expectedUUID uuid.UUID) (Metadata, bool) {
	metadata, _, ok := ParseAndRemoveH264(au, expectedUUID)
	return metadata, ok
}

// ParseAndRemoveH264 extracts a single archive-control command and returns an
// access unit without the SEI NALU that contained it. Control SEI is consumed
// to prevent a recorded stream from replaying commands when republished.
func ParseAndRemoveH264(au [][]byte, expectedUUID uuid.UUID) (Metadata, [][]byte, bool) {
	var found Metadata
	foundCount := 0
	targetCount := 0
	foundIndex := -1

	for i, nalu := range au {
		metadata, ok, naluTargetCount, valid := parseSEI(nalu, expectedUUID)
		if !valid {
			return Metadata{}, au, false
		}
		targetCount += naluTargetCount
		if targetCount > 1 {
			return Metadata{}, au, false
		}
		if ok {
			found = metadata
			foundCount++
			foundIndex = i
			if foundCount > 1 {
				return Metadata{}, au, false
			}
		}
	}

	if foundCount != 1 {
		return Metadata{}, au, false
	}

	filtered := make([][]byte, 0, len(au)-1)
	filtered = append(filtered, au[:foundIndex]...)
	filtered = append(filtered, au[foundIndex+1:]...)
	return found, filtered, true
}
