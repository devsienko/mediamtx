package archivecontrol

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type testWireMetadata struct {
	Schema    string `json:"schema"`
	Command   string `json:"command"`
	Timestamp string `json:"timestamp,omitempty"`
	Sequence  uint64 `json:"sequence,omitempty"`
	Stream    string `json:"stream,omitempty"`
}

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
	out = append(out, payload...)
	return out
}

func seiNAL(messages ...[]byte) []byte {
	var rbsp []byte
	for _, message := range messages {
		rbsp = append(rbsp, message...)
	}
	rbsp = append(rbsp, 0x80)
	return append([]byte{0x06}, addEmulationPrevention(rbsp)...)
}

func marshalMetadata(t testing.TB, wire testWireMetadata) []byte {
	t.Helper()

	payload, err := json.Marshal(wire)
	require.NoError(t, err)
	return payload
}

func controlMessage(t testing.TB, id uuid.UUID, wire testWireMetadata) []byte {
	t.Helper()

	payload := append(append([]byte(nil), id[:]...), marshalMetadata(t, wire)...)
	return seiMessage(5, payload)
}

func validWire(command string) testWireMetadata {
	return testWireMetadata{
		Schema:    Schema,
		Command:   command,
		Timestamp: "2026-10-28T10:11:12.345Z",
		Sequence:  math.MaxUint64,
		Stream:    "test/camera",
	}
}

func requireNotParsed(t testing.TB, au ...[]byte) {
	t.Helper()

	metadata, ok := ParseH264(au, uuid.MustParse(DefaultUUID))
	require.False(t, ok)
	require.Equal(t, Metadata{}, metadata)
}

func TestParseH264(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)

	for _, ca := range []struct {
		name     string
		wire     testWireMetadata
		expected Metadata
	}{
		{
			name: "start",
			wire: validWire("archive.start"),
			expected: Metadata{
				Command:   CommandStart,
				Timestamp: "2026-10-28T10:11:12.345Z",
				Sequence:  math.MaxUint64,
				Stream:    "test/camera",
			},
		},
		{
			name: "stop",
			wire: validWire("archive.stop"),
			expected: Metadata{
				Command:   CommandStop,
				Timestamp: "2026-10-28T10:11:12.345Z",
				Sequence:  math.MaxUint64,
				Stream:    "test/camera",
			},
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			metadata, ok := ParseH264([][]byte{
				nil,
				{0x65, 0x01, 0x02},
				seiNAL(controlMessage(t, id, ca.wire)),
			}, id)
			require.True(t, ok)
			require.Equal(t, ca.expected, metadata)
		})
	}
}

func TestParseH264PayloadType128(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)

	metadata, ok := ParseH264([][]byte{seiNAL(
		seiMessage(128, []byte{0x80, 0x00, 0x00, 0x01}),
		seiMessage(300, []byte("ignored")),
		controlMessage(t, id, validWire("archive.start")),
	)}, id)

	require.True(t, ok)
	require.Equal(t, CommandStart, metadata.Command)
}

func TestParseH264EmulationPrevention(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)

	metadata, ok := ParseH264([][]byte{seiNAL(
		seiMessage(1, []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03}),
		controlMessage(t, id, validWire("archive.stop")),
	)}, id)

	require.True(t, ok)
	require.Equal(t, CommandStop, metadata.Command)
}

func TestParseAndRemoveH264(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	sei := seiNAL(controlMessage(t, id, validWire("archive.start")))
	au := [][]byte{{0x09, 0xf0}, sei, {0x65, 0x01}}

	metadata, filtered, ok := ParseAndRemoveH264(au, id)
	require.True(t, ok)
	require.Equal(t, CommandStart, metadata.Command)
	require.Equal(t, [][]byte{{0x09, 0xf0}, {0x65, 0x01}}, filtered)
	require.Equal(t, sei, au[1], "the caller-owned access unit must not be mutated")
}

func TestParseH264MalformedSEI(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	valid := seiNAL(controlMessage(t, id, validWire("archive.start")))

	missingTrailing := append([]byte(nil), valid[:len(valid)-1]...)
	wrongTrailing := append([]byte(nil), valid...)
	wrongTrailing[len(wrongTrailing)-1] = 0x00
	extraAfterTrailing := append(append([]byte(nil), valid...), 0x00)
	forbiddenBit := append([]byte(nil), valid...)
	forbiddenBit[0] |= 0x80
	oversized := make([]byte, maxSEINALUSize+1)
	oversized[0] = 0x06
	oversized[len(oversized)-1] = 0x80

	for _, ca := range []struct {
		name string
		nalu []byte
	}{
		{name: "SEI header only", nalu: []byte{0x06}},
		{name: "forbidden zero bit", nalu: forbiddenBit},
		{name: "missing trailing bits", nalu: missingTrailing},
		{name: "wrong trailing bits", nalu: wrongTrailing},
		{name: "bytes after trailing bits", nalu: extraAfterTrailing},
		{name: "truncated payload type extension", nalu: []byte{0x06, 0xff, 0x80}},
		{name: "truncated payload size extension", nalu: []byte{0x06, 0x05, 0xff, 0x80}},
		{name: "payload larger than RBSP", nalu: []byte{0x06, 0x05, 0x05, 0x01, 0x80}},
		{name: "oversized NALU", nalu: oversized},
	} {
		t.Run(ca.name, func(t *testing.T) {
			// A malformed SEI must invalidate the entire access unit instead of
			// being skipped in favor of a later valid command.
			requireNotParsed(t, ca.nalu, valid)
		})
	}
}

func TestParseH264Sizes(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	base := marshalMetadata(t, validWire("archive.start"))

	buildWithMetadataSize := func(size int) []byte {
		t.Helper()
		require.GreaterOrEqual(t, size, len(base))

		jsonPayload := append([]byte(nil), base...)
		jsonPayload = append(jsonPayload, bytes.Repeat([]byte{' '}, size-len(jsonPayload))...)
		payload := append(append([]byte(nil), id[:]...), jsonPayload...)
		return seiNAL(seiMessage(5, payload))
	}

	t.Run("extended payload size", func(t *testing.T) {
		metadata, ok := ParseH264([][]byte{buildWithMetadataSize(300)}, id)
		require.True(t, ok)
		require.Equal(t, CommandStart, metadata.Command)
	})

	t.Run("maximum metadata size", func(t *testing.T) {
		metadata, ok := ParseH264([][]byte{buildWithMetadataSize(maxMetadataSize)}, id)
		require.True(t, ok)
		require.Equal(t, CommandStart, metadata.Command)
	})

	t.Run("metadata over limit", func(t *testing.T) {
		requireNotParsed(t, buildWithMetadataSize(maxMetadataSize+1))
	})

	t.Run("maximum NALU size", func(t *testing.T) {
		control := controlMessage(t, id, validWire("archive.start"))
		var nalu []byte
		for fillerSize := maxSEINALUSize; fillerSize >= 0; fillerSize-- {
			candidate := seiNAL(seiMessage(1, bytes.Repeat([]byte{0x55}, fillerSize)), control)
			if len(candidate) == maxSEINALUSize {
				nalu = candidate
				break
			}
		}
		require.Len(t, nalu, maxSEINALUSize)

		metadata, ok := ParseH264([][]byte{nalu}, id)
		require.True(t, ok)
		require.Equal(t, CommandStart, metadata.Command)
	})

	t.Run("NALU over limit poisons access unit", func(t *testing.T) {
		oversized := make([]byte, maxSEINALUSize+1)
		oversized[0] = 0x06
		oversized[len(oversized)-1] = 0x80
		requireNotParsed(t, oversized, buildWithMetadataSize(len(base)))
	})
}

func TestParseH264MultipleCommands(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	start := controlMessage(t, id, validWire("archive.start"))
	stop := controlMessage(t, id, validWire("archive.stop"))

	t.Run("same NALU", func(t *testing.T) {
		requireNotParsed(t, seiNAL(start, stop))
	})

	t.Run("different NALUs", func(t *testing.T) {
		requireNotParsed(t, seiNAL(start), seiNAL(stop))
	})

	t.Run("matching and non-matching UUID", func(t *testing.T) {
		otherID := uuid.MustParse("11111111-2222-4333-8444-555555555555")
		metadata, ok := ParseH264([][]byte{seiNAL(
			controlMessage(t, otherID, validWire("archive.stop")),
			start,
		)}, id)
		require.True(t, ok)
		require.Equal(t, CommandStart, metadata.Command)
	})
}

func TestParseH264RejectsNonMatchingMetadata(t *testing.T) {
	id := uuid.MustParse(DefaultUUID)
	otherID := uuid.MustParse("11111111-2222-4333-8444-555555555555")

	wrongSchema := validWire("archive.start")
	wrongSchema.Schema = "faceter.archive-control.v2"
	wrongCommand := validWire("archive.start.now")

	negativeSequence := []byte(`{"schema":"` + Schema + `","command":"archive.start","sequence":-1}`)
	trailingGarbage := append(marshalMetadata(t, validWire("archive.start")), []byte("not-json")...)

	for _, ca := range []struct {
		name    string
		message []byte
	}{
		{name: "wrong UUID", message: controlMessage(t, otherID, validWire("archive.start"))},
		{name: "wrong schema", message: controlMessage(t, id, wrongSchema)},
		{name: "unknown command", message: controlMessage(t, id, wrongCommand)},
		{name: "empty JSON", message: seiMessage(5, id[:])},
		{name: "malformed JSON", message: seiMessage(5, append(append([]byte(nil), id[:]...), '{'))},
		{name: "negative sequence", message: seiMessage(5, append(append([]byte(nil), id[:]...), negativeSequence...))},
		{name: "trailing JSON garbage", message: seiMessage(5, append(append([]byte(nil), id[:]...), trailingGarbage...))},
	} {
		t.Run(ca.name, func(t *testing.T) {
			requireNotParsed(t, seiNAL(ca.message))
		})
	}
}

func FuzzParseH264(f *testing.F) {
	id := uuid.MustParse(DefaultUUID)
	valid := seiNAL(controlMessage(f, id, validWire("archive.start")))

	for _, seed := range [][]byte{
		nil,
		{0x06},
		{0x06, 0x80},
		{0x06, 0xff, 0x80},
		valid,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		first, firstOK := ParseH264([][]byte{data}, id)
		second, secondOK := ParseH264([][]byte{data}, id)
		require.Equal(t, firstOK, secondOK)
		require.Equal(t, first, second)

		if firstOK {
			require.Contains(t, []Command{CommandStart, CommandStop}, first.Command)
		}

		split := len(data) / 2
		_, _ = ParseH264([][]byte{data[:split], nil, data[split:]}, id)

		var arbitraryID uuid.UUID
		copy(arbitraryID[:], data)
		_, _ = ParseH264([][]byte{data}, arbitraryID)
	})
}

func FuzzParseH264Metadata(f *testing.F) {
	id := uuid.MustParse(DefaultUUID)

	f.Add(marshalMetadata(f, validWire("archive.start")))
	f.Add(marshalMetadata(f, validWire("archive.stop")))
	f.Add([]byte(`{"schema":"faceter.archive-control.v1"}`))
	f.Add([]byte{0x00, 0x00, 0x01, 0xff})

	f.Fuzz(func(t *testing.T, jsonPayload []byte) {
		payload := append(append([]byte(nil), id[:]...), jsonPayload...)
		nalu := seiNAL(seiMessage(5, payload))

		metadata, ok := ParseH264([][]byte{nalu}, id)
		if ok {
			require.Contains(t, []Command{CommandStart, CommandStop}, metadata.Command)
		}
	})
}
