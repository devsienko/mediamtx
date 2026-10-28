package stream

import (
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
)

type subStreamMedia struct {
	inMedia       *description.Media
	streamMedia   *streamMedia
	useRTPPackets bool
	onDataH264    func([][]byte) ([][]byte, func())

	formats map[format.Format]*subStreamFormat
}

func (ssm *subStreamMedia) initialize() error {
	ssm.formats = make(map[format.Format]*subStreamFormat)

	for i, inFormat := range ssm.inMedia.Formats {
		origFormat := ssm.streamMedia.origMedia.Formats[i]
		var onDataH264 func([][]byte) ([][]byte, func())
		if _, ok := inFormat.(*format.H264); ok {
			onDataH264 = ssm.onDataH264
		}

		ssf := &subStreamFormat{
			inFormat:      inFormat,
			streamFormat:  ssm.streamMedia.formats[origFormat],
			useRTPPackets: ssm.useRTPPackets,
			onDataH264:    onDataH264,
		}
		err := ssf.initialize()
		if err != nil {
			return err
		}
		ssm.formats[inFormat] = ssf
	}

	return nil
}
