# RTSP test stream publisher with H.264 SEI commands

This tool publishes a local H.264 video to MediaMTX through RTSP. Keyboard
commands are embedded directly into the H.264 bitstream as SEI
`user_data_unregistered` NAL units. MediaMTX only relays the stream; no Control
API call is made.

## Run

From the repository root:

```bash
python3 tools/rtsp_streamer/stream.py
```

Open the stream in VLC:

```text
rtsp://localhost:8554/test
```

Keys are handled immediately without pressing Enter:

- `R` injects an `archive.start` command;
- `S` injects an `archive.stop` command;
- `Q` or `Ctrl+C` closes both FFmpeg processes and stops the stream.

These commands are signals only. MediaMTX does not interpret or execute them
until a matching SEI handler is implemented.

## SEI wire format

Each key press creates one H.264 prefix SEI NAL unit immediately before the
next access unit. Its payload type is `5` (`user_data_unregistered`). The
payload contains:

1. the 16-byte UUID `8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021` in RFC 4122 byte
   order;
2. compact UTF-8 JSON without a trailing zero byte.

Example JSON:

```json
{"schema":"faceter.archive-control.v1","command":"archive.start","timestamp":"2026-10-27T12:34:56.789Z","sequence":1,"stream":"test"}
```

The command is `archive.start` for `R` and `archive.stop` for `S`. `sequence`
increases for every command during a process run. The SEI RBSP uses standard
H.264 emulation-prevention bytes and ends with `rbsp_trailing_bits`.

The UUID can be changed with `--sei-uuid`.

## Other videos

To publish another H.264 file:

```bash
python3 tools/rtsp_streamer/stream.py ~/Movies/video.mp4
```

For a non-H.264 input, enable H.264 transcoding:

```bash
python3 tools/rtsp_streamer/stream.py ~/Movies/video.webm --transcode
```

Use another stream path with `--url`:

```bash
python3 tools/rtsp_streamer/stream.py --url rtsp://127.0.0.1:8554/demo
```

Run `python3 tools/rtsp_streamer/stream.py --help` for all options.

## Requirements

- Python 3.10 or newer;
- FFmpeg and ffprobe available in `PATH`;
- MediaMTX listening on the destination RTSP URL.

Override executable paths with `--ffmpeg`, `--ffprobe`, `FFMPEG_BIN`, or
`FFPROBE_BIN`.
