# RTSP test stream publisher with H.264 SEI commands

This tool publishes a local H.264 video to MediaMTX through RTSP. Keyboard
commands are embedded directly into the H.264 bitstream as SEI
`user_data_unregistered` NAL units. This MediaMTX fork recognizes the commands
and starts or stops its filesystem recorder; no Control API call is made.

Archive control is disabled by default. Enable it only for a path whose
publisher is authenticated and trusted:

```yml
paths:
  test:
    archiveControl: true
    archiveControlUUID: 8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021
    archiveControlMaxAge: 5m
    archiveControlMinRestartInterval: 1s
    recordPath: ./recordings/%path/%Y-%m-%d_%H-%M-%S-%f_%z
    recordDeleteAfter: 0s
```

`recordPath` is configured even while no archive is being written. Relative
paths are resolved from the MediaMTX working directory. `recordDeleteAfter: 0s`
keeps files indefinitely; choose a retention period in production if needed.

## Run

Start MediaMTX with the ready-to-use example configuration from the repository
root:

```bash
./mediamtx tools/rtsp_streamer/mediamtx.yml
```

Or build and run the Docker image:

```bash
docker build -f docker/local.Dockerfile -t mediamtx-fork:local .
mkdir -p recordings
docker run --rm --name mediamtx \
  -p 8554:8554 \
  -v "$PWD/tools/rtsp_streamer/mediamtx.yml:/mediamtx.yml:ro" \
  -v "$PWD/recordings:/recordings" \
  mediamtx-fork:local
```

Then, in another terminal:

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

`R` starts recording the current path with MediaMTX's configured `recordPath`.
`S` closes and finalizes that recording. A stop command only affects a recording
requested through SEI, so it cannot disable a path configured with `record: yes`.
The start access unit is included in the archive; the stop access unit is an
exclusive boundary and is not included. Rapid key presses are placed into
successive access units rather than combined into one.

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

The command is `archive.start` for `R` and `archive.stop` for `S`. MediaMTX
validates every field:

- `timestamp` must be RFC 3339 and within `archiveControlMaxAge` of server time;
- `sequence` must be non-zero and strictly increasing for the current live
  publisher session;
- `stream` must exactly match the MediaMTX path;
- `schema`, `command`, and the configured UUID must match.

The SEI RBSP uses standard H.264 emulation-prevention bytes and ends with
`rbsp_trailing_bits`. Put each command in its own prefix SEI NAL unit and do not
place more than one command in an access unit. MediaMTX consumes a recognized
control SEI before relaying or recording the access unit. This prevents an
archived stream from replaying its own commands when it is published again.

The publisher UUID can be changed with `--sei-uuid`; set the same value in
`archiveControlUUID`. The UUID identifies this protocol and is not a secret or
an authentication mechanism. Access control must be enforced with MediaMTX
publish authentication. After an accepted stop, a new start is rate-limited by
`archiveControlMinRestartInterval`.

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
