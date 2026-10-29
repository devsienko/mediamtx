# RTSP test publisher with H.264 camera-event SEI

This tool publishes a local H.264 video to MediaMTX through RTSP and embeds
camera events into the H.264 stream as `user_data_unregistered` SEI NAL units.
MediaMTX consumes each valid event and asynchronously sends it to an external
HTTP service.

## MediaMTX configuration

Configure a base service URL on the path. MediaMTX appends `/camera-event`:

```yml
paths:
  test:
    cameraEventServiceURL: http://127.0.0.1:8080
    cameraEventSEIUUID: 8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021
    cameraEventRequestTimeout: 5s
```

An empty `cameraEventServiceURL` disables processing. The standard MediaMTX
environment loader can override these values:

```bash
export MTX_PATHDEFAULTS_CAMERAEVENTSERVICEURL=http://events:8080
export MTX_PATHDEFAULTS_CAMERAEVENTREQUESTTIMEOUT=5s
```

For a single path, use `MTX_PATHS_TEST_CAMERAEVENTSERVICEURL`. Environment
variable names are uppercase and follow the configuration hierarchy.

For every accepted event MediaMTX sends:

```http
POST /camera-event
Content-Type: application/json
```

with the exact JSON envelope received from the camera:

```json
{
  "event": {
    "eventId": "8e86225f-6644-4daf-b150-a5fda0ba72d0",
    "timestamp": 1793145600,
    "eventPhase": "Start",
    "eventType": "Motion",
    "objectType": "Human"
  }
}
```

Delivery happens outside the video processing goroutine. Requests time out,
retry up to two times after network, 408, 429, or 5xx failures, and are queued
in order per MediaMTX path. Since a lost HTTP response can produce a retry, the
receiver must deduplicate requests by `eventId` and `eventPhase`.

## Run

Start MediaMTX with the example configuration:

```bash
./mediamtx tools/rtsp_streamer/mediamtx.yml
```

Then publish the test video:

```bash
python3 tools/rtsp_streamer/stream.py
```

Open `rtsp://localhost:8554/test` in VLC. Keys work without pressing Enter:

- `R` creates an event ID and injects `eventPhase: Start`;
- `S` injects `eventPhase: Stop` with the same event ID;
- `Q` or `Ctrl+C` stops the stream and both FFmpeg processes.

Event and object types can be changed:

```bash
python3 tools/rtsp_streamer/stream.py --event-type Motion --object-type Human
```

## SEI wire format

The tool inserts a dedicated H.264 prefix SEI immediately before the next
video slice. Its payload type is `5` (`user_data_unregistered`) and contains:

1. the configured 16-byte UUID in RFC 4122 byte order;
2. compact UTF-8 JSON without a trailing zero byte.

MediaMTX accepts only the `{"event": {...}}` envelope shown above. The legacy
`schema/type` envelope and archive-control commands are ignored. Exactly one
matching camera event is allowed per access unit. JSON is limited to 4 KiB and
the complete SEI NAL unit to 16 KiB.

A recognized event SEI is removed before relay or recording, preventing event
replay when a recorded stream is published later. The UUID identifies the
protocol but is not authentication; publishers must still be authenticated.

## Other videos

```bash
python3 tools/rtsp_streamer/stream.py ~/Movies/video.mp4
python3 tools/rtsp_streamer/stream.py ~/Movies/video.webm --transcode
python3 tools/rtsp_streamer/stream.py --url rtsp://127.0.0.1:8554/demo
```

Requirements: Python 3.10+, FFmpeg, and ffprobe. Run the script with `--help`
for all options.
