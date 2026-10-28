# Record streams

Live streams be recorded to disk and played back with the following file containers and codecs:

| container | codecs                                                                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| fMP4      | **Video**: AV1, VP9, H265, H264, MPEG-4 Video (H263, Xvid), MPEG-1/2 Video, M-JPEG<br/>**Audio**: Opus, MPEG-4 Audio (AAC), MPEG-1/2 Audio (MP3), AC-3, G711 (PCMA, PCMU), LPCM |
| MPEG-TS   | **Video**: H265, H264, MPEG-4 Video (H263, Xvid), MPEG-1/2 Video<br/>**Audio**: Opus, MPEG-4 Audio (AAC), MPEG-1/2 Audio (MP3), AC-3                                            |

## Usage

To record available streams to disk, set the `record` parameter in the configuration file:

```yml
pathDefaults:
  # Record streams to disk.
  record: yes
```

It's also possible to specify additional parameters:

```yml
pathDefaults:
  # Record streams to disk.
  record: yes
  # Path of recording segments.
  # Extension is added automatically.
  # Available variables are %path (path name), %Y %m %d (year, month, day),
  # %H %M %S (hours, minutes, seconds), %f (microseconds), %z (time zone), %s (unix epoch).
  recordPath: ./recordings/%path/%Y-%m-%d_%H-%M-%S-%f_%z
  # Format of recorded segments.
  # Available formats are "fmp4" (fragmented MP4) and "mpegts" (MPEG-TS).
  recordFormat: fmp4
  # fMP4 segments are concatenation of small MP4 files (parts), each with this duration.
  # MPEG-TS segments are concatenation of 188-bytes packets, flushed to disk with this period.
  # When a system failure occurs, the last part gets lost.
  # Therefore, the part duration is equal to the RPO (recovery point objective).
  recordPartDuration: 1s
  # This prevents RAM exhaustion.
  recordMaxPartSize: 50M
  # Minimum duration of each segment.
  recordSegmentDuration: 1h
  # Delete segments after this timespan.
  # Set to 0s to disable automatic deletion.
  recordDeleteAfter: 1d
```

All available recording parameters are listed in the [configuration file](../6-references/1-configuration-file.md).

## Control recording from H264 SEI metadata

This fork can start and stop the recorder from metadata embedded by a trusted
H264 publisher. The feature is disabled by default and is configured per path:

```yml
paths:
  test:
    archiveControl: true
    archiveControlUUID: 8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021
    archiveControlMaxAge: 5m
    archiveControlMinRestartInterval: 1s
    recordPath: ./recordings/%path/%Y-%m-%d_%H-%M-%S-%f_%z
```

The publisher sends a dedicated H264 prefix SEI NAL unit with payload type 5
(`user_data_unregistered`). Its payload is the configured 16-byte UUID followed
by compact UTF-8 JSON:

```json
{"schema":"faceter.archive-control.v1","command":"archive.start","timestamp":"2026-10-28T10:11:12.345Z","sequence":1,"stream":"test"}
```

`command` is either `archive.start` or `archive.stop`. The timestamp must be
RFC 3339 and fresh, the sequence must be non-zero and strictly increase during
the current live publisher session, and `stream` must exactly match the path.
Setting `archiveControlMaxAge` to `0s` disables only the freshness window; a
valid timestamp remains required.

Start is inclusive: the access unit containing `archive.start` is the first one
recorded. Stop is exclusive: the access unit containing `archive.stop` is used
to finalize the preceding sample and is not recorded. MediaMTX removes a
recognized control SEI before recording and relaying the access unit, preventing
command replay when an archive is published again. Only one control command is
accepted per access unit.

An SEI stop does not disable recording enabled with `record: true`. The UUID is
a protocol identifier, not authentication; enable this feature only on paths
with trusted, authenticated publishers. See the
[test publisher](../../tools/rtsp_streamer/README.md) for a working sender.

## Remote upload

To upload recordings to a remote location, you can use _MediaMTX_ together with [rclone](https://github.com/rclone/rclone), a command line tool that provides file synchronization capabilities with a huge variety of services (including S3, FTP, SMB, Google Drive):

1. Download and install [rclone](https://github.com/rclone/rclone).

2. Configure _rclone_:

   ```sh
   rclone config
   ```

3. Place `rclone` into the `runOnInit` and `runOnRecordSegmentComplete` hooks:

   ```yml
   pathDefaults:
     # this is needed to sync segments after a crash.
     # replace myconfig with the name of the rclone config.
     runOnInit: rclone sync -v ./recordings myconfig:/my-path/recordings

     # this is called when a segment has been finalized.
     # replace myconfig with the name of the rclone config.
     runOnRecordSegmentComplete: rclone sync -v --min-age=1ms ./recordings myconfig:/my-path/recordings
   ```

   If you want to delete local segments after they are uploaded, replace `rclone sync` with `rclone move`.
