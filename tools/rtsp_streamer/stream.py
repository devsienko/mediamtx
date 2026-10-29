#!/usr/bin/env python3
"""Publish H.264 over RTSP and inject keyboard-triggered camera-event SEI."""

from __future__ import annotations

import argparse
from fractions import Fraction
import json
import os
from pathlib import Path
import queue
import select
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from urllib import parse


SCRIPT_DIR = Path(__file__).resolve().parent
REPOSITORY_DIR = SCRIPT_DIR.parent.parent
DEFAULT_VIDEO = REPOSITORY_DIR / "internal" / "stream" / "offline_h264.mp4"
DEFAULT_URL = "rtsp://127.0.0.1:8554/test"
DEFAULT_SEI_UUID = "8f3f2f78-90b8-4f6e-a6d8-5f627bb8d021"


class StopRequested(Exception):
    """Raised when the user asks the publisher to stop."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish H.264 over RTSP and inject camera-event SEI messages."
    )
    parser.add_argument(
        "video",
        nargs="?",
        type=Path,
        default=DEFAULT_VIDEO,
        help=f"video file to publish (default: {DEFAULT_VIDEO})",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"destination RTSP URL (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--ffmpeg",
        default=os.environ.get("FFMPEG_BIN", "ffmpeg"),
        help="FFmpeg executable (default: ffmpeg or $FFMPEG_BIN)",
    )
    parser.add_argument(
        "--ffprobe",
        default=os.environ.get("FFPROBE_BIN", "ffprobe"),
        help="ffprobe executable (default: ffprobe or $FFPROBE_BIN)",
    )
    parser.add_argument(
        "--sei-uuid",
        default=DEFAULT_SEI_UUID,
        help=f"UUID of user_data_unregistered SEI messages (default: {DEFAULT_SEI_UUID})",
    )
    parser.add_argument(
        "--event-type",
        default="Motion",
        help="camera event type (default: Motion)",
    )
    parser.add_argument(
        "--object-type",
        default="Human",
        help="camera object type (default: Human)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="publish the file once instead of looping forever",
    )
    parser.add_argument(
        "--transcode",
        action="store_true",
        help="encode video as H.264 instead of copying an existing H.264 stream",
    )
    parser.add_argument(
        "--log-level",
        default="warning",
        choices=("quiet", "error", "warning", "info", "verbose", "debug"),
        help="FFmpeg log level (default: warning)",
    )
    return parser.parse_args()


def resolve_executable(value: str, label: str) -> str:
    resolved = shutil.which(value)
    if resolved is None:
        raise FileNotFoundError(
            f"{label} executable '{value}' was not found. Install FFmpeg or set its path explicitly."
        )
    return resolved


def stream_path(rtsp_url: str) -> str:
    parsed = parse.urlparse(rtsp_url)
    if parsed.scheme != "rtsp" or not parsed.hostname:
        raise ValueError("The destination URL must be a valid rtsp:// URL")
    path = parse.unquote(parsed.path).strip("/")
    if not path:
        raise ValueError("The RTSP URL must contain a stream path")
    return path


def probe_video(ffprobe: str, video: Path) -> tuple[str, Fraction]:
    result = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,avg_frame_rate",
            "-of",
            "json",
            str(video),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    streams = json.loads(result.stdout).get("streams", [])
    if not streams:
        raise ValueError(f"No video stream found in {video}")

    codec = streams[0].get("codec_name", "")
    frame_rate = Fraction(streams[0].get("avg_frame_rate", "0/1"))
    if frame_rate <= 0:
        raise ValueError("Could not determine the video frame rate")
    return codec, frame_rate


def build_source_command(
    args: argparse.Namespace,
    ffmpeg: str,
    video: Path,
) -> list[str]:
    command = [ffmpeg, "-hide_banner", "-nostdin", "-loglevel", args.log_level, "-re"]
    if not args.once:
        command.extend(("-stream_loop", "-1"))
    command.extend(("-i", str(video), "-map", "0:v:0", "-an"))

    if args.transcode:
        command.extend(
            (
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-tune",
                "zerolatency",
                "-pix_fmt",
                "yuv420p",
            )
        )
    else:
        command.extend(("-c:v", "copy"))

    command.extend(
        (
            "-bsf:v",
            "h264_mp4toannexb,h264_metadata=aud=insert",
            "-f",
            "h264",
            "pipe:1",
        )
    )
    return command


def build_publisher_command(
    args: argparse.Namespace,
    ffmpeg: str,
    frame_rate: Fraction,
) -> list[str]:
    return [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        args.log_level,
        "-r",
        str(float(frame_rate)),
        "-f",
        "h264",
        "-i",
        "pipe:0",
        "-c:v",
        "copy",
        "-f",
        "rtsp",
        "-rtsp_transport",
        "tcp",
        args.url,
    ]


def encode_sei_size(size: int) -> bytes:
    encoded = bytearray()
    while size >= 255:
        encoded.append(255)
        size -= 255
    encoded.append(size)
    return bytes(encoded)


def escape_rbsp(data: bytes) -> bytes:
    escaped = bytearray()
    zero_count = 0
    for value in data:
        if zero_count >= 2 and value <= 3:
            escaped.append(3)
            zero_count = 0
        escaped.append(value)
        zero_count = zero_count + 1 if value == 0 else 0
    return bytes(escaped)


def build_sei_nal(identifier: uuid.UUID, metadata: dict[str, object]) -> tuple[bytes, bytes]:
    json_payload = json.dumps(
        metadata,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    payload = identifier.bytes + json_payload

    # payloadType 5 is user_data_unregistered. 0x80 is rbsp_trailing_bits.
    rbsp = b"\x05" + encode_sei_size(len(payload)) + payload + b"\x80"
    nal = b"\x00\x00\x00\x01\x06" + escape_rbsp(rbsp)
    return nal, json_payload


def find_start_code(data: bytearray, offset: int = 0) -> tuple[int, int] | None:
    limit = len(data) - 2
    for index in range(offset, limit):
        if data[index] == 0 and data[index + 1] == 0:
            if data[index + 2] == 1:
                return index, 3
            if index + 3 < len(data) and data[index + 2] == 0 and data[index + 3] == 1:
                return index, 4
    return None


def annexb_nals(stream: object):
    buffer = bytearray()
    fd = stream.fileno()  # type: ignore[attr-defined]

    while True:
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        buffer.extend(chunk)

        while True:
            first = find_start_code(buffer)
            if first is None:
                if len(buffer) > 4:
                    del buffer[:-4]
                break
            if first[0] > 0:
                del buffer[: first[0]]
                first = (0, first[1])

            second = find_start_code(buffer, first[1])
            if second is None:
                break
            yield bytes(buffer[: second[0]])
            del buffer[: second[0]]

    first = find_start_code(buffer)
    if first is not None:
        yield bytes(buffer[first[0] :])


def nal_type(nal: bytes) -> int | None:
    if nal.startswith(b"\x00\x00\x00\x01") and len(nal) > 4:
        return nal[4] & 0x1F
    if nal.startswith(b"\x00\x00\x01") and len(nal) > 3:
        return nal[3] & 0x1F
    return None


def pump_h264(
    source_stdout: object,
    publisher_stdin: object,
    pending_sei: queue.Queue[bytes],
    errors: queue.Queue[BaseException],
) -> None:
    at_access_unit_start = False
    try:
        for nal in annexb_nals(source_stdout):
            current_type = nal_type(nal)
            if current_type == 9:  # access unit delimiter
                at_access_unit_start = True
            elif current_type in (1, 5) and at_access_unit_start:  # non-IDR / IDR slice
                # The protocol accepts exactly one command per access unit.
                # Keep additional key presses queued for following frames.
                try:
                    publisher_stdin.write(pending_sei.get_nowait())
                except queue.Empty:
                    pass
                at_access_unit_start = False

            publisher_stdin.write(nal)
            publisher_stdin.flush()
    except (BrokenPipeError, OSError) as exc:
        errors.put(exc)
    finally:
        try:
            publisher_stdin.close()
        except OSError:
            pass


class Keyboard:
    """Read individual keys without requiring Enter and restore the terminal."""

    def __init__(self) -> None:
        self.enabled = sys.stdin.isatty()
        self.fd: int | None = None
        self.attributes: list[object] | None = None

    def __enter__(self) -> Keyboard:
        if self.enabled and os.name == "posix":
            import termios
            import tty

            self.fd = sys.stdin.fileno()
            self.attributes = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        if self.fd is not None and self.attributes is not None:
            import termios

            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.attributes)

    def read(self, timeout: float) -> str | None:
        if not self.enabled:
            time.sleep(timeout)
            return None
        if os.name == "nt":
            import msvcrt

            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if msvcrt.kbhit():
                    return msvcrt.getwch()
                time.sleep(0.02)
            return None

        readable, _, _ = select.select([sys.stdin], [], [], timeout)
        return sys.stdin.read(1) if readable else None


def stop_process(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait()


def main() -> int:
    args = parse_args()
    video = args.video.expanduser().resolve()

    if not video.is_file():
        print(f"Video file does not exist: {video}", file=sys.stderr)
        return 2

    try:
        path = stream_path(args.url)
        sei_uuid = uuid.UUID(args.sei_uuid)
        ffmpeg = resolve_executable(args.ffmpeg, "FFmpeg")
        ffprobe = resolve_executable(args.ffprobe, "ffprobe")
        codec, frame_rate = probe_video(ffprobe, video)
    except (ValueError, FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(exc, file=sys.stderr)
        return 2

    if codec != "h264" and not args.transcode:
        print(
            f"Input codec is {codec or 'unknown'}, but SEI injection requires H.264. Use --transcode.",
            file=sys.stderr,
        )
        return 2

    source_command = build_source_command(args, ffmpeg, video)
    publisher_command = build_publisher_command(args, ffmpeg, frame_rate)
    pending_sei: queue.Queue[bytes] = queue.Queue()
    pipeline_errors: queue.Queue[BaseException] = queue.Queue()

    print(f"Video:    {video}")
    print(f"Stream:   {args.url}")
    print(f"Path:     {path}")
    print(f"SEI UUID: {sei_uuid}")
    print("Controls: [R] event Start  [S] event Stop  [Q/Ctrl+C] quit\n", flush=True)

    def request_stop(_signum: int, _frame: object) -> None:
        raise StopRequested

    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)
    source: subprocess.Popen[bytes] | None = None
    publisher: subprocess.Popen[bytes] | None = None
    pump_thread: threading.Thread | None = None
    stopped_by_user = False
    active_event_id: uuid.UUID | None = None

    try:
        publisher = subprocess.Popen(
            publisher_command,
            stdin=subprocess.PIPE,
            bufsize=0,
            start_new_session=(os.name == "posix"),
        )
        source = subprocess.Popen(
            source_command,
            stdout=subprocess.PIPE,
            bufsize=0,
            start_new_session=(os.name == "posix"),
        )
        assert source.stdout is not None
        assert publisher.stdin is not None

        pump_thread = threading.Thread(
            target=pump_h264,
            args=(source.stdout, publisher.stdin, pending_sei, pipeline_errors),
            name="h264-sei-pump",
            daemon=True,
        )
        pump_thread.start()

        try:
            with Keyboard() as keyboard:
                while source.poll() is None and publisher.poll() is None:
                    if not pipeline_errors.empty():
                        raise pipeline_errors.get()
                    key = keyboard.read(0.2)
                    if key is None:
                        continue
                    key = key.lower()
                    if key in ("r", "s"):
                        if key == "r":
                            if active_event_id is not None:
                                print(
                                    f"Event {active_event_id} is already active; press S before starting another.",
                                    flush=True,
                                )
                                continue
                            active_event_id = uuid.uuid4()
                            event_phase = "Start"
                        else:
                            if active_event_id is None:
                                print("No active event; press R first.", flush=True)
                                continue
                            event_phase = "Stop"

                        metadata: dict[str, object] = {
                            "event": {
                                "eventId": str(active_event_id),
                                "timestamp": int(time.time()),
                                "eventPhase": event_phase,
                                "eventType": args.event_type,
                                "objectType": args.object_type,
                            }
                        }
                        sei_nal, json_payload = build_sei_nal(sei_uuid, metadata)
                        pending_sei.put(sei_nal)
                        print(f"SEI queued: {json_payload.decode('utf-8')}", flush=True)
                        if event_phase == "Stop":
                            active_event_id = None
                    elif key == "q":
                        stopped_by_user = True
                        break
        except (KeyboardInterrupt, StopRequested):
            stopped_by_user = True
    except (OSError, BrokenPipeError) as exc:
        print(f"Streaming pipeline failed: {exc}", file=sys.stderr)
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        stop_process(source)
        if pump_thread is not None:
            pump_thread.join(timeout=1)
        stop_process(publisher)

    source_code = 0 if source is None or source.returncode is None else source.returncode
    publisher_code = 0 if publisher is None or publisher.returncode is None else publisher.returncode
    if stopped_by_user:
        print("Stream stopped.")
        return 0
    if source_code != 0 or publisher_code != 0:
        print(
            f"FFmpeg exited unexpectedly (source={source_code}, publisher={publisher_code}).",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
