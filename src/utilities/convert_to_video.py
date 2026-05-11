"""MP4 assembly via ffmpeg — standalone module so callers avoid importing all of utilities.tools."""

from __future__ import annotations

import os
import subprocess
import traceback


def convert_to_video(
    input_pattern: str,
    output_file: str,
    resolution: str = "1920:1080",
    loop_count: int = 2,
    framerate: int = 20,
    start_frame: int | None = None,
    frame_step: int = 1,
) -> None:
    """Convert a sequence of PNG files into an MP4 video using ffmpeg."""
    ffmpeg_cmd = (
        "/sw/spack-levante/mambaforge-22.9.0-2-Linux-x86_64-wuuo72/bin/ffmpeg"
    )

    input_args = ["-y"]

    if start_frame is not None:
        input_args.extend(["-start_number", str(start_frame)])

    if frame_step > 1:
        effective_framerate = framerate / frame_step
        input_args.extend(
            [
                "-framerate",
                str(framerate),
                "-i",
                input_pattern,
                "-vf",
                f"select='not(mod(n,{frame_step}))',setpts=N/({effective_framerate}*TB)",
            ]
        )
    else:
        input_args.extend(
            [
                "-stream_loop",
                str(loop_count),
                "-framerate",
                str(framerate),
                "-i",
                input_pattern,
            ]
        )

    output_args = [
        "-q:v",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-codec:v:0",
        "h264",
    ]

    if frame_step > 1:
        output_args.extend(["-vf", f"scale={resolution}"])
    else:
        output_args.extend(["-vf", f"scale={resolution}"])

    cmd = [ffmpeg_cmd] + input_args + output_args + [f"{output_file}"]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ffmpeg command: {' '.join(cmd)}")
        print(f"Exit code: {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    except FileNotFoundError:
        print(f"ffmpeg not found at path: {ffmpeg_cmd}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    finally:
        print(f"FFMPEG created MP4 file: {os.path.abspath(output_file)}")
