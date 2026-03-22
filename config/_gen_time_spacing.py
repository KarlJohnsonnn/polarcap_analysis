#!/usr/bin/env python3
"""Emit plotting.time_spacing_min lines for config/psd_process_evolution.yaml.

Segments (seconds from seed_start = 12:29:50):
  0–130 s step 1 s; 133–310 s step 3 s; 315–2110 s step 5 s (ends 13:05:00).
Run: python _gen_time_spacing.py
"""
from __future__ import annotations


def main() -> None:
    mins: list[float] = []
    for s in range(0, 131):
        mins.append(round(s / 60.0, 10))
    for s in range(133, 311, 3):
        mins.append(round(s / 60.0, 10))
    for s in range(315, 2111, 5):
        mins.append(round(s / 60.0, 10))

    def fmt(x: float) -> str:
        t = f"{x:.10f}".rstrip("0").rstrip(".")
        return t if t else "0"

    print("  time_spacing_min: [")
    for i in range(0, len(mins), 12):
        chunk = mins[i : i + 12]
        line = "      " + ", ".join(fmt(x) for x in chunk)
        if i + 12 < len(mins):
            line += ","
        print(line)
    print("    ]")
    print(f"# {len(mins)} boundaries → {len(mins) - 1} frames", file=__import__("sys").stderr)


if __name__ == "__main__":
    main()
