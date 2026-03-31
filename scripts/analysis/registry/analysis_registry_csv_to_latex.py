#!/usr/bin/env python3
"""Render analysis_registry.csv as a booktabs longtable for manuscript / supplementary material.

# defaults (same paths as build_analysis_registry.py)
    python3 scripts/analysis/registry/analysis_registry_csv_to_latex.py

# explicit paths
    python3 scripts/analysis/registry/analysis_registry_csv_to_latex.py path/to/analysis_registry.csv -o path/to/analysis_registry_table.tex
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CSV = REPO_ROOT / "output" / "tables" / "registry" / "analysis_registry.csv"
DEFAULT_TEX = REPO_ROOT / "output" / "tables" / "registry" / "tab_analysis_registry_table.tex"

# Must match columns written by build_analysis_registry.py (same order as the CSV header).
CSV_COLUMNS = [
    "cs_run",
    "exp_id",
    "expname",
    "is_reference",
    "flare_emission",
    "ishape",
    "ikeis",
    "domain",
    "remote_run_dir",
    "remote_json",
    "meteogram_count",
    "three_d_count",
    "lv1_ready",
    "lv2_ready",
    "local_lv1_available",
    "local_lv2_available",
    "local_lv3_available",
    "ref_exp_id",
    "ref_expname",
    "pair_method",
    "pair_status",
    "has_reference_pair",
    "ref_used_by_flare",
    "usable_for_lv1",
    "usable_for_lv2",
    "usable_for_plume_tracking",
    "usable_for_process_budget",
    "include_in_paper",
]

N_COLS = len(CSV_COLUMNS)

# Compact table layout (scope ends after \end{longtable}).
TABLE_FONT = r"\footnotesize"
TABLE_TABCOLSEP = "3pt"
TABLE_ARRAY_STRETCH = "0.88"

TEX_HEADER = r"""% Auto-generated from analysis_registry.csv. Edit scripts/analysis/registry/analysis_registry_csv_to_latex.py, not this file by hand.
%
% --- Required preamble (before \begin{document}) ---
% You cannot load these from inside \input{}; add them to the main .tex file.
%   \usepackage{booktabs}
%   \usepackage{longtable}
%   \usepackage{amssymb}
% Without longtable you get: "Environment longtable undefined", "\caption outside float",
% "Misplaced alignment tab character", and undefined booktabs rules (\toprule, \midrule).
%
% Optional (wide table): \usepackage{pdflscape} and \begin{landscape}...\end{landscape}
%
% This snippet wraps the longtable in a group: smaller font, reduced \tabcolsep, \arraystretch.
%

"""

PREAMBLE_LINES = (
    r"\begin{longtable}{@{}l c l c r c c l c c c c c c c c c c l l l c c c c c c@{}}",
    r"  \caption{Analysis registry of cloud simulations: experiment index, flare emission, domain configuration, remote-registry status, output counts, processing flags, reference pairing, and per-analysis inclusion gates. Checkmarks (\checkmark) denote true; em dash (---) denotes false or missing/not applicable.}",
    r"  \label{tab:analysis_registry} \\",
    r"  \toprule",
    r"  Cloud run &",
    r"  $i_{\mathrm{exp}}$ &",
    r"  Exp.\ ID &",
    r"  Ref. &",
    r"  $E_{\mathrm{flare}}$ &",
    r"  $i_{\mathrm{shape}}$ &",
    r"  $i_{\mathrm{keis}}$ &",
    r"  Domain &",
    r"  Run &",
    r"  JSON &",
    r"  $n_{\mathrm{met}}$ &",
    r"  $n_{3\mathrm{D}}$ &",
    r"  \multicolumn{1}{c}{L1} &",
    r"  \multicolumn{1}{c}{L2} &",
    r"  \multicolumn{1}{c}{loc.\ L1} &",
    r"  \multicolumn{1}{c}{loc.\ L2} &",
    r"  \multicolumn{1}{c}{loc.\ L3} &",
    r"  $i_{\mathrm{exp}}^{\mathrm{ref}}$ &",
    r"  Ref.\ ID &",
    r"  \multicolumn{1}{l}{Pair} &",
    r"  \multicolumn{1}{l}{Status} &",
    r"  \multicolumn{1}{c}{Paired} &",
    r"  \multicolumn{1}{c}{Fl.\ ref.} &",
    r"  \multicolumn{1}{c}{use L1} &",
    r"  \multicolumn{1}{c}{use L2} &",
    r"  \multicolumn{1}{c}{Plume} &",
    r"  \multicolumn{1}{c}{Budget} &",
    r"  \multicolumn{1}{c}{Paper} \\",
    r"  \midrule",
    r"  \endfirsthead",
    rf"  \multicolumn{{{N_COLS}}}{{c}}{{\tablename~\thetable{{}} --- \textit{{continued from previous page}}}} \\",
    r"  \toprule",
    r"  Cloud run &",
    r"  $i_{\mathrm{exp}}$ &",
    r"  Exp.\ ID &",
    r"  Ref. &",
    r"  $E_{\mathrm{flare}}$ &",
    r"  $i_{\mathrm{shape}}$ &",
    r"  $i_{\mathrm{keis}}$ &",
    r"  Domain &",
    r"  Run &",
    r"  JSON &",
    r"  $n_{\mathrm{met}}$ &",
    r"  $n_{3\mathrm{D}}$ &",
    r"  \multicolumn{1}{c}{L1} &",
    r"  \multicolumn{1}{c}{L2} &",
    r"  \multicolumn{1}{c}{loc.\ L1} &",
    r"  \multicolumn{1}{c}{loc.\ L2} &",
    r"  \multicolumn{1}{c}{loc.\ L3} &",
    r"  $i_{\mathrm{exp}}^{\mathrm{ref}}$ &",
    r"  Ref.\ ID &",
    r"  \multicolumn{1}{l}{Pair} &",
    r"  \multicolumn{1}{l}{Status} &",
    r"  \multicolumn{1}{c}{Paired} &",
    r"  \multicolumn{1}{c}{Fl.\ ref.} &",
    r"  \multicolumn{1}{c}{use L1} &",
    r"  \multicolumn{1}{c}{use L2} &",
    r"  \multicolumn{1}{c}{Plume} &",
    r"  \multicolumn{1}{c}{Budget} &",
    r"  \multicolumn{1}{c}{Paper} \\",
    r"  \midrule",
    r"  \endhead",
    r"  \midrule",
    rf"  \multicolumn{{{N_COLS}}}{{r}}{{\textit{{Continued on next page}}}} \\",
    r"  \endfoot",
    r"  \bottomrule",
    r"  \endlastfoot",
)


def _tex_escape(s: str) -> str:
    out = []
    for ch in s:
        if ch == "\\":
            out.append(r"\textbackslash{}")
        elif ch in _TEX_SPECIAL:
            out.append(_TEX_SPECIAL[ch])
        else:
            out.append(ch)
    return "".join(out)


_TEX_SPECIAL = {
    "&": r"\&",
    "%": r"\%",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
}


def _empty(s: str | None) -> bool:
    return s is None or str(s).strip() == ""


def _dash() -> str:
    return "---"


def _fmt_bool_cell(raw: str | None) -> str:
    u = (raw or "").strip().upper()
    if u in ("TRUE", "FALSE"):
        return r"\checkmark" if u == "TRUE" else _dash()
    if _empty(raw):
        return _dash()
    return _tex_escape(str(raw))


def _fmt_lv_ready(raw: str) -> str:
    if _empty(raw):
        return _dash()
    u = raw.strip().upper()
    if u in ("YES", "NO"):
        return "YES" if u == "YES" else "NO"
    return _tex_escape(raw.strip())


def _fmt_domain(raw: str) -> str:
    if _empty(raw):
        return _dash()
    return _tex_escape(raw.strip()).replace("x", r"$\times$")


def _fmt_num(raw: str) -> str:
    if _empty(raw):
        return _dash()
    try:
        x = float(raw)
        if x == int(x):
            return str(int(x))
        return f"{x:g}"
    except ValueError:
        return _tex_escape(str(raw).strip())


def _row_cells(row: dict[str, str]) -> list[str]:
    def g(key: str) -> str:
        return (row.get(key) or "").strip()

    cs = _tex_escape(g("cs_run"))
    out: list[str] = [r"\texttt{" + cs + "}"]
    out.append(_fmt_num(g("exp_id")))
    out.append(_tex_escape(g("expname")))
    out.append(_fmt_bool_cell(g("is_reference")))
    out.append(_fmt_num(g("flare_emission")))
    out.append(_fmt_num(g("ishape")))
    out.append(_fmt_num(g("ikeis")))
    out.append(_fmt_domain(g("domain")))
    out.append(_dash() if _empty(g("remote_run_dir")) else _tex_escape(g("remote_run_dir")))
    out.append(_dash() if _empty(g("remote_json")) else _tex_escape(g("remote_json")))
    out.append(_fmt_num(g("meteogram_count")))
    out.append(_fmt_num(g("three_d_count")))
    out.append(_fmt_lv_ready(g("lv1_ready")))
    out.append(_fmt_lv_ready(g("lv2_ready")))
    out.append(_fmt_bool_cell(g("local_lv1_available")))
    out.append(_fmt_bool_cell(g("local_lv2_available")))
    out.append(_fmt_bool_cell(g("local_lv3_available")))
    out.append(_fmt_num(g("ref_exp_id")) if not _empty(g("ref_exp_id")) else _dash())
    out.append(_dash() if _empty(g("ref_expname")) else _tex_escape(g("ref_expname")))
    out.append(_dash() if _empty(g("pair_method")) else _tex_escape(g("pair_method")))
    out.append(_dash() if _empty(g("pair_status")) else _tex_escape(g("pair_status")))
    out.append(_fmt_bool_cell(g("has_reference_pair")))
    out.append(_fmt_bool_cell(g("ref_used_by_flare")))
    out.append(_fmt_bool_cell(g("usable_for_lv1")))
    out.append(_fmt_bool_cell(g("usable_for_lv2")))
    out.append(_fmt_bool_cell(g("usable_for_plume_tracking")))
    out.append(_fmt_bool_cell(g("usable_for_process_budget")))
    out.append(_fmt_bool_cell(g("include_in_paper")))
    return out


def csv_to_latex_rows(rows: list[dict[str, str]]) -> list[str]:
    lines = []
    for row in rows:
        if not any((v or "").strip() for v in row.values()):
            continue
        cells = _row_cells(row)
        if len(cells) != N_COLS:
            raise RuntimeError(f"internal column mismatch: {len(cells)} vs {N_COLS}")
        lines.append(" & ".join(cells) + r" \\")
    return lines


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "csv_path",
        nargs="?",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Registry CSV (default: {DEFAULT_CSV})",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_TEX,
        help=f"Output .tex path (default: {DEFAULT_TEX})",
    )
    args = p.parse_args()
    csv_path: Path = args.csv_path
    if not csv_path.is_file():
        print(f"error: CSV not found: {csv_path}", file=sys.stderr)
        return 1

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != CSV_COLUMNS:
            print(
                "error: CSV columns do not match the expected header.\n"
                f"  expected: {CSV_COLUMNS}\n"
                f"  got:      {reader.fieldnames}",
                file=sys.stderr,
            )
            return 1
        rows = list(reader)

    body = "\n".join(csv_to_latex_rows(rows))
    out_text = (
        TEX_HEADER
        + "{\n"
        + TABLE_FONT
        + "\n"
        + rf"\setlength{{\tabcolsep}}{{{TABLE_TABCOLSEP}}}"
        + "\n"
        + rf"\renewcommand{{\arraystretch}}{{{TABLE_ARRAY_STRETCH}}}"
        + "\n"
        + "\n".join(PREAMBLE_LINES)
        + "\n"
        + body
        + "\n"
        + r"\end{longtable}"
        + "\n}\n"
    )

    out_path: Path = args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(out_text, encoding="utf-8")
    print(f"wrote {out_path} ({len(rows)} data rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
