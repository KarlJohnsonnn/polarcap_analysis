#!/usr/bin/env python3
# pyright: reportMissingImports=false
"""Build a browsable Markdown/HTML summary of the refreshed table outputs."""

from __future__ import annotations

import html
import math
import os
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = REPO_ROOT / "output" / "tables"
REPORT_DIR = OUTPUT_ROOT / "report"
ASSET_DIR = REPORT_DIR / "assets"
MANIFEST_PATH = REPO_ROOT / "scripts" / "analysis" / "synthesis" / "paper_tables.yaml"

TABLE_IDS = [
    "experiment_matrix",
    "initiation_metrics",
    "process_attribution",
    "growth_summary",
    "psd_stats_selected",
    "phase_budget_summary",
    "phase_budget_long",
]


def _read_csv(relpath: str) -> pd.DataFrame:
    return pd.read_csv(REPO_ROOT / relpath, dtype=str, keep_default_na=False)


def _read_manifest() -> dict[str, dict[str, Any]]:
    with MANIFEST_PATH.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return {spec["id"]: spec for spec in data.get("tables", [])}


def _relpath(path: Path) -> str:
    return os.path.relpath(path, start=REPORT_DIR).replace(os.sep, "/")


def _table_output_paths(table_id: str, spec: dict[str, Any]) -> dict[str, Path]:
    csv_path = REPO_ROOT / spec["csv_output"]
    tex_path = REPO_ROOT / spec["tex_output"]
    return {"csv": csv_path, "tex": tex_path}


def _clean_bool(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if text == "TRUE":
        return "Yes"
    if text == "FALSE":
        return "No"
    return str(value)


def _as_number(value: Any) -> float | None:
    if pd.isna(value) or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_value(value: Any, fmt: str | None) -> str:
    if pd.isna(value):
        return ""
    if fmt == "bool":
        return _clean_bool(value)
    if fmt == "int":
        number = _as_number(value)
        return "" if number is None else f"{int(round(number))}"
    if fmt == "float1":
        number = _as_number(value)
        return "" if number is None else f"{number:.1f}"
    if fmt == "float2":
        number = _as_number(value)
        return "" if number is None else f"{number:.2f}"
    if fmt == "sci1":
        number = _as_number(value)
        return "" if number is None else f"{number:.1e}"
    if fmt == "percent1":
        number = _as_number(value)
        return "" if number is None else f"{100.0 * number:.1f}%"
    text = str(value)
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


def _render_table_dataframe(df: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    columns = []
    for column in spec.get("display_columns", []):
        name = column["name"]
        label = column.get("label", name)
        fmt = column.get("fmt")
        rendered = df[name].map(lambda value, fmt=fmt: _format_value(value, fmt)) if name in df.columns else ""
        columns.append((label, rendered))
    display_df = pd.DataFrame({label: series for label, series in columns})
    return display_df


def _markdown_table(df: pd.DataFrame, *, limit: int | None = None) -> str:
    if df.empty:
        return "_No rows._"
    table_df = df.head(limit) if limit is not None else df
    headers = [str(col) for col in table_df.columns]
    rows = [[str(value) if value != "" else "" for value in row] for row in table_df.to_numpy()]
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))
    header_line = "| " + " | ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers)) + " |"
    sep_line = "| " + " | ".join("-" * widths[idx] for idx in range(len(headers))) + " |"
    body = [
        "| " + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(headers))) + " |"
        for row in rows
    ]
    return "\n".join([header_line, sep_line, *body])


def _html_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>No rows.</p>"
    head = "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns)
    rows = []
    for _, row in df.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row)
        rows.append(f"<tr>{cells}</tr>")
    return (
        '<div class="table-wrap"><table>'
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def _setup_axes_style(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", color="#d7dbe0", linewidth=0.8, alpha=0.7)
    ax.set_axisbelow(True)


def _save_svg(fig: plt.Figure, name: str) -> Path:
    path = ASSET_DIR / name
    fig.savefig(path, format="svg", bbox_inches="tight")
    plt.close(fig)
    return path


def _plot_coverage_summary(registry: pd.DataFrame) -> Path:
    subset = registry[registry["include_in_paper"].astype(str).str.upper() == "TRUE"].copy()
    counts = pd.Series(
        {
            "Paper subset rows": int(len(subset)),
            "Matched flare/reference": int((subset["has_reference_pair"].astype(str).str.upper() == "TRUE").sum()),
            "Local LV1": int((subset["local_lv1_available"].astype(str).str.upper() == "TRUE").sum()),
            "Local LV2": int((subset["local_lv2_available"].astype(str).str.upper() == "TRUE").sum()),
            "Local LV3": int((subset["local_lv3_available"].astype(str).str.upper() == "TRUE").sum()),
        }
    )
    fig, ax = plt.subplots(figsize=(7.4, 3.8))
    colors = ["#264653", "#2a9d8f", "#457b9d", "#e9c46a", "#e76f51"]
    ax.barh(counts.index[::-1], counts.values[::-1], color=colors[::-1])
    ax.set_xlabel("Count")
    ax.set_title("Paper Subset And Local Coverage")
    ax.set_xlim(0, max(counts.max() + 2, 5))
    for idx, value in enumerate(counts.values[::-1]):
        ax.text(value + 0.3, idx, f"{value}", va="center", fontsize=9)
    _setup_axes_style(ax)
    return _save_svg(fig, "coverage_summary.svg")


def _plot_initiation_summary(initiation: pd.DataFrame) -> Path:
    df = initiation.copy()
    df["label"] = df["expname"].astype(str)
    df["ice_onset_median_min_since_seed"] = pd.to_numeric(
        df["ice_onset_median_min_since_seed"], errors="coerce"
    )
    df["peak_excess_ice_number_m3_max"] = pd.to_numeric(df["peak_excess_ice_number_m3_max"], errors="coerce")
    df = df.sort_values(["ice_onset_median_min_since_seed", "peak_excess_ice_number_m3_max"], na_position="last")
    plot_df = df.dropna(subset=["ice_onset_median_min_since_seed"]).copy()
    fig, ax = plt.subplots(figsize=(7.6, 4.3))
    sizes = 50 + 170 * (
        plot_df["peak_excess_ice_number_m3_max"].fillna(0.0)
        / max(plot_df["peak_excess_ice_number_m3_max"].max(), 1.0)
    )
    ax.scatter(
        plot_df["ice_onset_median_min_since_seed"],
        plot_df["peak_excess_ice_number_m3_max"],
        s=sizes,
        color="#1d3557",
        alpha=0.85,
    )
    for row in plot_df.itertuples(index=False):
        ax.annotate(
            str(row.expname),
            (float(row.ice_onset_median_min_since_seed), float(row.peak_excess_ice_number_m3_max)),
            xytext=(5, 4),
            textcoords="offset points",
            fontsize=8,
        )
    ax.set_xlabel("Median excess-ice onset [min since seed]")
    ax.set_ylabel("Peak excess ice [m$^{-3}$]")
    ax.set_title("Initiation Timing Versus Peak Excess Ice")
    _setup_axes_style(ax)
    return _save_svg(fig, "initiation_summary.svg")


def _plot_growth_summary(growth: pd.DataFrame) -> Path:
    df = growth.copy()
    df["alpha_mean_median"] = pd.to_numeric(df["alpha_mean_median"], errors="coerce")
    df["ridge_peak_end_um"] = pd.to_numeric(df["ridge_peak_end_um"], errors="coerce")
    df["n_cells"] = pd.to_numeric(df["n_cells"], errors="coerce")
    fig, ax = plt.subplots(figsize=(7.4, 4.2))
    sizes = 60 + 45 * df["n_cells"].fillna(1.0)
    ax.scatter(df["ridge_peak_end_um"], df["alpha_mean_median"], s=sizes, color="#a23b72", alpha=0.8)
    for row in df.itertuples(index=False):
        ax.annotate(str(row.expname), (float(row.ridge_peak_end_um), float(row.alpha_mean_median)), xytext=(5, 4), textcoords="offset points", fontsize=8)
    ax.set_xlabel("Ridge peak end diameter [um]")
    ax.set_ylabel("Median alpha (mean ridge)")
    ax.set_title("Growth-Regime Summary For Promoted Runs")
    _setup_axes_style(ax)
    return _save_svg(fig, "growth_summary.svg")


def _time_midpoint(label: str) -> float:
    parts = re.split(r"\s*-\s*", str(label))
    values = [float(part) for part in parts if part]
    return sum(values) / len(values) if values else math.nan


def _plot_psd_selected(psd: pd.DataFrame) -> Path:
    df = psd.copy()
    df["time_mid_min"] = df["time_frame_min"].map(_time_midpoint)
    for column in ["liq_mean_diam_um", "ice_mean_diam_um", "alpha_ice_mean_diam"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7.6, 5.0), sharex=True, height_ratios=[2.2, 1.2])
    ax1.plot(df["time_mid_min"], df["liq_mean_diam_um"], marker="o", color="#457b9d", label="Liquid mean")
    ax1.plot(df["time_mid_min"], df["ice_mean_diam_um"], marker="o", color="#e76f51", label="Ice mean")
    ax1.set_ylabel("Mean diameter [um]")
    ax1.set_title(f"Selected PSD Windows ({df['selected_run_id'].iloc[0]})")
    ax1.legend(frameon=False, ncol=2, loc="upper left")
    _setup_axes_style(ax1)
    ax2.plot(df["time_mid_min"], df["alpha_ice_mean_diam"], marker="s", color="#2a9d8f")
    ax2.set_xlabel("Window midpoint [min since seed]")
    ax2.set_ylabel("Alpha")
    _setup_axes_style(ax2)
    return _save_svg(fig, "psd_selected.svg")


def _plot_phase_budget(phase_budget: pd.DataFrame) -> Path:
    df = phase_budget.copy()
    for column in ["liq_source_g_m2", "liq_sink_g_m2", "ice_sink_g_m2"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    x = range(len(df))
    width = 0.24
    fig, ax = plt.subplots(figsize=(7.6, 4.4))
    ax.bar([i - width for i in x], df["liq_source_g_m2"], width=width, color="#2a9d8f", label="Liquid source")
    ax.bar(list(x), df["liq_sink_g_m2"], width=width, color="#457b9d", label="Liquid sink")
    ax.bar([i + width for i in x], df["ice_sink_g_m2"], width=width, color="#e76f51", label="Ice sink")
    ax.axhline(0.0, color="#3d405b", linewidth=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels(df["station"].astype(str))
    ax.set_ylabel("Net contribution [g m$^{-2}$]")
    ax.set_title("Promoted Phase-Budget Context")
    ax.legend(frameon=False, ncol=3, loc="upper left")
    _setup_axes_style(ax)
    return _save_svg(fig, "phase_budget_summary.svg")


def _plot_process_attribution(process_df: pd.DataFrame) -> Path:
    df = process_df.copy()
    ice_counts = df["ice_dominant_process"].fillna("").value_counts().sort_values(ascending=True)
    liq_counts = df["liq_dominant_process"].fillna("").value_counts().sort_values(ascending=True)
    labels = sorted(set(ice_counts.index) | set(liq_counts.index))
    ice_values = [int(ice_counts.get(label, 0)) for label in labels]
    liq_values = [int(liq_counts.get(label, 0)) for label in labels]
    y = range(len(labels))
    fig, ax = plt.subplots(figsize=(8.0, 4.8))
    ax.barh([i - 0.18 for i in y], ice_values, height=0.34, color="#e76f51", label="Ice dominant")
    ax.barh([i + 0.18 for i in y], liq_values, height=0.34, color="#457b9d", label="Liquid dominant")
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Count across promoted station rows")
    ax.set_title("Dominant Early-Window Processes")
    ax.legend(frameon=False, loc="lower right")
    _setup_axes_style(ax)
    return _save_svg(fig, "process_attribution.svg")


def _summary_cards(registry: pd.DataFrame, claim_register: pd.DataFrame, sync_inventory: pd.DataFrame) -> list[tuple[str, str]]:
    subset = registry[registry["include_in_paper"].astype(str).str.upper() == "TRUE"]
    flare_rows = int((subset["is_reference"].astype(str).str.upper() == "FALSE").sum())
    supported_claims = int((claim_register["evidence_status"].astype(str).str.lower() == "supported").sum())
    missing_stages = int((sync_inventory["status"].astype(str) == "missing").sum())
    return [
        ("Paper subset rows", str(len(subset))),
        ("Flare rows", str(flare_rows)),
        ("Supported claims", str(supported_claims)),
        ("Remaining stage gaps", str(missing_stages)),
        ("Local LV2 rows", str(int((subset["local_lv2_available"].astype(str).str.upper() == "TRUE").sum()))),
        ("Local LV3 rows", str(int((subset["local_lv3_available"].astype(str).str.upper() == "TRUE").sum()))),
    ]


def _figure_inventory_context(figure_inventory: pd.DataFrame) -> pd.DataFrame:
    cols = ["artifact_id", "script", "output_path", "caption"]
    fig_df = figure_inventory[figure_inventory["artifact_type"] == "figure"][cols].copy()
    fig_df = fig_df.fillna("")
    return fig_df


def _figure_caption_lookup(figure_inventory: pd.DataFrame) -> dict[str, str]:
    fig_df = figure_inventory[figure_inventory["artifact_type"] == "figure"].fillna("")
    return {
        str(row.artifact_id): str(row.caption).strip()
        for row in fig_df.itertuples(index=False)
        if str(row.caption).strip()
    }


def _regenerated_figure_assets(
    figure_inventory: pd.DataFrame,
    psd_selected: pd.DataFrame,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    captions = _figure_caption_lookup(figure_inventory)
    images: list[dict[str, str]] = []
    links: list[dict[str, str]] = []

    static_candidates = [
        (
            "Cloud Overview",
            "fig_cloud_field_overview",
            REPO_ROOT / "output" / "gfx" / "png" / "01" / "cloud_field_overview_mass_profiles_steps_symlog_20260304110446_ALLBB.png",
            "Regenerated manuscript overview figure for the promoted `20260304110446` ALLBB case.",
        ),
        (
            "Plume-Lagrangian Evolution",
            "fig_plume_lagrangian",
            REPO_ROOT / "output" / "gfx" / "png" / "03" / "figure12_ensemble_mean_plume_path_foo.png",
            "Regenerated ensemble plume-lagrangian comparison figure with HOLIMO overlays.",
        ),
    ]

    selected_run_id = str(psd_selected["selected_run_id"].iloc[0]).strip() if not psd_selected.empty else ""
    if selected_run_id:
        static_candidates.extend(
            [
                (
                    "PSD Waterfall (Mass)",
                    "fig_psd_waterfall",
                    REPO_ROOT / "output" / "gfx" / "png" / "04" / f"figure13_psd_alt_time_mass_{selected_run_id}.png",
                    f"Regenerated mass-basis PSD waterfall for selected run `{selected_run_id}`.",
                ),
                (
                    "PSD Waterfall (Number)",
                    "fig_psd_waterfall",
                    REPO_ROOT / "output" / "gfx" / "png" / "04" / f"figure13_psd_alt_time_number_{selected_run_id}.png",
                    f"Regenerated number-basis PSD waterfall for selected run `{selected_run_id}`.",
                ),
            ]
        )

    for title, artifact_id, path, fallback in static_candidates:
        if path.exists():
            images.append(
                {
                    "title": title,
                    "description": captions.get(artifact_id, fallback),
                    "path": _relpath(path),
                }
            )

    spectral_path = REPO_ROOT / "output" / "gfx" / "html" / "05" / "cs-eriswil__20260304_110254" / "ridge_growth_Q_stn0_interactive.html"
    if spectral_path.exists():
        links.append(
            {
                "title": "Spectral Growth Interactive HTML",
                "description": captions.get(
                    "fig_spectral_waterfall",
                    "Interactive ridge-following spectral-growth artifact for the refreshed workflow.",
                ),
                "path": _relpath(spectral_path),
            }
        )

    return images, links


def _render_table_section_markdown(
    table_id: str,
    spec: dict[str, Any],
    display_df: pd.DataFrame,
    csv_path: Path,
    tex_path: Path,
) -> str:
    preview_rows = min(len(display_df), 10)
    more_note = "" if len(display_df) <= preview_rows else f"\n\nShowing first {preview_rows} of {len(display_df)} rows."
    return "\n".join(
        [
            f"### {spec['id']}",
            "",
            spec["caption"],
            "",
            f"- Rows: `{len(display_df)}`",
            f"- CSV: `{_relpath(csv_path)}`",
            f"- TeX: `{_relpath(tex_path)}`",
            "",
            _markdown_table(display_df, limit=preview_rows),
            more_note,
            "",
        ]
    )


def _render_table_section_html(
    table_id: str,
    spec: dict[str, Any],
    display_df: pd.DataFrame,
    csv_path: Path,
    tex_path: Path,
) -> str:
    return f"""
<details class="table-card" {'open' if table_id in {'experiment_matrix', 'initiation_metrics', 'growth_summary'} else ''}>
  <summary>{html.escape(spec['id'])} <span>{len(display_df)} rows</span></summary>
  <p class="caption">{html.escape(spec['caption'])}</p>
  <p class="links">
    <a href="{html.escape(_relpath(csv_path))}">CSV</a>
    <a href="{html.escape(_relpath(tex_path))}">TeX</a>
  </p>
  { _html_table(display_df) }
</details>
""".strip()


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ASSET_DIR.mkdir(parents=True, exist_ok=True)

    manifest = _read_manifest()
    analysis_registry = _read_csv("output/tables/registry/analysis_registry.csv")
    claim_register = _read_csv("output/tables/registry/claim_register.csv")
    sync_inventory = _read_csv("output/tables/registry/processed_sync_inventory.csv")
    figure_inventory = _read_csv("output/tables/registry/figure_inventory.csv")
    processing_log = (OUTPUT_ROOT / "registry" / "processing_refresh_log.md").read_text(encoding="utf-8")
    processed_sync_report = (OUTPUT_ROOT / "registry" / "processed_sync_report.md").read_text(encoding="utf-8")

    cards = _summary_cards(analysis_registry, claim_register, sync_inventory)
    psd_selected_df = _read_csv("output/tables/paper/tab_psd_stats_selected.csv")
    regenerated_images, regenerated_links = _regenerated_figure_assets(figure_inventory, psd_selected_df)
    figures = [
        ("Coverage summary", "Paper subset rows, matched references, and local LV1/LV2/LV3 availability.", _plot_coverage_summary(analysis_registry)),
        ("Initiation summary", "Median excess-ice onset against peak excess ice for promoted flare runs with onset detections.", _plot_initiation_summary(_read_csv("output/tables/paper/tab_initiation_metrics.csv"))),
        ("Process attribution", "Counts of dominant early-window ice and liquid pathways across the promoted station rows.", _plot_process_attribution(_read_csv("output/tables/paper/tab_process_attribution.csv"))),
        ("Growth summary", "Promoted LV1 ridge-growth cases in diameter-alpha space.", _plot_growth_summary(_read_csv("output/tables/paper/tab_growth_summary.csv"))),
        ("Selected PSD windows", "Liquid and ice mean diameters plus alpha across the featured PSD time windows.", _plot_psd_selected(psd_selected_df)),
        ("Phase-budget context", "Compact source/sink magnitudes for the promoted `20260304110446` overview case.", _plot_phase_budget(_read_csv("output/tables/paper/tab_phase_budget_summary.csv"))),
    ]

    table_sections_md: list[str] = []
    table_sections_html: list[str] = []
    for table_id in TABLE_IDS:
        spec = manifest[table_id]
        paths = _table_output_paths(table_id, spec)
        raw_df = pd.read_csv(paths["csv"], dtype=str, keep_default_na=False)
        display_df = _render_table_dataframe(raw_df, spec)
        table_sections_md.append(_render_table_section_markdown(table_id, spec, display_df, paths["csv"], paths["tex"]))
        table_sections_html.append(_render_table_section_html(table_id, spec, display_df, paths["csv"], paths["tex"]))

    figure_rows = _figure_inventory_context(figure_inventory)
    claim_cols = ["claim_id", "claim_text", "evidence_value", "source_table", "evidence_status"]
    claim_df = claim_register[claim_cols].copy()
    claim_df.columns = ["Claim ID", "Claim", "Evidence", "Source table", "Status"]
    figure_df = figure_rows.copy()
    figure_df.columns = ["Artifact", "Script", "Output target", "Caption"]

    missing_df = sync_inventory[sync_inventory["status"] == "missing"].copy()
    missing_df = missing_df[["cs_run", "stage", "note", "raw_m_count", "raw_3d_count"]].head(12)
    missing_df.columns = ["Run", "Stage", "Gap note", "M files", "3D files"]

    markdown = "\n".join(
        [
            "# PolarCAP Refresh Report",
            "",
            "This report summarizes the refreshed canonical outputs under `output/tables/`.",
            "",
            f"- HTML version: `{_relpath(REPORT_DIR / 'analysis_refresh_report.html')}`",
            f"- Processing log: `{_relpath(OUTPUT_ROOT / 'registry' / 'processing_refresh_log.md')}`",
            f"- Processed sync report: `{_relpath(OUTPUT_ROOT / 'registry' / 'processed_sync_report.md')}`",
            "",
            "## Key Numbers",
            "",
            *[f"- **{title}:** {value}" for title, value in cards],
            "",
            "## Context",
            "",
            "- Outputs were rebuilt from the canonical `data/processed/` view and written to `output/tables/`.",
            "- Missing remote LV3 rates for `cs-eriswil__20260304_110254` were regenerated locally before the phase-budget tables were rebuilt.",
            "- Legacy manuscript table paths in `data/registry/paper_tables/` remain synchronized with the canonical paper table directory.",
            "- The report now includes regenerated manuscript PNG figures where available, followed by derived SVG summaries from the refreshed canonical CSV tables.",
            "",
            "## Regenerated Figure Artifacts",
            "",
            *[
                "\n".join(
                    [
                        f"### {item['title']}",
                        "",
                        item["description"],
                        "",
                        f"![{item['title']}]({item['path']})",
                        "",
                    ]
                )
                for item in regenerated_images
            ],
            *[
                "\n".join(
                    [
                        f"### {item['title']}",
                        "",
                        item["description"],
                        "",
                        f"[Open interactive artifact]({item['path']})",
                        "",
                    ]
                )
                for item in regenerated_links
            ],
            "",
            "## Derived Figures",
            "",
            *[
                "\n".join(
                    [
                        f"### {title}",
                        "",
                        desc,
                        "",
                        f"![{title}]({_relpath(path)})",
                        "",
                    ]
                )
                for title, desc, path in figures
            ],
            "## Claim Snapshot",
            "",
            _markdown_table(claim_df, limit=min(len(claim_df), 8)),
            "",
            "## Manuscript Figure Context",
            "",
            _markdown_table(figure_df, limit=min(len(figure_df), 6)),
            "",
            "## Paper Tables",
            "",
            *table_sections_md,
            "## Remaining Gaps",
            "",
            _markdown_table(missing_df, limit=min(len(missing_df), 12)),
            "",
            "## Source Logs",
            "",
            "### Processing Refresh Log",
            "",
            "```markdown",
            processing_log.rstrip(),
            "```",
            "",
            "### Processed Sync Report",
            "",
            "```markdown",
            processed_sync_report.rstrip(),
            "```",
            "",
        ]
    )

    html_cards = "".join(
        f'<div class="card"><div class="card-value">{html.escape(value)}</div><div class="card-label">{html.escape(title)}</div></div>'
        for title, value in cards
    )
    html_figures = "".join(
        f"""
<figure class="figure-card">
  <img src="{html.escape(_relpath(path))}" alt="{html.escape(title)}">
  <figcaption><strong>{html.escape(title)}</strong><span>{html.escape(desc)}</span></figcaption>
</figure>
""".strip()
        for title, desc, path in figures
    )
    html_regenerated = "".join(
        f"""
<figure class="figure-card">
  <img src="{html.escape(item['path'])}" alt="{html.escape(item['title'])}">
  <figcaption><strong>{html.escape(item['title'])}</strong><span>{html.escape(item['description'])}</span></figcaption>
</figure>
""".strip()
        for item in regenerated_images
    )
    html_regenerated_links = "".join(
        f"""
<div class="link-card">
  <strong>{html.escape(item['title'])}</strong>
  <span>{html.escape(item['description'])}</span>
  <a href="{html.escape(item['path'])}">Open interactive artifact</a>
</div>
""".strip()
        for item in regenerated_links
    )
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>PolarCAP Refresh Report</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fa;
      --panel: #ffffff;
      --ink: #14213d;
      --muted: #5c677d;
      --line: #d9dee7;
      --accent: #1d3557;
      --accent2: #2a9d8f;
      --shadow: 0 10px 25px rgba(20, 33, 61, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #eef3f8 0%, var(--bg) 100%);
      line-height: 1.5;
    }}
    main {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 24px 64px;
    }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    p, ul {{ margin: 0 0 16px; }}
    .hero {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 28px;
      box-shadow: var(--shadow);
      margin-bottom: 24px;
    }}
    .hero p {{ color: var(--muted); max-width: 960px; }}
    .hero-links a {{
      display: inline-block;
      margin-right: 12px;
      color: var(--accent);
      text-decoration: none;
      font-weight: 600;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 14px;
      margin: 24px 0;
    }}
    .card {{
      background: linear-gradient(135deg, #ffffff 0%, #f7fbff 100%);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px 16px;
      box-shadow: var(--shadow);
    }}
    .card-value {{
      font-size: 1.9rem;
      font-weight: 700;
      color: var(--accent);
    }}
    .card-label {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 24px;
      box-shadow: var(--shadow);
      margin-bottom: 22px;
    }}
    .figure-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 18px;
    }}
    .figure-card {{
      margin: 0;
      border: 1px solid var(--line);
      border-radius: 16px;
      overflow: hidden;
      background: #fff;
    }}
    .figure-card img {{
      width: 100%;
      display: block;
      background: #fff;
    }}
    .figure-card figcaption {{
      display: block;
      padding: 14px 16px 16px;
    }}
    .figure-card figcaption strong {{
      display: block;
      margin-bottom: 6px;
    }}
    .figure-card figcaption span {{
      color: var(--muted);
      font-size: 0.93rem;
    }}
    .link-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      background: #fff;
    }}
    .link-card strong {{
      display: block;
      margin-bottom: 8px;
    }}
    .link-card span {{
      display: block;
      color: var(--muted);
      margin-bottom: 10px;
      font-size: 0.93rem;
    }}
    .link-card a {{
      color: var(--accent);
      font-weight: 600;
      text-decoration: none;
    }}
    .table-wrap {{
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
      min-width: 760px;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    thead th {{
      position: sticky;
      top: 0;
      background: #f8fbfd;
      z-index: 1;
    }}
    tbody tr:nth-child(even) {{
      background: #fbfcfe;
    }}
    details.table-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      margin-bottom: 16px;
      background: #fff;
    }}
    details.table-card summary {{
      cursor: pointer;
      font-weight: 700;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    details.table-card summary span {{
      color: var(--muted);
      font-weight: 500;
      font-size: 0.92rem;
    }}
    .caption {{
      color: var(--muted);
      margin-top: 12px;
    }}
    .links a {{
      margin-right: 14px;
      color: var(--accent);
      text-decoration: none;
      font-weight: 600;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      background: #0f172a;
      color: #e5edf7;
      border-radius: 14px;
      padding: 16px;
      overflow-x: auto;
    }}
    .two-col {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    @media (max-width: 900px) {{
      .two-col {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>PolarCAP Refresh Report</h1>
      <p>This report visualizes the refreshed canonical outputs under <code>output/tables/</code>. The figures below are derived from the rebuilt CSV tables so they reflect the current refresh even when legacy PNG figure artifacts were not regenerated in this pass.</p>
      <p class="hero-links">
        <a href="{html.escape(_relpath(OUTPUT_ROOT / 'registry' / 'processing_refresh_log.md'))}">Processing log</a>
        <a href="{html.escape(_relpath(OUTPUT_ROOT / 'registry' / 'processed_sync_report.md'))}">Processed sync report</a>
        <a href="{html.escape(_relpath(REPORT_DIR / 'analysis_refresh_report.md'))}">Markdown version</a>
      </p>
      <div class="cards">{html_cards}</div>
    </section>

    <section>
      <h2>Context</h2>
      <ul>
        <li>Canonical manuscript-facing tables now live under <code>output/tables/</code>, with compatibility mirrors preserved in <code>data/registry/paper_tables/</code>.</li>
        <li>The local processed-data view in <code>data/processed/</code> was refreshed from the approved <code>/work</code> and <code>/scratch</code> roots using symlinks where possible.</li>
        <li>Missing remote LV3 rates for <code>cs-eriswil__20260304_110254</code> were rebuilt locally before the cloud phase-budget tables were regenerated.</li>
        <li>The regenerated PNG figures below come from the refreshed workflow; the derived SVG summaries remain useful for quick inspection across tables.</li>
      </ul>
    </section>

    <section>
      <h2>Regenerated Figure Artifacts</h2>
      <div class="figure-grid">{html_regenerated}{html_regenerated_links}</div>
    </section>

    <section>
      <h2>Derived Figures</h2>
      <div class="figure-grid">{html_figures}</div>
    </section>

    <section class="two-col">
      <div>
        <h2>Claim Snapshot</h2>
        {_html_table(claim_df)}
      </div>
      <div>
        <h2>Manuscript Figure Context</h2>
        {_html_table(figure_df.head(6))}
      </div>
    </section>

    <section>
      <h2>Paper Tables</h2>
      {''.join(table_sections_html)}
    </section>

    <section class="two-col">
      <div>
        <h2>Remaining Gaps</h2>
        {_html_table(missing_df)}
      </div>
      <div>
        <h2>Source Logs</h2>
        <h3>Processing Refresh Log</h3>
        <pre>{html.escape(processing_log.rstrip())}</pre>
        <h3>Processed Sync Report</h3>
        <pre>{html.escape(processed_sync_report.rstrip())}</pre>
      </div>
    </section>
  </main>
</body>
</html>
"""

    (REPORT_DIR / "analysis_refresh_report.md").write_text(markdown, encoding="utf-8")
    (REPORT_DIR / "analysis_refresh_report.html").write_text(html_doc, encoding="utf-8")
    print(f"Wrote {(REPORT_DIR / 'analysis_refresh_report.md').relative_to(REPO_ROOT)}")
    print(f"Wrote {(REPORT_DIR / 'analysis_refresh_report.html').relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
