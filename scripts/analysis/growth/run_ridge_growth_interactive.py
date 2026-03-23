#!/usr/bin/env python3
"""Build an interactive HTML dashboard for ridge-growth CSVs (spectral-waterfall export).

Options: ice_ok masking, heatmap colorscale, diameter y-scale, time-window clip, env ridge
meteorology (when CSV columns exist), robust y-limit presets plus native zoom/pan/double-click
reset (editable view, not stored).
"""
from __future__ import annotations

import argparse
import sys
import webbrowser
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
GROWTH_DIR = Path(__file__).resolve().parent
if str(GROWTH_DIR) not in sys.path:
    sys.path.insert(0, str(GROWTH_DIR))

import run_ridge_growth_quicklook as rgq  # noqa: E402

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "plotly is required: pip install plotly>=5.18"
    ) from exc

DEFAULT_INPUT = rgq.DEFAULT_INPUT
C_LIQ = "#4C72B0"
C_ICE = "#DD8452"
C_ANCHOR = "#595959"

# Optional CSV columns written when growth_csv_include_environment is on (see spectral_waterfall).
ENV_COLS = (
    "T_ridge_K",
    "w_ridge_m_s",
    "S_wat_ridge",
    "S_ice_ridge",
    "vfall_liq_ridge_m_s",
    "vfall_ice_ridge_m_s",
    "v_hydr_ridge_m_s",  # legacy single column from older CSVs
)


def _default_html_path(input_csv: Path) -> Path:
    return REPO_ROOT / "output" / "gfx" / "html" / "05" / input_csv.parent.name / f"{input_csv.stem}_interactive.html"


def _finite_vals(a: np.ndarray) -> np.ndarray:
    v = np.asarray(a, dtype=float).ravel()
    return v[np.isfinite(v)]


def _robust_mad_range(a: np.ndarray) -> tuple[float, float]:
    v = _finite_vals(a)
    if v.size == 0:
        return -1.0, 1.0
    med = float(np.median(v))
    mad = float(np.median(np.abs(v - med)))
    scale = 1.4826 * mad if mad > 0 else (float(np.std(v)) if np.std(v) > 0 else 1.0)
    lo, hi = med - 4.0 * scale, med + 4.0 * scale
    p5, p95 = np.percentile(v, [5.0, 95.0])
    span = max(p95 - p5, 1e-9)
    lo = max(lo, p5 - 0.5 * span)
    hi = min(hi, p95 + 0.5 * span)
    pad = 0.05 * (hi - lo) if hi > lo else 1.0
    return lo - pad, hi + pad


def _percentile_range(a: np.ndarray, lo_q: float, hi_q: float) -> tuple[float, float]:
    v = _finite_vals(a)
    if v.size == 0:
        return -1.0, 1.0
    lo, hi = np.percentile(v, [lo_q, hi_q])
    pad = 0.02 * (hi - lo) if hi > lo else 1.0
    return float(lo - pad), float(hi + pad)


def _full_finite_range(a: np.ndarray) -> tuple[float, float]:
    v = _finite_vals(a)
    if v.size == 0:
        return -1.0, 1.0
    lo, hi = float(np.min(v)), float(np.max(v))
    pad = 0.02 * (hi - lo) if hi > lo else 1.0
    return lo - pad, hi + pad


def _xaxis_keys_time_from_start(fig: go.Figure) -> list[str]:
    keys: list[str] = []
    lay = fig.to_dict().get("layout", {})
    for k, v in lay.items():
        if not isinstance(k, str) or not k.startswith("xaxis"):
            continue
        if not isinstance(v, dict):
            continue
        title = v.get("title")
        if isinstance(title, dict):
            txt = title.get("text") or ""
        else:
            txt = str(title or "")
        if txt and "time from start" in txt:
            keys.append(k)

    def _sort_key(name: str) -> tuple[int, str]:
        rest = name[5:]
        return (0 if rest == "" else int(rest), name)

    return sorted(keys, key=_sort_key) if keys else ["xaxis", "xaxis2"]


def _time_window_relayout(fig: go.Figure, t_lo_min: float, t_hi_min: float) -> dict:
    hi = float(t_hi_min)
    lo = float(t_lo_min)
    if lo >= hi:
        lo = max(0.0, hi - 1e-3)
    out: dict[str, list[float]] = {}
    for k in _xaxis_keys_time_from_start(fig):
        out[f"{k}.range"] = [lo, hi]
    return out


def _build_figure(df: pd.DataFrame, *, title: str) -> go.Figure:
    t_min = df["t_mid_min"].to_numpy(dtype=float)
    d_liq = pd.to_numeric(df["D_liq_um"], errors="coerce").to_numpy(dtype=float)
    d_ice = pd.to_numeric(df["D_ice_um"], errors="coerce").to_numpy(dtype=float)
    g_liq = pd.to_numeric(df["dD_liq_dt_um_s"], errors="coerce").to_numpy(dtype=float)
    g_ice = pd.to_numeric(df["dD_ice_dt_um_s"], errors="coerce").to_numpy(dtype=float)
    z_ridge = pd.to_numeric(df["z_ridge_m"], errors="coerce").to_numpy(dtype=float)
    z_anchor = pd.to_numeric(df["z_anchor_m"], errors="coerce").to_numpy(dtype=float)
    ice_ok = df["ice_ok"].to_numpy()

    env_present = {c: pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float) for c in ENV_COLS if c in df.columns}
    upper_env = any(c in env_present for c in ("T_ridge_K", "w_ridge_m_s", "S_wat_ridge", "S_ice_ridge"))
    has_v = any(
        k in env_present for k in ("vfall_liq_ridge_m_s", "vfall_ice_ridge_m_s", "v_hydr_ridge_m_s")
    )
    has_env = bool(env_present)
    if has_env:
        env_extra_rows = 2 if upper_env and has_v else 1
    else:
        env_extra_rows = 0

    def ice_mask(arr: np.ndarray) -> np.ndarray:
        out = arr.astype(float, copy=True)
        out[~ice_ok] = np.nan
        return out

    d_liq_ice = ice_mask(d_liq)
    d_ice_ice = ice_mask(d_ice)
    g_liq_ice = ice_mask(g_liq)
    g_ice_ice = ice_mask(g_ice)
    z_ridge_ice = ice_mask(z_ridge)
    z_anchor_ice = ice_mask(z_anchor)

    heat = np.vstack(
        [
            rgq._robust_zscore(pd.to_numeric(df[key], errors="coerce").to_numpy(dtype=float))
            for key, _ in rgq.HEATMAP_METRICS
        ]
    )
    y_labels = [lbl for _k, lbl in rgq.HEATMAP_METRICS]

    dt = rgq._time_weights_minutes(t_min)
    t_edges = np.empty(len(t_min) + 1, dtype=float)
    t_edges[0] = max(0.0, t_min[0] - 0.5 * dt[0]) if len(t_min) else 0.0
    t_edges[1:] = t_edges[0] + np.cumsum(dt)
    t_centers = 0.5 * (t_edges[:-1] + t_edges[1:])

    durs_liq = rgq._duration_by_regime(d_liq, df["dt_window_min"].to_numpy(dtype=float))
    durs_ice = rgq._duration_by_regime(d_ice, df["dt_window_min"].to_numpy(dtype=float))
    reg_lbl = list(rgq.DIAMETER_REGIME_LABELS)

    n_rows = 3 + env_extra_rows if has_env else 3
    row_heights = ([1.0, 1.0, 1.0, 0.9, 0.75] if env_extra_rows == 2 else [1.0, 1.0, 1.0, 0.85]) if has_env else [1.0, 1.0, 1.0]
    sp_titles = [
        "Mean diameters",
        "Robust z (heatmap)",
        "Growth rates",
        "Diameter histogram",
        "Heights",
        "Time in diameter regime",
    ]
    if has_env:
        if env_extra_rows == 2:
            extra_t = ["Ridge T and w", "Ridge supersaturation", "Hydrometeor fall speed at ridge", ""]
        elif upper_env:
            extra_t = ["T / w at ridge", "Supersaturation"]
        else:
            extra_t = ["Hydrometeor fall speed at ridge", ""]
        sp_titles = list(sp_titles[:6]) + extra_t

    tw_spec: list[list[dict]] = [
        [{"secondary_y": False}, {"type": "heatmap"}],
        [{}, {}],
        [{}, {}],
    ]
    if has_env:
        use_sec = "T_ridge_K" in env_present and "w_ridge_m_s" in env_present
        tw_spec.append([{"secondary_y": use_sec if upper_env else False}, {}])
        if env_extra_rows == 2:
            tw_spec.append([{}, {}])

    fig = make_subplots(
        rows=n_rows,
        cols=2,
        column_widths=[0.58, 0.42],
        row_heights=row_heights,
        vertical_spacing=0.06,
        horizontal_spacing=0.06,
        subplot_titles=tuple(sp_titles),
        specs=tw_spec,
        shared_xaxes=True,
    )

    fig.add_trace(go.Scatter(x=t_min, y=d_liq, name="D_liq", line=dict(color=C_LIQ, width=1.4), mode="lines"), row=1, col=1)
    fig.add_trace(go.Scatter(x=t_min, y=d_ice, name="D_ice", line=dict(color=C_ICE, width=1.4), mode="lines"), row=1, col=1)
    fig.add_trace(go.Scatter(x=t_min, y=g_liq, name="dD_liq/dt", line=dict(color=C_LIQ, width=1.2), mode="lines"), row=2, col=1)
    fig.add_trace(go.Scatter(x=t_min, y=g_ice, name="dD_ice/dt", line=dict(color=C_ICE, width=1.2), mode="lines"), row=2, col=1)
    fig.add_trace(go.Scatter(x=t_min, y=z_ridge, name="z_ridge", line=dict(color="black", width=1.2), mode="lines"), row=3, col=1)
    fig.add_trace(
        go.Scatter(x=t_min, y=z_anchor, name="z_anchor", line=dict(color=C_ANCHOR, width=1.0, dash="dot"), mode="lines"),
        row=3,
        col=1,
    )

    fig.add_trace(
        go.Heatmap(
            z=heat,
            x=t_centers,
            y=y_labels,
            colorscale="RdBu_r",
            zmin=-3.0,
            zmax=3.0,
            colorbar=dict(title="robust z", len=0.35, y=0.82),
            hovertemplate="t=%{x:.2f} min<br>%{y}<br>z=%{z:.2f}<extra></extra>",
        ),
        row=1,
        col=2,
    )

    fig.add_trace(
        go.Histogram(x=d_liq[np.isfinite(d_liq)], name="liq hist", marker_color=C_LIQ, opacity=0.5, nbinsx=24),
        row=2,
        col=2,
    )
    fig.add_trace(
        go.Histogram(x=d_ice[np.isfinite(d_ice)], name="ice hist", marker_color=C_ICE, opacity=0.5, nbinsx=24),
        row=2,
        col=2,
    )

    reg_i = np.arange(len(reg_lbl), dtype=float)
    fig.add_trace(
        go.Bar(
            y=reg_i - 0.18,
            x=durs_liq,
            width=0.34,
            name="liq Δt",
            marker_color=C_LIQ,
            orientation="h",
            customdata=reg_lbl,
            hovertemplate="%{customdata}<br>liquid: %{x:.2f} min<extra></extra>",
        ),
        row=3,
        col=2,
    )
    fig.add_trace(
        go.Bar(
            y=reg_i + 0.18,
            x=durs_ice,
            width=0.34,
            name="ice Δt",
            marker_color=C_ICE,
            orientation="h",
            customdata=reg_lbl,
            hovertemplate="%{customdata}<br>ice: %{x:.2f} min<extra></extra>",
        ),
        row=3,
        col=2,
    )

    heatmap_trace_index = 6
    env_range_maps: dict[str, dict[str, tuple[float, float]]] = {}

    if has_env:
        r4 = 4
        r_v = 5 if env_extra_rows == 2 else 4
        if upper_env:
            if "T_ridge_K" in env_present:
                ta = env_present["T_ridge_K"]
                fig.add_trace(
                    go.Scatter(
                        x=t_min,
                        y=ta,
                        name="T ridge",
                        line=dict(color="#1b7837", width=1.3),
                        mode="lines",
                    ),
                    row=r4,
                    col=1,
                    secondary_y=False,
                )
            if "w_ridge_m_s" in env_present:
                wa = env_present["w_ridge_m_s"]
                fig.add_trace(
                    go.Scatter(
                        x=t_min,
                        y=wa,
                        name="w ridge",
                        line=dict(color="#762a83", width=1.3),
                        mode="lines",
                    ),
                    row=r4,
                    col=1,
                    secondary_y=bool("T_ridge_K" in env_present),
                )
        if upper_env:
            if "S_wat_ridge" in env_present:
                fig.add_trace(
                    go.Scatter(
                        x=t_min,
                        y=env_present["S_wat_ridge"],
                        name="S water",
                        line=dict(color="#2166ac", width=1.2),
                        mode="lines",
                    ),
                    row=r4,
                    col=2,
                )
            if "S_ice_ridge" in env_present:
                fig.add_trace(
                    go.Scatter(
                        x=t_min,
                        y=env_present["S_ice_ridge"],
                        name="S ice",
                        line=dict(color="#b2182b", width=1.2),
                        mode="lines",
                    ),
                    row=r4,
                    col=2,
                )
        if "vfall_liq_ridge_m_s" in env_present:
            fig.add_trace(
                go.Scatter(
                    x=t_min,
                    y=env_present["vfall_liq_ridge_m_s"],
                    name="VW (bin-mean)",
                    line=dict(color="#d95f02", width=1.3),
                    mode="lines",
                ),
                row=r_v,
                col=1,
            )
        if "vfall_ice_ridge_m_s" in env_present:
            fig.add_trace(
                go.Scatter(
                    x=t_min,
                    y=env_present["vfall_ice_ridge_m_s"],
                    name="VF (bin-mean)",
                    line=dict(color="#7570b3", width=1.3),
                    mode="lines",
                ),
                row=r_v,
                col=1,
            )
        if "v_hydr_ridge_m_s" in env_present and "vfall_liq_ridge_m_s" not in env_present:
            fig.add_trace(
                go.Scatter(
                    x=t_min,
                    y=env_present["v_hydr_ridge_m_s"],
                    name="v_hydr (legacy)",
                    line=dict(color="#d95f02", width=1.3),
                    mode="lines",
                ),
                row=r_v,
                col=1,
            )

        if upper_env:
            fig.update_yaxes(title_text="T ridge / K", row=r4, col=1, secondary_y=False)
            if "T_ridge_K" in env_present and "w_ridge_m_s" in env_present:
                fig.update_yaxes(title_text="w / (m s⁻¹)", row=r4, col=1, secondary_y=True)
            elif "w_ridge_m_s" in env_present:
                fig.update_yaxes(title_text="w / (m s⁻¹)", row=r4, col=1, secondary_y=False)
            fig.update_yaxes(title_text="supersat. / (-)", row=r4, col=2)
        fig.update_yaxes(title_text="spectral v_fall / (m s⁻¹)", row=r_v, col=1)
        for col, arr in env_present.items():
            env_range_maps[col] = {
                "mad4": _robust_mad_range(arr),
                "p5_95": _percentile_range(arr, 5.0, 95.0),
                "full": _full_finite_range(arr),
            }

    last_left_row = n_rows
    fig.update_xaxes(title_text="time from start / min", row=last_left_row, col=1, rangeslider_visible=True)
    fig.update_xaxes(matches="x", showticklabels=False, row=1, col=1)
    fig.update_xaxes(matches="x", showticklabels=False, row=2, col=1)
    fig.update_xaxes(matches="x", showticklabels=False, row=3, col=1)
    if has_env:
        fig.update_xaxes(matches="x", showticklabels=False, row=4, col=1)
        if upper_env:
            fig.update_xaxes(matches="x", showticklabels=False, row=4, col=2)
        if env_extra_rows == 2:
            fig.update_xaxes(matches="x", showticklabels=False, row=5, col=1)

    fig.update_xaxes(title_text="time from start / min", row=1, col=2)
    fig.update_xaxes(title_text=r"D / µm", row=2, col=2)
    fig.update_xaxes(title_text="minutes in regime", row=3, col=2)

    fig.update_yaxes(title_text=r"D / µm", row=1, col=1)
    fig.update_yaxes(title_text=r"dD/dt / µm s⁻¹", row=2, col=1)
    fig.update_yaxes(title_text="z / m", row=3, col=1)
    fig.update_yaxes(title_text="metric", row=1, col=2, autorange="reversed")
    fig.update_yaxes(title_text="count", row=2, col=2)
    fig.update_yaxes(
        title_text=r"D regime / µm",
        tickmode="array",
        tickvals=reg_i,
        ticktext=reg_lbl,
        row=3,
        col=2,
    )

    t_max = float(np.nanmax(t_min)) if len(t_min) else 1.0
    t_hi = t_max + 1e-6 * max(t_max, 1.0)
    clip_sec_options = (0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0)
    time_clip_buttons = []
    for sec in clip_sec_options:
        t_lo = sec / 60.0
        label = "Time: full span" if sec <= 0 else f"Time: skip first {sec:g} s"
        time_clip_buttons.append(
            {"label": label, "method": "relayout", "args": [_time_window_relayout(fig, t_lo, t_hi)]}
        )

    def _combined_supersat_range(mode: str) -> tuple[float, float]:
        parts = [env_present[c] for c in ("S_wat_ridge", "S_ice_ridge") if c in env_present]
        if not parts:
            return -1.0, 1.0
        comb = np.concatenate([np.asarray(p, dtype=float).ravel() for p in parts])
        if mode == "mad4":
            return _robust_mad_range(comb)
        if mode == "p5_95":
            return _percentile_range(comb, 5.0, 95.0)
        return _full_finite_range(comb)

    def _env_y_relayout(mode: str) -> dict:
        lay = fig.to_dict().get("layout", {})
        out: dict[str, list[float]] = {}
        for yk, yd in lay.items():
            if not isinstance(yk, str) or not yk.startswith("yaxis"):
                continue
            if not isinstance(yd, dict):
                continue
            title = yd.get("title")
            txt = title.get("text", "") if isinstance(title, dict) else str(title or "")
            lo = hi = None
            if "T ridge" in txt and "T_ridge_K" in env_range_maps:
                lo, hi = env_range_maps["T_ridge_K"][mode]
            elif "w /" in txt and "w_ridge_m_s" in env_range_maps:
                lo, hi = env_range_maps["w_ridge_m_s"][mode]
            elif "supersat" in txt and ("S_wat_ridge" in env_present or "S_ice_ridge" in env_present):
                lo, hi = _combined_supersat_range(mode)
            elif "spectral v_fall" in txt:
                vkeys = ("vfall_liq_ridge_m_s", "vfall_ice_ridge_m_s", "v_hydr_ridge_m_s")
                parts = [env_present[k] for k in vkeys if k in env_present]
                if parts:
                    comb = np.concatenate([np.asarray(a, dtype=float).ravel() for a in parts])
                    if mode == "mad4":
                        lo, hi = _robust_mad_range(comb)
                    elif mode == "p5_95":
                        lo, hi = _percentile_range(comb, 5.0, 95.0)
                    else:
                        lo, hi = _full_finite_range(comb)
            if lo is not None and hi is not None:
                out[f"{yk}.range"] = [float(lo), float(hi)]
        return out

    env_axis_keys = [
        k
        for k in fig.to_dict().get("layout", {})
        if isinstance(k, str) and k.startswith("yaxis") and _env_y_relayout("mad4").get(f"{k}.range") is None
    ]

    updatemenus: list[dict] = [
        {
            "type": "dropdown",
            "x": 0.02,
            "y": 1.14,
            "xanchor": "left",
            "yanchor": "top",
            "buttons": [
                {
                    "label": "Time panels: all windows",
                    "method": "restyle",
                    "args": [{"y": [d_liq, d_ice, g_liq, g_ice, z_ridge, z_anchor]}, [0, 1, 2, 3, 4, 5]],
                },
                {
                    "label": "Time panels: ice_ok windows only",
                    "method": "restyle",
                    "args": [
                        {"y": [d_liq_ice, d_ice_ice, g_liq_ice, g_ice_ice, z_ridge_ice, z_anchor_ice]},
                        [0, 1, 2, 3, 4, 5],
                    ],
                },
            ],
        },
        {
            "type": "dropdown",
            "x": 0.36,
            "y": 1.14,
            "xanchor": "left",
            "yanchor": "top",
            "buttons": [
                {"label": "Heatmap: RdBu_r", "method": "restyle", "args": [{"colorscale": "RdBu_r"}, [heatmap_trace_index]]},
                {"label": "Heatmap: RdYlBu", "method": "restyle", "args": [{"colorscale": "RdYlBu"}, [heatmap_trace_index]]},
                {"label": "Heatmap: viridis", "method": "restyle", "args": [{"colorscale": "Viridis"}, [heatmap_trace_index]]},
                {"label": "Heatmap: plasma", "method": "restyle", "args": [{"colorscale": "Plasma"}, [heatmap_trace_index]]},
            ],
        },
        {
            "type": "dropdown",
            "x": 0.62,
            "y": 1.14,
            "xanchor": "left",
            "yanchor": "top",
            "buttons": [
                {"label": "Diameters: linear y", "method": "relayout", "args": [{"yaxis.type": "linear"}]},
                {"label": "Diameters: log y", "method": "relayout", "args": [{"yaxis.type": "log"}]},
            ],
        },
        {
            "type": "dropdown",
            "x": 0.02,
            "y": 1.05,
            "xanchor": "left",
            "yanchor": "top",
            "buttons": time_clip_buttons,
        },
    ]

    if has_env and env_range_maps:
        updatemenus.append(
            {
                "type": "dropdown",
                "x": 0.36,
                "y": 1.05,
                "xanchor": "left",
                "yanchor": "top",
                "buttons": [
                    {
                        "label": "Env y: robust (MAD×4)",
                        "method": "relayout",
                        "args": [_env_y_relayout("mad4")],
                    },
                    {
                        "label": "Env y: p5–p95",
                        "method": "relayout",
                        "args": [_env_y_relayout("p5_95")],
                    },
                    {
                        "label": "Env y: full min–max",
                        "method": "relayout",
                        "args": [_env_y_relayout("full")],
                    },
                ],
            }
        )

    ann_text = (
        "Dropdowns: mask · colormap · diameter y · time clip"
        + (" · env y presets" if has_env else "")
        + ". Legend toggles traces; box-zoom any axis; double-click resets."
    )

    fig.update_layout(
        barmode="overlay",
        title=dict(text=title, x=0.5, xanchor="center"),
        height=280 + 220 * n_rows,
        width=1100,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
        margin=dict(t=128, b=60),
        hovermode="x unified",
        updatemenus=updatemenus,
        annotations=[
            dict(
                text=ann_text,
                xref="paper",
                yref="paper",
                x=0.0,
                y=1.26,
                showarrow=False,
                font=dict(size=10),
                xanchor="left",
            )
        ],
    )

    fig.add_hline(y=0.0, line_dash="dash", line_color="gray", row=2, col=1, opacity=0.6)
    if has_env and upper_env and ("S_wat_ridge" in env_present or "S_ice_ridge" in env_present):
        fig.add_hline(y=0.0, line_dash="dash", line_color="gray", row=4, col=2, opacity=0.5)

    if has_env and env_range_maps:
        fig.update_layout(**_env_y_relayout("mad4"))

    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description="Write interactive Plotly HTML for a ridge-growth CSV.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input ridge-growth CSV.")
    parser.add_argument("--output", type=Path, default=None, help="Output .html path.")
    parser.add_argument("--open", action="store_true", help="Open the HTML in the default browser.")
    args = parser.parse_args()

    out = args.output or _default_html_path(args.input)
    df = rgq._load_growth_csv(args.input)
    station = int(df["station"].iloc[0])
    exp_id = int(df["exp_id"].iloc[0])
    range_key = str(df["range_key"].iloc[0])
    stem_parts = args.input.stem.split("_")
    kind_tag = stem_parts[2] if len(stem_parts) >= 3 else "?"
    title = f"Ridge growth (interactive) — {kind_tag}, exp {exp_id}, stn {station}, {range_key}"

    fig = _build_figure(df, title=title)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(
        out,
        include_plotlyjs="cdn",
        full_html=True,
        config={"responsive": True, "scrollZoom": True, "displayModeBar": True},
    )
    print(f"saved -> {out}")
    if args.open:
        webbrowser.open(out.as_uri())


if __name__ == "__main__":
    main()
