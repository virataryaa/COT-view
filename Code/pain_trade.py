"""
Pain Trade Monitor — All Commodities
CIT: KC / CC / SB / CT  |  Disaggregated: LRC / LCC
Run: streamlit run pain_trade.py
"""

import functools
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Pain Trade Monitor", layout="wide",
                   initial_sidebar_state="collapsed")
st.markdown("""<style>
  [data-testid="stAppViewContainer"],[data-testid="stMain"],.main{background:#fafafa!important}
  [data-testid="stHeader"]{background:transparent!important}
  .block-container{padding-top:2rem!important;padding-bottom:1.5rem;max-width:1440px}
  hr{border:none!important;border-top:1px solid #e8e8ed!important;margin:.4rem 0!important}
  [data-testid="stRadio"] label{font-size:.78rem!important}
</style>""", unsafe_allow_html=True)

# ── Palette ───────────────────────────────────────────────────────────────────
NAVY        = "#0a2463"
DARK_GREEN  = "#1a6b1a"
LIGHT_GREEN = "#7dce7d"
DARK_RED    = "#8b0000"
LIGHT_RED   = "#f4a0a0"
BLACK       = "#1d1d1f"
AMBER       = "#e8a020"

_D = dict(
    template="plotly_white",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="-apple-system,Helvetica Neue,sans-serif", color=BLACK, size=10),
)

def lbl(text):
    return (f"<div style='background:{NAVY};padding:5px 13px;border-radius:5px;"
            f"margin-bottom:8px'><span style='font-size:.78rem;font-weight:500;"
            f"letter-spacing:.07em;text-transform:uppercase;color:#dde4f0'>{text}</span></div>")

# ── Rollex utils ─────────────────────────────────────────────────────────────
from rollex_utils import load_rollex as _rx_load

# ── Commodity config ──────────────────────────────────────────────────────────
_BASE = Path(__file__).parent.parent / "Database"  # COT parquets only

COMM_CONFIG = {
    "KC":  {"name": "Arabica",      "cot": "cit",    "color": "#0a2463",
            "third_leg": "Index",      "long3": "Index Long", "short3": "Index Short"},
    "LRC": {"name": "Robusta",      "cot": "disagg", "color": "#8b1a00",
            "third_leg": "Other Rep.", "long3": "Other Long", "short3": "Other Short"},
    "CC":  {"name": "NYC Cocoa",    "cot": "cit",    "color": "#e8a020",
            "third_leg": "Index",      "long3": "Index Long", "short3": "Index Short"},
    "LCC": {"name": "London Cocoa", "cot": "disagg", "color": "#4a7fb5",
            "third_leg": "Other Rep.", "long3": "Other Long", "short3": "Other Short"},
    "SB":  {"name": "Sugar",        "cot": "cit",    "color": "#1a6b1a",
            "third_leg": "Index",      "long3": "Index Long", "short3": "Index Short"},
    "CT":  {"name": "Cotton",       "cot": "cit",    "color": "#7b2d8b",
            "third_leg": "Index",      "long3": "Index Long", "short3": "Index Short"},
}

CIT_FILE    = _BASE / "cot_cit.parquet"
DISAGG_FILE = _BASE / "cot_disagg.parquet"

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_cot():
    cit    = pd.read_parquet(CIT_FILE)
    disagg = pd.read_parquet(DISAGG_FILE)
    cit["Date"]    = pd.to_datetime(cit["Date"])
    disagg["Date"] = pd.to_datetime(disagg["Date"])
    return cit, disagg

@st.cache_data(ttl=3600)
def load_rollex(comm: str):
    df = _rx_load(comm)[["rollex_px"]].reset_index()
    df.columns = ["Date", "Rollex"]
    df["Date"] = pd.to_datetime(df["Date"])
    return df

cot_cit, cot_disagg = load_cot()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(
    "<h2 style='font-family:\"Playfair Display\",Georgia,serif;color:#0a2463;"
    "font-weight:400;letter-spacing:-.01em;margin-bottom:2px'>"
    "Pain Trade Monitor</h2>",
    unsafe_allow_html=True,
)
st.markdown("<hr>", unsafe_allow_html=True)

# ── Date range sync helpers ───────────────────────────────────────────────────
def _slider_to_cal(comm):
    v = st.session_state.get(f"sl_{comm}")
    if v:
        st.session_state[f"cal_{comm}"] = list(v)

def _cal_to_slider(comm):
    dates = st.session_state.get(f"cal_{comm}")
    if isinstance(dates, (list, tuple)) and len(dates) == 2:
        st.session_state[f"sl_{comm}"] = (dates[0], dates[1])

# ── Tabs — one per commodity ──────────────────────────────────────────────────
tab_labels = [COMM_CONFIG[c]["name"] for c in COMM_CONFIG]
comm_tabs  = st.tabs(tab_labels)

for tab, comm in zip(comm_tabs, COMM_CONFIG):
    cfg          = COMM_CONFIG[comm]
    is_cit       = cfg["cot"] == "cit"
    cot_raw      = cot_cit if is_cit else cot_disagg
    df_comm      = cot_raw[cot_raw["Commodity"] == comm].sort_values("Date").reset_index(drop=True)
    rollex_daily = load_rollex(comm)

    with tab:
        if df_comm.empty:
            st.warning(f"No COT data found for {comm}.")
            continue

        # ── Controls (collapsible) ────────────────────────────────────────────
        min_d         = df_comm["Date"].min().date()
        max_d         = max(df_comm["Date"].max().date(), rollex_daily["Date"].max().date())
        default_start = (df_comm["Date"].max() - pd.DateOffset(years=2)).date()

        _dr       = st.session_state.get(f"sl_{comm}", (default_start, max_d))
        third_label = cfg["third_leg"]
        _incl_cur = st.session_state.get(f"radio_{comm}", f"Yes — Spec + Non Rep + {third_label}")
        _dr_str   = f"{_dr[0].strftime('%d %b %Y')} → {_dr[1].strftime('%d %b %Y')}"

        with st.expander(f"Controls · {_dr_str} · {_incl_cur.split('—')[1].strip()}", expanded=False):
            ctrl1, ctrl2 = st.columns([2, 5])

            with ctrl1:
                incl = st.radio(
                    f"Include {third_label} in spec legs?",
                    [f"Yes — Spec + Non Rep + {third_label}",
                     "No — Spec + Non Rep only"],
                    index=0, horizontal=False, key=f"radio_{comm}",
                )
                use_third = incl.startswith("Yes")

            with ctrl2:
                st.slider(
                    "Date range", min_value=min_d, max_value=max_d,
                    value=(default_start, max_d), format="YYYY-MM-DD",
                    key=f"sl_{comm}",
                    on_change=functools.partial(_slider_to_cal, comm),
                )

                _ca, _cb = st.columns(2)
                _cur = st.session_state.get(f"sl_{comm}", (default_start, max_d))
                with _ca:
                    st.date_input("From", value=_cur[0], min_value=min_d, max_value=max_d,
                                  key=f"cal_s_{comm}", label_visibility="collapsed",
                                  on_change=lambda c=comm: (
                                      st.session_state.update({f"sl_{c}": (
                                          st.session_state[f"cal_s_{c}"],
                                          st.session_state.get(f"sl_{c}", (default_start, max_d))[1]
                                      )})
                                  ))
                with _cb:
                    st.date_input("To", value=_cur[1], min_value=min_d, max_value=max_d,
                                  key=f"cal_e_{comm}", label_visibility="collapsed",
                                  on_change=lambda c=comm: (
                                      st.session_state.update({f"sl_{c}": (
                                          st.session_state.get(f"sl_{c}", (default_start, max_d))[0],
                                          st.session_state[f"cal_e_{c}"]
                                      )})
                                  ))

        use_third  = st.session_state.get(f"radio_{comm}", f"Yes — Spec + Non Rep + {third_label}").startswith("Yes")
        _dr        = st.session_state.get(f"sl_{comm}", (default_start, max_d))
        date_range = (_dr[0], _dr[1])

        st.markdown("<hr>", unsafe_allow_html=True)

        # ── Compute spec legs ─────────────────────────────────────────────────
        df = df_comm.copy()

        if use_third:
            gross_long  = (df["Spec Long"]  + df["Non Rep Long"]  + df[cfg["long3"]])  / 1000
            gross_short = (df["Spec Short"] + df["Non Rep Short"] + df[cfg["short3"]]) / 1000
            leg_label   = f"Spec + Non Rep + {third_label}"
        else:
            gross_long  = (df["Spec Long"]  + df["Non Rep Long"])  / 1000
            gross_short = (df["Spec Short"] + df["Non Rep Short"]) / 1000
            leg_label   = "Spec + Non Rep"

        long_chg  = gross_long.diff()
        short_chg = gross_short.diff()

        df["Long Add"]    =  long_chg.clip(lower=0)
        df["Long Liq"]    =  long_chg.clip(upper=0)
        df["Short Add"]   = -short_chg.clip(lower=0)
        df["Short Cover"] = -short_chg.clip(upper=0)

        # Rollex on COT date — direct lookup, no resampling
        df = pd.merge_asof(
            df.sort_values("Date"),
            rollex_daily.sort_values("Date"),
            on="Date", direction="nearest", tolerance=pd.Timedelta("1D"),
        )

        # Date filter
        dff = df[
            (df["Date"] >= pd.Timestamp(date_range[0])) &
            (df["Date"] <= pd.Timestamp(date_range[1]))
        ].copy()

        last_cot_date = dff["Date"].max()
        last_cot_str  = last_cot_date.strftime("%d/%m/%Y")
        latest_rx_str = rollex_daily["Date"].max().strftime("%d/%m/%Y")

        # Latest daily Rollex up to slider end date
        _rx_upto    = rollex_daily[rollex_daily["Date"] <= pd.Timestamp(date_range[1])].dropna(subset=["Rollex"])
        window_px   = float(_rx_upto["Rollex"].iloc[-1]) if not _rx_upto.empty else np.nan
        window_date = _rx_upto["Date"].iloc[-1].strftime("%d/%m/%Y") if not _rx_upto.empty else "—"

        # ── VISUAL 1 — Spec legs bars + Rollex ───────────────────────────────
        st.markdown(
            lbl(f"{comm} — Spec Legs Weekly Change ({leg_label}) · Rollex (Right) "
                f"| COT as of {last_cot_str} · Rollex as of {latest_rx_str}"),
            unsafe_allow_html=True,
        )

        fig1 = make_subplots(specs=[[{"secondary_y": True}]])

        for col, color, name in [
            ("Long Add",    DARK_GREEN,  "Long Add"),
            ("Long Liq",    LIGHT_GREEN, "Long Liq."),
            ("Short Add",   DARK_RED,    "Short Add"),
            ("Short Cover", LIGHT_RED,   "Short Cover"),
        ]:
            fig1.add_trace(
                go.Bar(x=dff["Date"], y=dff[col], name=name,
                       marker_color=color, opacity=0.92),
                secondary_y=False,
            )

        rollex_solid = dff.dropna(subset=["Rollex"])
        fig1.add_trace(
            go.Scatter(
                x=rollex_solid["Date"], y=rollex_solid["Rollex"],
                name="Rollex (COT period)", mode="lines",
                line=dict(color=BLACK, width=2),
            ),
            secondary_y=True,
        )

        # Dotted post-COT extension
        last_solid_row = rollex_solid.iloc[-1:][["Date", "Rollex"]].copy()
        rollex_after   = rollex_daily[rollex_daily["Date"] > last_cot_date][["Date", "Rollex"]].copy()
        rollex_ext     = pd.concat([last_solid_row, rollex_after]).sort_values("Date")
        if len(rollex_ext) > 1:
            fig1.add_trace(
                go.Scatter(
                    x=rollex_ext["Date"], y=rollex_ext["Rollex"],
                    name=f"Rollex post-COT ({latest_rx_str})",
                    mode="lines",
                    line=dict(color=AMBER, width=2, dash="dot"),
                ),
                secondary_y=True,
            )

        fig1.update_layout(
            barmode="relative", height=420,
            margin=dict(t=10, b=10, l=4, r=4),
            legend=dict(orientation="h", y=1.06, x=0, font=dict(size=9)),
            xaxis=dict(showgrid=False, tickfont=dict(size=9)),
            **_D,
        )
        fig1.update_yaxes(title_text="k Contracts", secondary_y=False,
                          showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9))
        fig1.update_yaxes(title_text="Rollex Price", secondary_y=True,
                          showgrid=False, tickfont=dict(size=9))

        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("<hr>", unsafe_allow_html=True)

        # ── VISUAL 2 — Rollex (Y) vs COT breakdown (X) ───────────────────────
        scatter_df = dff.dropna(subset=["Rollex"]).copy()

        window_px_str = f"{window_px:.1f}" if pd.notna(window_px) else "—"
        st.markdown(
            lbl(f"{comm} — Long Add · Liq | Short Add · Cover | Rollex (Y-axis) "
                f"· Rollex {window_px_str} as of {window_date} "
                f"· COT as of {last_cot_str}"),
            unsafe_allow_html=True,
        )

        # Zoom controls
        _y_data_min = int(scatter_df["Rollex"].min() * 0.97) if not scatter_df.empty else 0
        _y_data_max = int(scatter_df["Rollex"].max() * 1.03) if not scatter_df.empty else 500
        _x_abs      = scatter_df[["Long Add", "Long Liq", "Short Add", "Short Cover"]].abs().max().max()
        _x_data_max = int(_x_abs * 1.1) if not np.isnan(_x_abs) else 25

        with st.expander("Zoom controls", expanded=False):
            zc1, zc2 = st.columns(2)
            with zc1:
                y_zoom = st.slider("Y zoom — Rollex price",
                                   min_value=_y_data_min, max_value=_y_data_max,
                                   value=(_y_data_min, _y_data_max), key=f"v2_y_{comm}")
            with zc2:
                x_zoom = st.slider("X zoom — k Contracts",
                                   min_value=-_x_data_max, max_value=_x_data_max,
                                   value=(-_x_data_max, _x_data_max), key=f"v2_x_{comm}")

        def _hbar_trace(values, prices, color, name):
            xs, ys = [], []
            for v, p in zip(values, prices):
                if pd.notna(v) and pd.notna(p) and v != 0:
                    xs += [0, float(v), None]
                    ys += [float(p), float(p), None]
            return go.Scatter(x=xs, y=ys, mode="lines", name=name,
                              line=dict(color=color, width=4))

        fig2 = go.Figure()
        fig2.add_trace(_hbar_trace(scatter_df["Long Add"],    scatter_df["Rollex"], DARK_GREEN,  "Long Add"))
        fig2.add_trace(_hbar_trace(scatter_df["Long Liq"],    scatter_df["Rollex"], LIGHT_GREEN, "Long Liq."))
        fig2.add_trace(_hbar_trace(scatter_df["Short Add"],   scatter_df["Rollex"], DARK_RED,    "Short Add"))
        fig2.add_trace(_hbar_trace(scatter_df["Short Cover"], scatter_df["Rollex"], LIGHT_RED,   "Short Cover"))

        # Rollex reference line (left label, navy/gray dashed)
        if pd.notna(window_px):
            fig2.add_hline(
                y=window_px,
                line_color="#4a5568", line_width=2, line_dash="dash",
                annotation_text=f"Rollex {window_px:.1f} ({window_date})  ",
                annotation_font=dict(size=10, color="#4a5568"),
                annotation_position="left",
            )
        fig2.add_vline(x=0, line_color="#cccccc", line_width=1)

        recent5     = scatter_df.tail(5).reset_index(drop=True)
        week_labels = ["W-4", "W-3", "W-2", "W-1", "Latest"]

        for i, row in recent5.iterrows():
            label     = week_labels[i]
            rx_price  = float(row["Rollex"])
            cot_date  = row["Date"].strftime("%d/%m")
            is_latest = label == "Latest"

            tag_text = (
                f"<b>{label}</b>"
                f"<i style='font-size:7px;color:#888888'> {cot_date}</i>"
            )

            fig2.add_shape(
                type="line",
                x0=0, y0=rx_price, x1=_x_data_max, y1=rx_price,
                line=dict(color="#bbbbbb", width=1, dash="dot"),
                xref="x", yref="y", layer="below",
            )

            fig2.add_annotation(
                x=1.01, xref="paper",
                y=rx_price, yref="y",
                text=tag_text,
                showarrow=False, xanchor="left", align="left",
                font=dict(size=8, color=NAVY if not is_latest else DARK_RED,
                          family="-apple-system,sans-serif"),
                bgcolor="rgba(255,255,255,0.88)",
            )

        fig2.update_layout(
            height=600,
            margin=dict(t=10, b=10, l=60, r=220),
            legend=dict(orientation="h", y=1.04, x=0, font=dict(size=9)),
            xaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9),
                       title="k Contracts", zeroline=False, range=list(x_zoom)),
            yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9),
                       title="Rollex Price", range=list(y_zoom)),
            **_D,
        )

        _l, _ch, _r = st.columns([0.125, 0.75, 0.125])
        with _ch:
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("<hr>", unsafe_allow_html=True)

        # ── ADVANCED ANALYTICS ────────────────────────────────────────────────
        with st.expander("Advanced Analytics — WAEP · Pain Score · Max Pain", expanded=False):

            WAEP_WINDOW = 4  # trailing weeks

            full_ana = df.dropna(subset=["Rollex"]).copy().reset_index(drop=True)

            # Vectorised rolling WAEP
            # Long Add is positive, Short Add is negative (bars go left) — take abs
            la_pos  = full_ana["Long Add"].clip(lower=0)
            sa_pos  = full_ana["Short Add"].clip(upper=0).abs()
            rx      = full_ana["Rollex"]

            waep_long_s  = (la_pos * rx).rolling(WAEP_WINDOW, min_periods=1).sum() / \
                            la_pos.rolling(WAEP_WINDOW, min_periods=1).sum()
            waep_short_s = (sa_pos * rx).rolling(WAEP_WINDOW, min_periods=1).sum() / \
                            sa_pos.rolling(WAEP_WINDOW, min_periods=1).sum()

            full_ana["waep_long"]   = waep_long_s
            full_ana["waep_short"]  = waep_short_s
            full_ana["pain_long"]   = full_ana["Rollex"] - full_ana["waep_long"]   # +ve = longs in profit, -ve = pain
            full_ana["pain_short"]  = full_ana["waep_short"] - full_ana["Rollex"]  # +ve = shorts in profit, -ve = pain

            # Current values — latest row within slider range
            cur = full_ana[full_ana["Date"] <= pd.Timestamp(date_range[1])].iloc[-1]
            cur_waep_long  = float(cur["waep_long"])
            cur_waep_short = float(cur["waep_short"])
            cur_pain_long  = float(cur["pain_long"])   # -ve → longs underwater
            cur_pain_short = float(cur["pain_short"])  # -ve → shorts underwater

            # Max pain level — price where aggregate pain is maximised
            # weighted midpoint of the two WAEPs, weighted by their respective position sizes
            la_total = la_pos.tail(WAEP_WINDOW).sum()
            sa_total = sa_pos.tail(WAEP_WINDOW).sum()
            denom    = la_total + sa_total
            max_pain_px = (cur_waep_long * sa_total + cur_waep_short * la_total) / denom \
                          if denom > 0 else np.nan

            # Historical percentiles (full available history)
            pct_long  = (full_ana["pain_long"]  < cur_pain_long).mean()  * 100
            pct_short = (full_ana["pain_short"] < cur_pain_short).mean() * 100

            # ── Metric row ────────────────────────────────────────────────────
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric(
                    "WAEP — Fresh Longs",
                    f"{cur_waep_long:.1f}",
                    delta=f"{cur_pain_long:+.1f} vs current",
                    delta_color="normal",
                    help="Weighted avg entry price of spec longs over trailing 52 weeks"
                )
            with m2:
                st.metric(
                    "WAEP — Fresh Shorts",
                    f"{cur_waep_short:.1f}",
                    delta=f"{cur_pain_short:+.1f} vs current",
                    delta_color="normal",
                    help="Weighted avg entry price of spec shorts over trailing 52 weeks"
                )
            with m3:
                st.metric(
                    "Max Pain Level",
                    f"{max_pain_px:.1f}" if pd.notna(max_pain_px) else "—",
                    help="Price where both longs and shorts are simultaneously most underwater"
                )
            with m4:
                st.metric("Current Rollex", f"{window_px:.1f}" if pd.notna(window_px) else "—")

            st.markdown(
                f"<div style='font-size:.78rem;color:#666;margin:.4rem 0 .8rem'>"
                f"Long pain at <b>{pct_long:.0f}th</b> percentile historically &nbsp;·&nbsp; "
                f"Short pain at <b>{pct_short:.0f}th</b> percentile historically"
                f"</div>",
                unsafe_allow_html=True,
            )

            # ── Pain timeline ─────────────────────────────────────────────────
            st.markdown(lbl("Pain Score History — Distance of Current Price from WAEP Entry"),
                        unsafe_allow_html=True)

            fig_pain = go.Figure()
            fig_pain.add_trace(go.Scatter(
                x=full_ana["Date"], y=full_ana["pain_long"],
                name="Long Pain  (+ve = profit, −ve = underwater)",
                mode="lines", line=dict(color=DARK_GREEN, width=1.5),
                fill="tozeroy", fillcolor="rgba(26,107,26,0.12)",
            ))
            fig_pain.add_trace(go.Scatter(
                x=full_ana["Date"], y=full_ana["pain_short"],
                name="Short Pain  (+ve = profit, −ve = underwater)",
                mode="lines", line=dict(color=DARK_RED, width=1.5),
                fill="tozeroy", fillcolor="rgba(139,0,0,0.12)",
            ))
            fig_pain.add_hline(y=0, line_color="#bbbbbb", line_width=1)

            # Mark current pain levels
            for val, color, label_ in [
                (cur_pain_long,  DARK_GREEN, f"Long pain now {cur_pain_long:+.1f}"),
                (cur_pain_short, DARK_RED,   f"Short pain now {cur_pain_short:+.1f}"),
            ]:
                fig_pain.add_hline(
                    y=val, line_color=color, line_width=1, line_dash="dot",
                    annotation_text=label_, annotation_font=dict(size=9, color=color),
                    annotation_position="right",
                )

            fig_pain.update_layout(
                height=300,
                margin=dict(t=10, b=10, l=4, r=120),
                legend=dict(orientation="h", y=1.08, font=dict(size=9)),
                xaxis=dict(showgrid=False, tickfont=dict(size=9)),
                yaxis=dict(title="Price Distance from WAEP", tickfont=dict(size=9),
                           showgrid=True, gridcolor="#f0f0f0"),
                **_D,
            )
            st.plotly_chart(fig_pain, use_container_width=True)

            # ── WAEP lines on scatter context ─────────────────────────────────
            st.markdown(lbl("WAEP Levels in Context — vs Scatter"), unsafe_allow_html=True)

            fig_waep = go.Figure()
            fig_waep.add_trace(_hbar_trace(scatter_df["Long Add"],    scatter_df["Rollex"], DARK_GREEN,  "Long Add"))
            fig_waep.add_trace(_hbar_trace(scatter_df["Short Add"],   scatter_df["Rollex"], DARK_RED,    "Short Add"))
            fig_waep.add_trace(_hbar_trace(scatter_df["Long Liq"],    scatter_df["Rollex"], LIGHT_GREEN, "Long Liq."))
            fig_waep.add_trace(_hbar_trace(scatter_df["Short Cover"], scatter_df["Rollex"], LIGHT_RED,   "Short Cover"))

            for px_val, color, label_ in [
                (cur_waep_long,  DARK_GREEN,  f"WAEP Long {cur_waep_long:.1f}  "),
                (cur_waep_short, DARK_RED,    f"WAEP Short {cur_waep_short:.1f}  "),
                (max_pain_px,    AMBER,        f"Max Pain {max_pain_px:.1f}  ") if pd.notna(max_pain_px) else (None, None, None),
                (window_px,      "#4a5568",    f"Current {window_px:.1f}  ") if pd.notna(window_px) else (None, None, None),
            ]:
                if px_val is None:
                    continue
                fig_waep.add_hline(
                    y=px_val, line_color=color, line_width=1.5, line_dash="dash",
                    annotation_text=label_,
                    annotation_font=dict(size=9, color=color),
                    annotation_position="left",
                )

            fig_waep.add_vline(x=0, line_color="#cccccc", line_width=1)
            fig_waep.update_layout(
                height=450,
                margin=dict(t=10, b=10, l=60, r=60),
                legend=dict(orientation="h", y=1.06, font=dict(size=9)),
                xaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9),
                           title="k Contracts", zeroline=False),
                yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9),
                           title="Rollex Price"),
                **_D,
            )
            st.plotly_chart(fig_waep, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(
    "Pain Trade Monitor · KC / CC / SB / CT (CIT) · LRC / LCC (Disaggregated) · "
    "Rollex: roll-adjusted · Pain = fresh positioning now underwater"
)
