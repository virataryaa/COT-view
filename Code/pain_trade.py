"""
Pain Trade Monitor — KC Prototype
Run: streamlit run pain_trade.py
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Pain Trade Monitor — KC", layout="wide",
                   initial_sidebar_state="collapsed")
st.markdown("""<style>
  [data-testid="stAppViewContainer"],[data-testid="stMain"],.main{background:#fafafa!important}
  [data-testid="stHeader"]{background:transparent!important}
  .block-container{padding-top:2rem!important;padding-bottom:1.5rem;max-width:1440px}
  hr{border:none!important;border-top:1px solid #e8e8ed!important;margin:.4rem 0!important}
  [data-testid="stRadio"] label{font-size:.78rem!important}
</style>""", unsafe_allow_html=True)

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

_BASE   = Path(__file__).parent.parent / "Database"
_COT    = _BASE / "cot_cit.parquet"
_ROLLEX = _BASE / "rollex_KC.parquet"

# =============================================================================
# DATA LOAD — raw columns only, legs computed after radio selection
# =============================================================================
@st.cache_data
def load_data():
    cot = pd.read_parquet(_COT)
    cot = cot[cot["Commodity"] == "KC"].sort_values("Date").reset_index(drop=True)

    rollex = pd.read_parquet(_ROLLEX)[["rollex_px"]].reset_index()
    rollex.columns = ["Date", "Rollex"]
    rollex["Date"] = pd.to_datetime(rollex["Date"])

    rollex_w = (
        rollex.set_index("Date")["Rollex"]
              .resample("W-FRI").last()
              .reset_index()
    )
    rollex_w.columns = ["Date", "Rollex"]

    df = pd.merge_asof(
        cot.sort_values("Date"),
        rollex_w.sort_values("Date"),
        on="Date", direction="nearest", tolerance=pd.Timedelta("7D"),
    )

    latest_rollex = float(rollex["Rollex"].dropna().iloc[-1])
    latest_rollex_date = rollex["Date"].dropna().iloc[-1]
    return df, latest_rollex, rollex, latest_rollex_date


df_raw, latest_px, rollex_daily, latest_rollex_date = load_data()

# =============================================================================
# HEADER
# =============================================================================
st.markdown(
    "<h2 style='font-family:\"Playfair Display\",Georgia,serif;color:#0a2463;"
    "font-weight:400;letter-spacing:-.01em;margin-bottom:2px'>"
    "Pain Trade Monitor"
    "<span style='font-size:1rem;font-weight:400;color:#6e6e73;margin-left:12px'>"
    "KC · Arabica Coffee</span></h2>",
    unsafe_allow_html=True,
)
st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================================
# CONTROLS — Radio + Date slider on same row
# =============================================================================
ctrl1, ctrl2 = st.columns([2, 5])

with ctrl1:
    incl_index = st.radio(
        "Include Index Traders in spec legs?",
        ["Yes — Spec + Non Rep + Index", "No — Spec + Non Rep only"],
        index=0, horizontal=False,
    )
    use_index = incl_index.startswith("Yes")

with ctrl2:
    min_d = df_raw["Date"].min().date()
    max_d = max(df_raw["Date"].max().date(), rollex_daily["Date"].max().date())
    default_start = (df_raw["Date"].max() - pd.DateOffset(years=2)).date()
    date_range = st.slider(
        "Date range", min_value=min_d, max_value=max_d,
        value=(default_start, max_d), format="YYYY-MM-DD",
    )

st.markdown("<hr>", unsafe_allow_html=True)

# =============================================================================
# COMPUTE LEGS based on radio selection
# =============================================================================
df = df_raw.copy()

if use_index:
    gross_long  = (df["Spec Long"]  + df["Non Rep Long"]  + df["Index Long"])  / 1000
    gross_short = (df["Spec Short"] + df["Non Rep Short"] + df["Index Short"]) / 1000
    leg_label   = "Spec + Non Rep + Index"
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

# Date filter
dff = df[
    (df["Date"] >= pd.Timestamp(date_range[0])) &
    (df["Date"] <= pd.Timestamp(date_range[1]))
].copy()

# =============================================================================
# VISUAL 1 — Weekly Spec Legs breakdown + Rollex line + dotted post-COT extension
# =============================================================================
last_cot_date = dff["Date"].max()
last_cot_str  = last_cot_date.strftime("%d/%m/%Y")
latest_rx_str = latest_rollex_date.strftime("%d/%m/%Y")

st.markdown(
    lbl(f"KC — Spec Legs Weekly Change ({leg_label}) · Rollex (Right) "
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

# Solid Rollex line up to last COT date
rollex_solid = dff.dropna(subset=["Rollex"])
fig1.add_trace(
    go.Scatter(
        x=rollex_solid["Date"], y=rollex_solid["Rollex"],
        name="Rollex (COT period)", mode="lines",
        line=dict(color=BLACK, width=2),
    ),
    secondary_y=True,
)

# Dotted Rollex extension: anchor from last solid point → daily to latest
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

# =============================================================================
# VISUAL 2 — Rollex (Y-axis) vs COT Leg Breakdown (X-axis)
# =============================================================================
# Use latest daily Rollex up to slider end date (not COT-merged weekly)
scatter_df  = dff.dropna(subset=["Rollex"]).copy()
_rx_upto    = rollex_daily[rollex_daily["Date"] <= pd.Timestamp(date_range[1])].dropna(subset=["Rollex"])
window_px   = float(_rx_upto["Rollex"].iloc[-1]) if not _rx_upto.empty else latest_px
window_date = _rx_upto["Date"].iloc[-1].strftime("%d/%m/%Y") if not _rx_upto.empty else latest_rx_str

st.markdown(
    lbl(f"Long Add · Liq | Short Add · Cover | Rollex (Y-axis) "
        f"· Rollex {window_px:.1f} as of {window_date} "
        f"· COT as of {last_cot_str}"),
    unsafe_allow_html=True,
)

# ── Zoom sliders for V2 (collapsible) ────────────────────────────────────────
_y_data_min = int(scatter_df["Rollex"].min() * 0.97) if not scatter_df.empty else 0
_y_data_max = int(scatter_df["Rollex"].max() * 1.03) if not scatter_df.empty else 500
_x_abs = scatter_df[["Long Add", "Long Liq", "Short Add", "Short Cover"]].abs().max().max()
_x_data_max = int(_x_abs * 1.1) if not np.isnan(_x_abs) else 25

with st.expander("Zoom controls", expanded=False):
    zc1, zc2 = st.columns(2)
    with zc1:
        y_zoom = st.slider("Y zoom — Rollex price", min_value=_y_data_min, max_value=_y_data_max,
                           value=(_y_data_min, _y_data_max), key="v2_y")
    with zc2:
        x_zoom = st.slider("X zoom — k Contracts", min_value=-_x_data_max, max_value=_x_data_max,
                           value=(-_x_data_max, _x_data_max), key="v2_x")

def _hbar_trace(values, prices, color, name):
    xs, ys = [], []
    for v, p in zip(values, prices):
        if pd.notna(v) and pd.notna(p) and v != 0:
            xs += [0, float(v), None]
            ys += [float(p), float(p), None]
    return go.Scatter(
        x=xs, y=ys, mode="lines", name=name,
        line=dict(color=color, width=4),
    )

fig2 = go.Figure()
fig2.add_trace(_hbar_trace(scatter_df["Long Add"],    scatter_df["Rollex"], DARK_GREEN,  "Long Add"))
fig2.add_trace(_hbar_trace(scatter_df["Long Liq"],    scatter_df["Rollex"], LIGHT_GREEN, "Long Liq."))
fig2.add_trace(_hbar_trace(scatter_df["Short Add"],   scatter_df["Rollex"], DARK_RED,    "Short Add"))
fig2.add_trace(_hbar_trace(scatter_df["Short Cover"], scatter_df["Rollex"], LIGHT_RED,   "Short Cover"))

# ── Rollex reference line — navy/gray dashed, label on left ──────────────────
fig2.add_hline(
    y=window_px,
    line_color="#4a5568", line_width=2, line_dash="dash",
    annotation_text=f"Rollex {window_px:.1f} ({window_date})  ",
    annotation_font=dict(size=10, color="#4a5568"),
    annotation_position="left",
)

fig2.add_vline(x=0, line_color="#cccccc", line_width=1)

# ── Per-week Pain/Pleasure helper (fresh positioning only) ────────────────────
def _pain_label(row, cur_px):
    long_add  = float(row["Long Add"])
    short_add = float(row["Short Add"])   # stored negative
    price     = float(row["Rollex"])
    if long_add >= abs(short_add):        # fresh longs dominated
        dominant_k = long_add
        activity   = "Fresh Longs"
        in_pain    = price > cur_px
    else:                                 # fresh shorts dominated
        dominant_k = abs(short_add)
        activity   = "Fresh Shorts"
        in_pain    = price < cur_px
    sentiment       = "PAIN" if in_pain else "PLEASURE"
    sentiment_color = DARK_RED if in_pain else DARK_GREEN
    k_str           = f"{dominant_k:.1f}k"
    return sentiment, sentiment_color, k_str, activity

# ── Annotate last 5 weeks: W-4 … Latest + Pain/Pleasure ──────────────────────
recent5     = scatter_df.tail(5).reset_index(drop=True)
week_labels = ["W-4", "W-3", "W-2", "W-1", "Latest"]

for i, row in recent5.iterrows():
    label                          = week_labels[i]
    rx_price                       = float(row["Rollex"])
    cot_date                       = row["Date"].strftime("%d/%m")
    is_latest                      = label == "Latest"
    pain_text, pain_color, k_str, activity = _pain_label(row, window_px)

    fig2.add_annotation(
        x=1.01, xref="paper",
        y=rx_price, yref="y",
        text=(f"<b>{label}</b> {cot_date} — "
              f"<b style='color:{pain_color}'>{pain_text}</b> "
              f"({k_str} {activity})"),
        showarrow=False, xanchor="left",
        font=dict(size=8, color=NAVY if not is_latest else DARK_RED,
                  family="-apple-system,sans-serif"),
        bgcolor="rgba(255,255,255,0.85)",
    )

fig2.update_layout(
    height=600,
    margin=dict(t=10, b=10, l=60, r=180),
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

# =============================================================================
# FOOTER
# =============================================================================
st.markdown("<hr>", unsafe_allow_html=True)
st.caption(
    f"Pain Trade Monitor · KC Arabica · COT: CFTC CIT · Rollex: roll-adjusted · "
    f"Legs: {leg_label} · Latest Rollex: {latest_px:.2f} · "
    f"COT as of {df_raw['Date'].max().strftime('%d/%m/%Y')}"
)
