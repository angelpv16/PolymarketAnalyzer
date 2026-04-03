"""Frontend Streamlit para PolymarketAnalyzer.

Behavioral fingerprinting · detecta la estrategia de cualquier cartera.
"""

import streamlit as st
import plotly.express as px

from src import config
from src.fetcher import fetch_completo
from src.storage import (
    get_connection,
    guardar_trades,
    guardar_markets,
    obtener_trades_wallet,
)
from src.analyzer import calcular_features
from src.classifier import clasificar

# ── Configuración de página ──────────────────────────────────────────────────

st.set_page_config(page_title="Polymarket Analyzer", page_icon="📊", layout="wide")

st.markdown(
    """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    [data-testid="stMetric"] {padding-top: 0.75rem; padding-bottom: 0.75rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Colores por arquetipo ────────────────────────────────────────────────────

COLORES: dict[str, str] = {
    "end-of-day": "#10b981",
    "contrarian": "#ef4444",
    "insider-like": "#8b5cf6",
    "momentum": "#f59e0b",
    "hold-to-resolution": "#3b82f6",
    "bimodal / mixed": "#f97316",
    "value / mixed": "#64748b",
}

# ── Cache wrappers ───────────────────────────────────────────────────────────


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_completo_cached(wallet: str) -> tuple[list[dict], list[dict]]:
    return fetch_completo(wallet)


@st.cache_data(ttl=3600, show_spinner=False)
def _calcular_features_cached(wallet: str) -> dict:
    return calcular_features(wallet)


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Wallets guardadas")

    for w in config.WALLETS:
        label = f"{w[:10]}...{w[-4:]}"
        if st.button(label, key=f"btn_{w}"):
            st.session_state["wallet_input"] = w

    st.divider()
    st.subheader("Base de datos")

    con = get_connection()
    try:
        total_wallets = con.execute(
            "SELECT count(DISTINCT wallet) FROM trades"
        ).fetchone()[0]
        total_trades = con.execute("SELECT count(*) FROM trades").fetchone()[0]
    finally:
        con.close()

    st.metric("Wallets analizadas", total_wallets)
    st.metric("Trades almacenados", f"{total_trades:,}")

# ── Header ───────────────────────────────────────────────────────────────────

st.title("📊 Polymarket Analyzer")
st.caption("Behavioral fingerprinting · detecta la estrategia de cualquier cartera")
st.divider()

# ── Input ────────────────────────────────────────────────────────────────────

wallet_input = st.text_input(
    "Wallet address",
    key="wallet_input",
    placeholder="0x...",
)

analizar = st.button("Analizar", type="primary")

# ── Lógica al analizar ───────────────────────────────────────────────────────

if analizar:
    wallet = wallet_input.strip()

    if not wallet or not wallet.startswith("0x"):
        st.error("Introduce una dirección válida que empiece por 0x.")
    else:
        try:
            with st.spinner("Obteniendo trades..."):
                trades, markets = _fetch_completo_cached(wallet)
        except Exception as e:
            st.error(f"Error obteniendo trades: {e}")
            st.stop()

        try:
            with st.spinner("Guardando en base de datos..."):
                trades_para_db = [
                    {k: v for k, v in t.items() if not k.startswith("_")}
                    for t in trades
                ]
                guardar_trades(wallet, trades_para_db)
                guardar_markets(markets)
        except Exception as e:
            st.error(f"Error guardando datos: {e}")
            st.stop()

        try:
            with st.spinner("Calculando comportamiento..."):
                features = _calcular_features_cached(wallet)
        except Exception as e:
            st.error(f"Error calculando features: {e}")
            st.stop()

        if features.get("n_trades", 0) == 0:
            st.warning("No se encontraron trades para esta wallet.")
            st.stop()

        try:
            with st.spinner("Clasificando estrategia..."):
                estilo, descripcion, scores = clasificar(features)
        except Exception as e:
            st.error(f"Error clasificando: {e}")
            st.stop()

        st.session_state["resultado"] = {
            "wallet": wallet,
            "features": features,
            "estilo": estilo,
            "descripcion": descripcion,
            "scores": scores,
        }

# ── Resultados ───────────────────────────────────────────────────────────────

if "resultado" in st.session_state:
    r = st.session_state["resultado"]
    features = r["features"]
    estilo = r["estilo"]
    descripcion = r["descripcion"]
    scores = r["scores"]
    wallet = r["wallet"]

    color = COLORES.get(estilo, "#64748b")

    # Bloque 1 — Arquetipo detectado
    st.markdown(
        f"""
        <div style="
            background-color: {color}22;
            border-left: 5px solid {color};
            padding: 1rem 1.5rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        ">
            <span style="
                color: {color};
                font-size: 1.6rem;
                font-weight: 700;
                letter-spacing: 0.05em;
            ">{estilo.upper()}</span>
            <br/>
            <span style="color: #ccc; font-size: 0.95rem;">{descripcion}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Bloque 2 — Métricas
    def fmt_metric(val, fmt_type="default"):
        if val is None:
            return "—"
        if fmt_type == "pct":
            return f"{val:.1f}%"
        if fmt_type == "dollar":
            return f"${val:.2f}"
        if fmt_type == "int":
            return str(int(val))
        return str(val)

    def metric_delta(val):
        return "sin datos" if val is None else None

    row1 = st.columns(4)
    row1[0].metric("Trades totales", fmt_metric(features["n_trades"], "int"))
    row1[1].metric("Mercados operados", fmt_metric(features["n_mercados"], "int"))
    row1[2].metric(
        "Importe medio",
        fmt_metric(features["importe_medio"], "dollar"),
        delta=metric_delta(features["importe_medio"]),
    )
    row1[3].metric(
        "Coef. variación",
        fmt_metric(features["coef_variacion"]),
        delta=metric_delta(features["coef_variacion"]),
    )

    row2 = st.columns(4)
    row2[0].metric(
        "Antelación media (días)",
        fmt_metric(features["antelacion_media_dias"]),
        delta=metric_delta(features["antelacion_media_dias"]),
    )
    row2[1].metric(
        "% entradas última semana",
        fmt_metric(features["pct_entradas_ultima_semana_mercado"], "pct"),
        delta=metric_delta(features["pct_entradas_ultima_semana_mercado"]),
    )
    row2[2].metric(
        "Hold rate",
        fmt_metric(features["hold_rate"], "pct"),
        delta=metric_delta(features["hold_rate"]),
    )
    row2[3].metric(
        "Hora entrada moda",
        fmt_metric(features["hora_entrada_moda"], "int"),
        delta=metric_delta(features["hora_entrada_moda"]),
    )

    # Bloque 3 — Gráfico de distribución de precios
    dist = features.get("price_distribucion")
    if dist:
        rangos = ["0–20%", "20–40%", "40–60%", "60–80%", "80–100%"]
        valores = [
            dist.get("0-20", 0),
            dist.get("20-40", 0),
            dist.get("40-60", 0),
            dist.get("60-80", 0),
            dist.get("80-100", 0),
        ]

        fig = px.bar(
            x=rangos,
            y=valores,
            labels={"x": "Rango de precio", "y": "% de trades"},
            title="Distribución de precios de entrada",
            color_discrete_sequence=[color],
            template="plotly_white",
        )
        fig.update_layout(
            height=280,
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Bloque 4 — Scores de arquetipos
    st.subheader("Scores por arquetipo")
    for arq, score in sorted(scores.items(), key=lambda x: -x[1]):
        col_label, col_bar = st.columns([1, 3])
        col_label.markdown(f"**{arq}** — {score:.0%}")
        col_bar.progress(score)

    # Bloque 5 — Últimos trades
    st.subheader("Últimos 20 trades")
    df = obtener_trades_wallet(wallet)
    if not df.empty:
        df_display = df.tail(20).copy()
        df_display["market_id"] = df_display["market_id"].str[:12]
        df_display["price"] = df_display["price"].map(lambda x: f"{x:.3f}")
        df_display["size"] = df_display["size"].map(lambda x: f"{x:.2f}")
        st.dataframe(
            df_display[["timestamp", "market_id", "side", "price", "size", "outcome"]],
            use_container_width=True,
        )
    else:
        st.info("No hay trades almacenados para esta wallet.")

    # Bloque 6 — Nota sobre datos no disponibles
    if features.get("win_rate") is None or features.get("roi_estimado") is None:
        st.info(
            "ℹ️ Algunas métricas (win rate, ROI) no están disponibles porque "
            "el campo outcome contiene el nombre del token, no el resultado "
            "de resolución del mercado."
        )
