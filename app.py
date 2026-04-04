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
    init_db,
    obtener_trades_wallet,
)
from src.analyzer import calcular_features
from src.classifier import clasificar
from src.pattern_analyzer import calcular_patrones
from src.llm_analyzer import generar_narrativa
from src.rules_extractor import extraer_reglas, exportar_reglas_txt
from src.config import GEMINI_API_KEY

init_db()

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


@st.cache_data(ttl=3600, show_spinner=False)
def _calcular_patrones_cached(wallet: str) -> dict:
    return calcular_patrones(wallet)


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

        try:
            with st.spinner("Analizando patrones secuenciales..."):
                patrones = _calcular_patrones_cached(wallet)
        except Exception as e:
            st.error(f"Error calculando patrones: {e}")
            st.stop()

        narrativa = None
        if GEMINI_API_KEY:
            try:
                with st.spinner("Generando narrativa con Gemini..."):
                    narrativa = generar_narrativa(
                        wallet, features, patrones, estilo, descripcion, scores
                    )
            except Exception as e:
                st.warning(f"Error generando narrativa: {e}")

        try:
            with st.spinner("Extrayendo reglas operacionales..."):
                reglas = extraer_reglas(
                    wallet, features, patrones, estilo, descripcion
                )
        except Exception as e:
            st.warning(f"Error extrayendo reglas: {e}")
            reglas = None

        st.session_state["resultado"] = {
            "wallet": wallet,
            "features": features,
            "estilo": estilo,
            "descripcion": descripcion,
            "scores": scores,
            "patrones": patrones,
            "narrativa": narrativa,
            "reglas": reglas,
        }

# ── Resultados ───────────────────────────────────────────────────────────────

if "resultado" in st.session_state:
    r = st.session_state["resultado"]
    features = r["features"]
    estilo = r["estilo"]
    descripcion = r["descripcion"]
    scores = r["scores"]
    wallet = r["wallet"]
    patrones = r.get("patrones", {})
    narrativa = r.get("narrativa")
    reglas = r.get("reglas")

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

    # Bloque 4 — Patrones secuenciales
    if patrones:
        st.subheader("Patrones secuenciales")
        col_a, col_b, col_c = st.columns(3)

        # Columna 1: Acumulación + Size Scaling
        with col_a:
            acc = patrones.get("acumulacion", {})
            if acc.get("accumulation_type"):
                st.markdown(f"**Acumulación:** `{acc['accumulation_type']}`")
                st.caption(
                    f"Re-entries: {acc.get('n_reentries_analyzed', 0)} | "
                    f"Flat: {acc.get('pct_flat_reentries', 0):.0f}% | "
                    f"Dip: {acc.get('pct_dip_reentries', 0):.0f}% | "
                    f"Momentum: {acc.get('pct_momentum_reentries', 0):.0f}%"
                )
            else:
                st.markdown("**Acumulación:** sin datos")

            ss = patrones.get("size_scaling", {})
            if ss.get("size_scaling_type"):
                st.markdown(f"**Size scaling:** `{ss['size_scaling_type']}`")
                ratio_2nd = ss.get("avg_size_ratio_2nd_to_1st")
                ratio_3rd = ss.get("avg_size_ratio_3rd_to_1st")
                caption = f"Markets multi-entry: {ss.get('n_markets_multi_entry', 0)}"
                if ratio_2nd is not None:
                    caption += f" | 2nd/1st: {ratio_2nd:.2f}x"
                if ratio_3rd is not None:
                    caption += f" | 3rd/1st: {ratio_3rd:.2f}x"
                st.caption(caption)
            else:
                st.markdown("**Size scaling:** sin datos")

        # Columna 2: Sesiones
        with col_b:
            ses = patrones.get("sesiones", {})
            if ses.get("n_sessions", 0) > 0:
                st.metric("Sesiones", ses["n_sessions"])
                st.caption(
                    f"Mediana trades/sesión: {ses.get('median_trades_per_session', '—')} | "
                    f"Duración media: {ses.get('avg_session_duration_min', '—')} min"
                )
                st.caption(
                    f"Markets/sesión: {ses.get('avg_markets_per_session', '—')} | "
                    f"Max trades: {ses.get('max_trades_single_session', '—')}"
                )
            else:
                st.markdown("**Sesiones:** sin datos")

            cm = patrones.get("ciclo_mercado", {})
            if cm.get("avg_entry_pct_lifecycle") is not None:
                st.markdown(f"**Ciclo de mercado:** {cm['avg_entry_pct_lifecycle']:.0f}% del lifecycle")
                st.caption(
                    f"Early (<25%): {cm.get('pct_early_entries', 0):.0f}% | "
                    f"Late (>75%): {cm.get('pct_late_entries', 0):.0f}% | "
                    f"Near deadline: {cm.get('adds_near_deadline', 0)}"
                )

        # Columna 3: Concentración + Salidas
        with col_c:
            conc = patrones.get("concentracion", {})
            if conc.get("gini_coefficient") is not None:
                st.markdown(f"**Concentración (Gini):** {conc['gini_coefficient']:.3f}")
                st.caption(
                    f"Top market: {conc.get('top_market_pct', 0):.1f}% | "
                    f"Top 3: {conc.get('top_3_markets_pct', 0):.1f}% | "
                    f"Markets para 80%: {conc.get('n_markets_for_80pct', '—')}"
                )
            else:
                st.markdown("**Concentración:** sin datos")

            sal = patrones.get("salidas", {})
            if sal.get("exit_timing"):
                st.markdown(f"**Salidas:** `{sal['exit_timing']}`")
                caption = f"Markets con exit: {sal.get('n_markets_with_exit', 0)} ({sal.get('pct_markets_with_exit', 0):.1f}%)"
                if sal.get("avg_exit_days_before_end") is not None:
                    caption += f" | Media: {sal['avg_exit_days_before_end']:.0f}d antes del cierre"
                st.caption(caption)
            else:
                st.markdown("**Salidas:** sin datos")

    # Bloque 5 — Narrativa LLM
    if narrativa:
        st.subheader("Análisis narrativo")
        st.markdown(
            f"""
            <div style="
                background-color: #1a1a2e;
                border-left: 4px solid #e94560;
                padding: 1.2rem 1.5rem;
                border-radius: 0.5rem;
                margin-bottom: 1rem;
                line-height: 1.6;
                color: #eee;
            ">{narrativa}</div>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Generado por Gemini 2.5 Flash")
    elif not GEMINI_API_KEY:
        st.info(
            "Para activar el análisis narrativo con IA, configura GEMINI_API_KEY en tu archivo .env. "
            "Puedes obtener una key gratuita en https://aistudio.google.com/apikey"
        )

    # Bloque 6 — Reglas operacionales
    if reglas:
        st.subheader("Reglas operacionales")
        st.caption("Checklist accionable para replicar esta estrategia — cada regla es una decisión SÍ/NO en tiempo real.")

        for cat, lista in reglas.items():
            with st.expander(f"**{cat}** ({len(lista)} reglas)", expanded=True):
                for regla_texto in lista:
                    st.checkbox(regla_texto, value=False, disabled=True, key=f"rule_{cat}_{regla_texto[:40]}")

        # Exportar
        txt = exportar_reglas_txt(reglas, estilo, wallet)
        st.download_button(
            label="Exportar reglas (.txt)",
            data=txt,
            file_name=f"reglas_{wallet[:10]}.txt",
            mime="text/plain",
        )

    # Bloque 7 — Scores de arquetipos
    st.subheader("Scores por arquetipo")
    for arq, score in sorted(scores.items(), key=lambda x: -x[1]):
        col_label, col_bar = st.columns([1, 3])
        col_label.markdown(f"**{arq}** — {score:.0%}")
        col_bar.progress(score)

    # Bloque 8 — Últimos 20 trades
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

    # Bloque 9 — Nota sobre datos no disponibles
    if features.get("win_rate") is None or features.get("roi_estimado") is None:
        st.info(
            "ℹ️ Algunas métricas (win rate, ROI) no están disponibles porque "
            "el campo outcome contiene el nombre del token, no el resultado "
            "de resolución del mercado."
        )
