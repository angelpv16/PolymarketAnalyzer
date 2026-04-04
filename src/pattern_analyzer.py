"""Módulo de análisis de patrones secuenciales para carteras de Polymarket.

Detecta 6 patrones de comportamiento secuencial dentro de cada mercado:
acumulación, escalado de tamaño, sesiones, salidas, ciclo de mercado y concentración.
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from src.storage import get_connection

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _cargar_datos(wallet: str) -> pd.DataFrame:
    """Carga trades joinados con markets para una wallet."""
    con = get_connection()
    try:
        merged = con.execute(
            """
            SELECT t.*, m.question, m.category, m.end_date,
                   m.resolved, m.winning_outcome
            FROM trades t
            LEFT JOIN markets m ON t.market_id = m.id
            WHERE t.wallet = ?
            ORDER BY t.timestamp
            """,
            [wallet],
        ).fetchdf()
        return merged
    finally:
        con.close()


# ── P1: Acumulación ─────────────────────────────────────────────────────────


def _patron_acumulacion(merged: pd.DataFrame) -> dict:
    """Analiza patrón de re-entradas en el mismo mercado.

    Compara precios entre BUYs consecutivos para clasificar:
    - flat_accumulator: >=70% re-entries con delta <=1c
    - dip_buyer: mayoría compra a precios más bajos
    - momentum_chaser: mayoría compra a precios más altos
    """
    compras = merged[merged["side"] == "BUY"].sort_values("timestamp")

    deltas = []
    flat_count = 0
    dip_count = 0
    momentum_count = 0

    for market_id, group in compras.groupby("market_id"):
        if len(group) < 2:
            continue
        prices = group["price"].values
        for i in range(1, len(prices)):
            delta = prices[i] - prices[i - 1]
            deltas.append(delta)
            if abs(delta) <= 0.01:
                flat_count += 1
            elif delta < 0:
                dip_count += 1
            else:
                momentum_count += 1

    n_reentries = len(deltas)
    if n_reentries == 0:
        return {
            "accumulation_type": None,
            "pct_flat_reentries": None,
            "pct_dip_reentries": None,
            "pct_momentum_reentries": None,
            "avg_price_delta_cents": None,
            "n_reentries_analyzed": 0,
        }

    pct_flat = round(flat_count / n_reentries * 100, 2)
    pct_dip = round(dip_count / n_reentries * 100, 2)
    pct_momentum = round(momentum_count / n_reentries * 100, 2)
    avg_delta = round(float(np.mean(deltas)) * 100, 4)  # en centavos

    if pct_flat >= 70:
        acc_type = "flat_accumulator"
    elif dip_count > momentum_count:
        acc_type = "dip_buyer"
    else:
        acc_type = "momentum_chaser"

    return {
        "accumulation_type": acc_type,
        "pct_flat_reentries": pct_flat,
        "pct_dip_reentries": pct_dip,
        "pct_momentum_reentries": pct_momentum,
        "avg_price_delta_cents": avg_delta,
        "n_reentries_analyzed": n_reentries,
    }


# ── P2: Escalado de tamaño ──────────────────────────────────────────────────


def _patron_size_scaling(merged: pd.DataFrame) -> dict:
    """Analiza cómo escala el tamaño de posición entre entradas sucesivas.

    Clasifica:
    - inverse_pyramid: ratio <0.75 (entradas decrecientes)
    - uniform: ratio 0.75-1.25
    - escalating: ratio >1.25 (entradas crecientes)
    """
    compras = merged[merged["side"] == "BUY"].sort_values("timestamp")

    ratios_2nd = []
    ratios_3rd = []
    n_multi = 0

    for market_id, group in compras.groupby("market_id"):
        if len(group) < 2:
            continue
        n_multi += 1
        sizes = group["size"].values
        if sizes[0] > 0:
            ratios_2nd.append(sizes[1] / sizes[0])
        if len(sizes) >= 3 and sizes[0] > 0:
            ratios_3rd.append(sizes[2] / sizes[0])

    if not ratios_2nd:
        return {
            "size_scaling_type": None,
            "avg_size_ratio_2nd_to_1st": None,
            "avg_size_ratio_3rd_to_1st": None,
            "n_markets_multi_entry": 0,
        }

    avg_2nd = round(float(np.mean(ratios_2nd)), 4)
    avg_3rd = round(float(np.mean(ratios_3rd)), 4) if ratios_3rd else None

    if avg_2nd < 0.75:
        scaling_type = "inverse_pyramid"
    elif avg_2nd > 1.25:
        scaling_type = "escalating"
    else:
        scaling_type = "uniform"

    return {
        "size_scaling_type": scaling_type,
        "avg_size_ratio_2nd_to_1st": avg_2nd,
        "avg_size_ratio_3rd_to_1st": avg_3rd,
        "n_markets_multi_entry": n_multi,
    }


# ── P3: Sesiones ────────────────────────────────────────────────────────────


def _patron_sesiones(merged: pd.DataFrame) -> dict:
    """Agrupa trades en sesiones (gap >1h = nueva sesión)."""
    if merged.empty:
        return {
            "n_sessions": 0,
            "median_trades_per_session": None,
            "avg_session_duration_min": None,
            "avg_markets_per_session": None,
            "max_trades_single_session": None,
        }

    sorted_df = merged.sort_values("timestamp").copy()
    timestamps = sorted_df["timestamp"].values

    # Asignar sesiones
    session_ids = [0]
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]) / np.timedelta64(1, "h")
        if gap > 1:
            session_ids.append(session_ids[-1] + 1)
        else:
            session_ids.append(session_ids[-1])

    sorted_df["session_id"] = session_ids
    n_sessions = sorted_df["session_id"].nunique()

    trades_per_session = sorted_df.groupby("session_id").size()
    markets_per_session = sorted_df.groupby("session_id")["market_id"].nunique()

    # Duración de cada sesión
    durations = []
    for _, session in sorted_df.groupby("session_id"):
        ts = session["timestamp"]
        dur_min = (ts.max() - ts.min()) / pd.Timedelta(minutes=1)
        durations.append(dur_min)

    return {
        "n_sessions": n_sessions,
        "median_trades_per_session": round(float(trades_per_session.median()), 1),
        "avg_session_duration_min": round(float(np.mean(durations)), 1),
        "avg_markets_per_session": round(float(markets_per_session.mean()), 1),
        "max_trades_single_session": int(trades_per_session.max()),
    }


# ── P4: Salidas ──────────────────────────────────────────────────────────────


def _patron_salidas(merged: pd.DataFrame) -> dict:
    """Analiza patrón de salidas (SELLs) y su timing relativo al end_date."""
    compras = merged[merged["side"] == "BUY"]
    ventas = merged[merged["side"] == "SELL"]

    mercados_compra = set(compras["market_id"].unique())
    mercados_venta = set(ventas["market_id"].unique())

    if not mercados_compra:
        return {
            "pct_markets_with_exit": None,
            "n_markets_with_exit": 0,
            "exit_timing": None,
            "avg_exit_days_before_end": None,
        }

    n_with_exit = len(mercados_venta & mercados_compra)
    pct_with_exit = round(n_with_exit / len(mercados_compra) * 100, 2)

    # Timing de salidas
    ventas_con_end = ventas.dropna(subset=["end_date"])
    exit_days = []
    if len(ventas_con_end) > 0:
        delta = ventas_con_end["end_date"] - ventas_con_end["timestamp"]
        exit_days = (delta.dt.total_seconds() / 86400).tolist()

    avg_exit_days = round(float(np.mean(exit_days)), 1) if exit_days else None

    if pct_with_exit < 10:
        timing = "no_exit"
    elif avg_exit_days is not None and avg_exit_days > 14:
        timing = "early_exit"
    elif avg_exit_days is not None and avg_exit_days < 3:
        timing = "late_exit"
    else:
        timing = "mixed_exit"

    return {
        "pct_markets_with_exit": pct_with_exit,
        "n_markets_with_exit": n_with_exit,
        "exit_timing": timing,
        "avg_exit_days_before_end": avg_exit_days,
    }


# ── P5: Ciclo de mercado ────────────────────────────────────────────────────


def _patron_ciclo_mercado(merged: pd.DataFrame) -> dict:
    """Analiza cuándo entra la wallet relativo al ciclo de vida del mercado."""
    compras = merged[merged["side"] == "BUY"].dropna(subset=["end_date"])

    if compras.empty:
        return {
            "avg_entry_pct_lifecycle": None,
            "pct_early_entries": None,
            "pct_late_entries": None,
            "adds_near_deadline": None,
        }

    # Para cada compra, calcular en qué punto del ciclo entra
    # Usamos: primera compra del mercado como proxy de "inicio"
    lifecycle_pcts = []
    near_deadline_count = 0

    for market_id, group in compras.groupby("market_id"):
        end_date = group["end_date"].iloc[0]
        # Usar la primera compra global del mercado como referencia de inicio
        first_buy = group["timestamp"].min()
        market_span = (end_date - first_buy).total_seconds()
        if market_span <= 0:
            continue

        for _, row in group.iterrows():
            elapsed = (row["timestamp"] - first_buy).total_seconds()
            pct = elapsed / market_span * 100
            lifecycle_pcts.append(pct)

            days_before_end = (end_date - row["timestamp"]).total_seconds() / 86400
            if days_before_end <= 7:
                near_deadline_count += 1

    if not lifecycle_pcts:
        return {
            "avg_entry_pct_lifecycle": None,
            "pct_early_entries": None,
            "pct_late_entries": None,
            "adds_near_deadline": None,
        }

    pcts = np.array(lifecycle_pcts)
    n_total = len(pcts)

    return {
        "avg_entry_pct_lifecycle": round(float(pcts.mean()), 2),
        "pct_early_entries": round(float((pcts < 25).sum() / n_total * 100), 2),
        "pct_late_entries": round(float((pcts > 75).sum() / n_total * 100), 2),
        "adds_near_deadline": near_deadline_count,
    }


# ── P6: Concentración ───────────────────────────────────────────────────────


def _patron_concentracion(merged: pd.DataFrame) -> dict:
    """Calcula concentración del volumen por mercado usando Gini coefficient."""
    compras = merged[merged["side"] == "BUY"]

    if compras.empty:
        return {
            "gini_coefficient": None,
            "top_market_pct": None,
            "top_3_markets_pct": None,
            "n_markets_for_80pct": None,
        }

    vol_by_market = compras.groupby("market_id")["size"].sum().sort_values(ascending=False)
    total_vol = vol_by_market.sum()

    if total_vol == 0:
        return {
            "gini_coefficient": None,
            "top_market_pct": None,
            "top_3_markets_pct": None,
            "n_markets_for_80pct": None,
        }

    # Gini coefficient
    values = np.sort(vol_by_market.values)
    n = len(values)
    index = np.arange(1, n + 1)
    gini = float((2 * np.sum(index * values) - (n + 1) * np.sum(values)) / (n * np.sum(values)))
    gini = round(max(0.0, gini), 4)

    # Top market %
    top_market_pct = round(float(vol_by_market.iloc[0] / total_vol * 100), 2)

    # Top 3 markets %
    top_3 = vol_by_market.head(3).sum()
    top_3_pct = round(float(top_3 / total_vol * 100), 2)

    # Markets needed for 80%
    cumsum = vol_by_market.cumsum()
    threshold_80 = total_vol * 0.80
    n_for_80 = int((cumsum <= threshold_80).sum() + 1)
    n_for_80 = min(n_for_80, n)

    return {
        "gini_coefficient": gini,
        "top_market_pct": top_market_pct,
        "top_3_markets_pct": top_3_pct,
        "n_markets_for_80pct": n_for_80,
    }


# ── Función principal ────────────────────────────────────────────────────────


def calcular_patrones(wallet: str) -> dict:
    """Calcula los 6 patrones secuenciales para una wallet.

    Devuelve un dict con las siguientes secciones:
    - acumulacion: tipo de re-entrada, deltas de precio
    - size_scaling: escalado de tamaño entre entradas
    - sesiones: agrupación temporal de trades
    - salidas: patrón de exits y timing
    - ciclo_mercado: momento de entrada en el ciclo de vida
    - concentracion: distribución de volumen entre mercados
    """
    merged = _cargar_datos(wallet)

    if merged.empty:
        logger.warning("No hay trades para wallet %s", wallet)
        return {
            "acumulacion": _patron_acumulacion(merged),
            "size_scaling": _patron_size_scaling(merged),
            "sesiones": _patron_sesiones(merged),
            "salidas": _patron_salidas(merged),
            "ciclo_mercado": _patron_ciclo_mercado(merged),
            "concentracion": _patron_concentracion(merged),
        }

    patrones: dict = {
        "acumulacion": _patron_acumulacion(merged),
        "size_scaling": _patron_size_scaling(merged),
        "sesiones": _patron_sesiones(merged),
        "salidas": _patron_salidas(merged),
        "ciclo_mercado": _patron_ciclo_mercado(merged),
        "concentracion": _patron_concentracion(merged),
    }

    logger.info("Patrones calculados para %s: %d grupos", wallet, len(patrones))
    return patrones


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    wallet = "0xc867f7b28a7cbe179e098dd07077f01f84e38b00"

    print(f"Calculando patrones secuenciales para {wallet}...\n")
    patrones = calcular_patrones(wallet)

    for grupo, datos in patrones.items():
        print(f"\n=== {grupo.upper()} ===")
        for k, v in datos.items():
            print(f"  {k}: {v}")
