"""Módulo de análisis de comportamiento para carteras de Polymarket.

Calcula features de behavioral fingerprinting a partir de los trades
almacenados en DuckDB: timing, sizing, mercado y rendimiento.
"""

import logging
from collections import Counter
from typing import Optional

import numpy as np
import pandas as pd

from src.storage import get_connection

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _cargar_datos(wallet: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga trades y markets joinados para una wallet."""
    con = get_connection()
    try:
        trades = con.execute(
            "SELECT * FROM trades WHERE wallet = ? ORDER BY timestamp",
            [wallet],
        ).fetchdf()
        merged = con.execute(
            """
            SELECT t.*, m.question, m.category, m.end_date
            FROM trades t
            LEFT JOIN markets m ON t.market_id = m.id
            WHERE t.wallet = ?
            ORDER BY t.timestamp
            """,
            [wallet],
        ).fetchdf()
        return trades, merged
    finally:
        con.close()


# ── Features de timing ──────────────────────────────────────────────────────


def _features_timing(merged: pd.DataFrame) -> dict:
    """Calcula features de timing sobre trades joinados con markets."""
    resultado: dict = {}

    # Solo entradas (compras)
    compras = merged[merged["side"] == "BUY"]

    # antelacion_media_dias: días entre trade y end_date del mercado
    con_end_date = compras.dropna(subset=["end_date"])
    if len(con_end_date) > 0:
        delta = con_end_date["end_date"] - con_end_date["timestamp"]
        dias = delta.dt.total_seconds() / 86400
        resultado["antelacion_media_dias"] = round(float(dias.mean()), 2)
    else:
        logger.warning("antelacion_media_dias: no hay trades con end_date")
        resultado["antelacion_media_dias"] = None

    # hora_entrada_moda: hora más frecuente de entradas
    if len(compras) > 0:
        horas = compras["timestamp"].dt.hour
        resultado["hora_entrada_moda"] = int(horas.mode().iloc[0])
    else:
        logger.warning("hora_entrada_moda: no hay compras")
        resultado["hora_entrada_moda"] = None

    # pct_entradas_ultima_semana_mercado: % de trades en últimos 7 días antes del end_date
    if len(con_end_date) > 0:
        delta = con_end_date["end_date"] - con_end_date["timestamp"]
        dias = delta.dt.total_seconds() / 86400
        en_ultima_semana = (dias <= 7).sum()
        resultado["pct_entradas_ultima_semana_mercado"] = round(
            float(en_ultima_semana / len(con_end_date) * 100), 2
        )
    else:
        logger.warning("pct_entradas_ultima_semana_mercado: no hay trades con end_date")
        resultado["pct_entradas_ultima_semana_mercado"] = None

    return resultado


# ── Features de sizing ──────────────────────────────────────────────────────


def _features_sizing(trades: pd.DataFrame) -> dict:
    """Calcula features de sizing sobre compras."""
    resultado: dict = {}
    compras = trades[trades["side"] == "BUY"]

    # importe_medio
    if len(compras) > 0:
        resultado["importe_medio"] = round(float(compras["size"].mean()), 2)
    else:
        logger.warning("importe_medio: no hay compras")
        resultado["importe_medio"] = None

    # coef_variacion
    if len(compras) > 1:
        media = compras["size"].mean()
        if media > 0:
            resultado["coef_variacion"] = round(float(compras["size"].std() / media), 4)
        else:
            resultado["coef_variacion"] = None
    else:
        logger.warning("coef_variacion: necesita al menos 2 trades, tiene %d", len(compras))
        resultado["coef_variacion"] = None

    # corr_size_price
    if len(compras) >= 5:
        corr = compras["size"].corr(compras["price"])
        resultado["corr_size_price"] = round(float(corr), 4) if pd.notna(corr) else None
    else:
        logger.warning("corr_size_price: necesita al menos 5 trades, tiene %d", len(compras))
        resultado["corr_size_price"] = None

    return resultado


# ── Features de mercado ─────────────────────────────────────────────────────


def _features_mercado(trades: pd.DataFrame, merged: pd.DataFrame) -> dict:
    """Calcula features sobre el tipo de mercados operados."""
    resultado: dict = {}
    compras = trades[trades["side"] == "BUY"]
    compras_merged = merged[merged["side"] == "BUY"]

    # price_entrada_media y price_std
    if len(compras) > 0:
        resultado["price_entrada_media"] = round(float(compras["price"].mean()), 4)
        resultado["price_std"] = round(float(compras["price"].std()), 4)
    else:
        logger.warning("price_entrada_media/price_std: no hay compras")
        resultado["price_entrada_media"] = None
        resultado["price_std"] = None

    # categoria_top y pct_categoria_top
    if len(compras_merged) > 0:
        categorias = compras_merged["category"].fillna("").str.strip()
        categorias = categorias.replace("", pd.NA).dropna()
        if len(categorias) > 0:
            conteo = Counter(categorias)
            top_cat, top_count = conteo.most_common(1)[0]
            resultado["categoria_top"] = top_cat
            resultado["pct_categoria_top"] = round(float(top_count / len(compras_merged) * 100), 2)
        else:
            resultado["categoria_top"] = "unknown"
            resultado["pct_categoria_top"] = 100.0
    else:
        resultado["categoria_top"] = "unknown"
        resultado["pct_categoria_top"] = None

    # price_distribucion
    if len(compras) > 0:
        prices_pct = compras["price"] * 100
        bins = [0, 20, 40, 60, 80, 100]
        labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
        cortes = pd.cut(prices_pct, bins=bins, labels=labels, include_lowest=True)
        dist = cortes.value_counts(normalize=True) * 100
        resultado["price_distribucion"] = {
            label: round(float(dist.get(label, 0)), 2) for label in labels
        }
    else:
        resultado["price_distribucion"] = None

    return resultado


# ── Features de salidas y rendimiento ────────────────────────────────────────


def _features_rendimiento(trades: pd.DataFrame) -> dict:
    """Calcula features de rendimiento y salidas."""
    resultado: dict = {}

    compras = trades[trades["side"] == "BUY"]
    ventas = trades[trades["side"] == "SELL"]

    # win_rate: no disponible — el campo outcome contiene el nombre del token
    # (Yes, Up, Down, Lakers...) no si la posición ganó. Sin datos de resolución.
    logger.warning(
        "win_rate: no calculable — el campo 'outcome' contiene el nombre del token, "
        "no el resultado de la posición. Se necesitaría el estado de resolución del mercado."
    )
    resultado["win_rate"] = None

    # hold_rate: % de mercados donde solo hay BUYs (sin SELL → mantuvo hasta resolución)
    mercados_compra = set(compras["market_id"].unique())
    mercados_venta = set(ventas["market_id"].unique())
    if len(mercados_compra) > 0:
        mercados_held = mercados_compra - mercados_venta
        resultado["hold_rate"] = round(
            float(len(mercados_held) / len(mercados_compra) * 100), 2
        )
    else:
        logger.warning("hold_rate: no hay mercados con compras")
        resultado["hold_rate"] = None

    # roi_estimado: no calculable sin datos de resolución de mercados
    logger.warning(
        "roi_estimado: no calculable — sin datos de resolución de mercados. "
        "Se necesitaría saber qué mercados resolvieron y en qué dirección."
    )
    resultado["roi_estimado"] = None

    # n_trades y n_mercados
    resultado["n_trades"] = len(trades)
    resultado["n_mercados"] = int(trades["market_id"].nunique())

    return resultado


# ── Función principal ────────────────────────────────────────────────────────


def calcular_features(wallet: str) -> dict:
    """Calcula todos los features de behavioral fingerprinting para una wallet.

    Devuelve un dict con las siguientes claves agrupadas por categoría:

    TIMING:
        antelacion_media_dias, hora_entrada_moda, pct_entradas_ultima_semana_mercado
    SIZING:
        importe_medio, coef_variacion, corr_size_price
    MERCADO:
        price_entrada_media, price_std, categoria_top, pct_categoria_top, price_distribucion
    RENDIMIENTO:
        win_rate, hold_rate, roi_estimado, n_trades, n_mercados

    Los features que no se puedan calcular devuelven None.
    """
    trades, merged = _cargar_datos(wallet)

    if trades.empty:
        logger.warning("No hay trades para wallet %s", wallet)
        return {
            "antelacion_media_dias": None, "hora_entrada_moda": None,
            "pct_entradas_ultima_semana_mercado": None,
            "importe_medio": None, "coef_variacion": None, "corr_size_price": None,
            "price_entrada_media": None, "price_std": None,
            "categoria_top": None, "pct_categoria_top": None, "price_distribucion": None,
            "win_rate": None, "hold_rate": None, "roi_estimado": None,
            "n_trades": 0, "n_mercados": 0,
        }

    features: dict = {}
    features.update(_features_timing(merged))
    features.update(_features_sizing(trades))
    features.update(_features_mercado(trades, merged))
    features.update(_features_rendimiento(trades))

    logger.info("Features calculados para %s: %d features", wallet, len(features))
    return features


# ── Resumen en texto ─────────────────────────────────────────────────────────


def resumen_texto(wallet: str, features: dict) -> str:
    """Genera un resumen legible de los features calculados."""

    def fmt(valor: object) -> str:
        if valor is None:
            return "sin datos"
        if isinstance(valor, float):
            return f"{valor:.2f}"
        if isinstance(valor, dict):
            return ", ".join(f"{k}: {v:.1f}%" for k, v in valor.items())
        return str(valor)

    lineas = [
        f"=== Perfil de comportamiento: {wallet[:10]}...{wallet[-6:]} ===",
        "",
        "TIMING:",
        f"  Antelación media al cierre:  {fmt(features.get('antelacion_media_dias'))} días",
        f"  Hora de entrada más frecuente: {fmt(features.get('hora_entrada_moda'))}h",
        f"  Entradas en última semana:     {fmt(features.get('pct_entradas_ultima_semana_mercado'))}%",
        "",
        "SIZING:",
        f"  Importe medio (USDC):  {fmt(features.get('importe_medio'))}",
        f"  Coef. de variación:    {fmt(features.get('coef_variacion'))}",
        f"  Corr. size-price:      {fmt(features.get('corr_size_price'))}",
        "",
        "MERCADO:",
        f"  Price entrada media:   {fmt(features.get('price_entrada_media'))}",
        f"  Price std:             {fmt(features.get('price_std'))}",
        f"  Categoría top:         {fmt(features.get('categoria_top'))}",
        f"  % categoría top:       {fmt(features.get('pct_categoria_top'))}%",
        f"  Distribución de price: {fmt(features.get('price_distribucion'))}",
        "",
        "RENDIMIENTO:",
        f"  Win rate:       {fmt(features.get('win_rate'))}",
        f"  Hold rate:      {fmt(features.get('hold_rate'))}%",
        f"  ROI estimado:   {fmt(features.get('roi_estimado'))}",
        f"  Total trades:   {fmt(features.get('n_trades'))}",
        f"  Mercados operados: {fmt(features.get('n_mercados'))}",
    ]
    return "\n".join(lineas)


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    wallet = "0xc867f7b28a7cbe179e098dd07077f01f84e38b00"

    print(f"Calculando features para {wallet}...\n")
    features = calcular_features(wallet)

    print("=== FEATURES ===")
    for k, v in features.items():
        print(f"  {k}: {v}")

    print()
    print(resumen_texto(wallet, features))
