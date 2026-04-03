"""Módulo de clasificación de arquetipos de trading para Polymarket.

Clasifica una cartera en uno de 7 arquetipos de estrategia basándose
en los features calculados por el analyzer. Las reglas se evalúan en
orden de prioridad; los scores dan una visión cuantitativa complementaria.
"""

import logging
from typing import Optional

from src.analyzer import calcular_features

logger = logging.getLogger(__name__)


# ── Normalización ─────────────────────────────────────────────────────────────


def _normalizar_features(features: dict) -> dict:
    """Normaliza hold_rate y pct_entradas_ultima_semana_mercado a rango 0-1.

    El analyzer los almacena como porcentajes (0-100), pero las reglas
    de clasificación usan fracciones (0-1). Si el valor es > 1, se divide
    entre 100.
    """
    f = dict(features)
    for key in ("hold_rate", "pct_entradas_ultima_semana_mercado"):
        val = f.get(key)
        if val is not None and val > 1:
            f[key] = val / 100.0
    return f


# ── Helpers de score ──────────────────────────────────────────────────────────


def _score_above(val: Optional[float], threshold: float) -> float:
    """Score 0-1 según cuánto supera val al threshold. 1.0 si val >= threshold."""
    if val is None:
        return 0.5
    if val >= threshold:
        return 1.0
    if threshold == 0:
        return 0.0
    return max(0.0, val / threshold)


def _score_below(val: Optional[float], threshold: float) -> float:
    """Score 0-1 según cuánto está val por debajo del threshold. 1.0 si val <= threshold."""
    if val is None:
        return 0.5
    if val <= threshold:
        return 1.0
    if threshold == 0:
        return 0.0
    return max(0.0, 1.0 - (val - threshold) / threshold)


# ── Cálculo de scores ────────────────────────────────────────────────────────


def _calcular_scores(f: dict) -> dict:
    """Calcula scores 0-1 para cada arquetipo basándose en proximidad a umbrales."""
    antelacion = f.get("antelacion_media_dias")
    pct_ult_sem = f.get("pct_entradas_ultima_semana_mercado")
    hold_rate = f.get("hold_rate")
    price_media = f.get("price_entrada_media")
    corr_sp = f.get("corr_size_price")
    win_rate = f.get("win_rate")
    dist = f.get("price_distribucion") or {}
    dist_0_20 = dist.get("0-20", 0)
    dist_40_60 = dist.get("40-60", 0)

    scores: dict[str, float] = {}

    # end-of-day
    scores["end-of-day"] = round(
        _score_below(antelacion, 3) * 0.4
        + _score_above(pct_ult_sem, 0.7) * 0.4
        + _score_above(hold_rate, 0.85) * 0.2,
        4,
    )

    # contrarian
    scores["contrarian"] = round(
        _score_below(price_media, 0.25) * 0.35
        + _score_above(antelacion, 10) * 0.35
        + _score_above(dist_0_20, 50) * 0.3,
        4,
    )

    # insider-like
    s_ins = (
        _score_above(antelacion, 20) * 0.4
        + _score_above(hold_rate, 0.90) * 0.35
        + (_score_above(win_rate, 0.65) if win_rate is not None else 0.5) * 0.25
    )
    scores["insider-like"] = round(s_ins, 4)

    # momentum
    scores["momentum"] = round(_score_above(corr_sp, 0.45), 4)

    # hold-to-resolution
    scores["hold-to-resolution"] = round(
        _score_above(hold_rate, 0.92) * 0.6
        + _score_above(antelacion, 3) * 0.4,
        4,
    )

    # bimodal / mixed
    scores["bimodal / mixed"] = round(
        _score_above(dist_0_20, 35) * 0.5
        + _score_above(dist_40_60, 35) * 0.5,
        4,
    )

    # value / mixed (baseline)
    scores["value / mixed"] = 0.3

    return scores


# ── Clasificación por reglas ─────────────────────────────────────────────────


def clasificar(features: dict) -> tuple[str, str, dict]:
    """Clasifica una cartera en un arquetipo de trading.

    Args:
        features: dict de features tal como devuelve calcular_features().

    Returns:
        Tupla (estilo, descripcion, scores) donde:
        - estilo: string del arquetipo detectado
        - descripcion: 1-2 frases explicativas con datos concretos
        - scores: dict {arquetipo: float 0-1} para todos los arquetipos
    """
    f = _normalizar_features(features)

    antelacion = f.get("antelacion_media_dias")
    pct_ult_sem = f.get("pct_entradas_ultima_semana_mercado")
    hold_rate = f.get("hold_rate")
    price_media = f.get("price_entrada_media")
    corr_sp = f.get("corr_size_price")
    win_rate = f.get("win_rate")
    dist = f.get("price_distribucion") or {}
    dist_0_20 = dist.get("0-20", 0)
    dist_40_60 = dist.get("40-60", 0)

    scores = _calcular_scores(f)

    estilo: Optional[str] = None
    desc: Optional[str] = None

    # 1. end-of-day
    if (antelacion is not None and antelacion < 3
            and pct_ult_sem is not None and pct_ult_sem > 0.7):
        estilo = "end-of-day"
        desc = (
            f"Opera en los últimos {antelacion:.1f} días antes del cierre. "
            f"El {pct_ult_sem:.0%} de sus trades están en la última semana del mercado."
        )

    # 2. contrarian
    if estilo is None and (
            price_media is not None and price_media < 0.25
            and antelacion is not None and antelacion > 10):
        estilo = "contrarian"
        desc = (
            f"Compra tokens con precio medio de {price_media:.0%}. "
            f"Apuesta contra el consenso con alta antelación."
        )

    # 3. insider-like
    if estilo is None and (
            antelacion is not None and antelacion > 20
            and hold_rate is not None and hold_rate > 0.90):
        if win_rate is None or win_rate > 0.65:
            estilo = "insider-like"
            desc = (
                f"Entra muy pronto ({antelacion:.0f} días antes) "
                f"y mantiene el {hold_rate:.0%} de las posiciones hasta resolución."
            )

    # 4. momentum
    if estilo is None and (corr_sp is not None and corr_sp > 0.45):
        estilo = "momentum"
        desc = (
            f"Aumenta el tamaño de posición cuando el precio sube "
            f"(correlación size-price: {corr_sp:.2f})."
        )

    # 5. hold-to-resolution
    if estilo is None and (
            hold_rate is not None and hold_rate > 0.92
            and antelacion is not None and antelacion >= 3):
        estilo = "hold-to-resolution"
        desc = (
            f"Mantiene el {hold_rate:.0%} de sus posiciones hasta resolución. "
            f"Alta convicción, sin cierres anticipados."
        )

    # 6. bimodal / mixed
    if estilo is None and (dist_0_20 > 35 and dist_40_60 > 35):
        estilo = "bimodal / mixed"
        desc = (
            f"Distribución de precios bimodal: compra tanto tokens muy baratos "
            f"({dist_0_20:.0f}%) como de probabilidad media ({dist_40_60:.0f}%). "
            f"Estrategia mixta no clasificable en un único arquetipo."
        )

    # 7. value / mixed (default)
    if estilo is None:
        estilo = "value / mixed"
        desc = (
            "No encaja claramente en ningún arquetipo específico. "
            "Estrategia diversificada o de valor."
        )

    return estilo, desc, scores


# ── Resumen en texto ──────────────────────────────────────────────────────────


def generar_resumen_texto(wallet: str, features: dict, estilo: str, desc: str) -> str:
    """Genera un bloque de texto formateado con el perfil clasificado.

    Args:
        wallet: dirección de la cartera.
        features: dict de features.
        estilo: arquetipo detectado.
        desc: descripción del arquetipo.

    Returns:
        String multilínea con el resumen completo.
    """
    f = _normalizar_features(features)
    wallet_short = f"{wallet[:10]}...{wallet[-4:]}"

    lineas = [
        f"{'=' * 60}",
        f"  Wallet:     {wallet_short}",
        f"  Arquetipo:  {estilo.upper()}",
        f"{'=' * 60}",
        "",
        desc,
        "",
        "── Métricas clave ──",
    ]

    metricas = [
        ("Trades totales", f.get("n_trades"), "{}"),
        ("Mercados operados", f.get("n_mercados"), "{}"),
        ("Antelación media", f.get("antelacion_media_dias"), "{:.2f} días"),
        ("% entradas última semana", f.get("pct_entradas_ultima_semana_mercado"), "{:.1%}"),
        ("Hold rate", f.get("hold_rate"), "{:.1%}"),
        ("Price entrada media", f.get("price_entrada_media"), "{:.4f}"),
        ("Price std", f.get("price_std"), "{:.4f}"),
        ("Importe medio", f.get("importe_medio"), "{:.2f} USDC"),
        ("Coef. variación", f.get("coef_variacion"), "{:.4f}"),
        ("Corr. size-price", f.get("corr_size_price"), "{:.4f}"),
        ("Hora entrada moda", f.get("hora_entrada_moda"), "{}h"),
        ("Categoría top", f.get("categoria_top"), "{}"),
    ]

    for nombre, valor, fmt in metricas:
        if valor is None:
            continue
        lineas.append(f"  {nombre:30s} {fmt.format(valor)}")

    dist = f.get("price_distribucion")
    if dist:
        lineas.append("")
        lineas.append("── Distribución de precios ──")
        for rango, pct in dist.items():
            bar = "█" * int(pct / 2)
            lineas.append(f"  {rango:10s} {pct:5.1f}% {bar}")

    if f.get("win_rate") is None:
        lineas.append("")
        lineas.append(
            "Nota: Rendimiento no calculable — el campo outcome no contiene"
        )
        lineas.append("el resultado de resolución, sino el nombre del token.")

    return "\n".join(lineas)


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    wallet = "0xc867f7b28a7cbe179e098dd07077f01f84e38b00"

    print(f"Calculando features para {wallet}...\n")
    features = calcular_features(wallet)

    estilo, desc, scores = clasificar(features)

    print(generar_resumen_texto(wallet, features, estilo, desc))

    print(f"\n{'=' * 60}")
    print("  SCORES POR ARQUETIPO")
    print(f"{'=' * 60}")
    for arq, score in sorted(scores.items(), key=lambda x: -x[1]):
        marker = " ◄" if arq == estilo else ""
        print(f"  {arq:25s} {score:.4f}{marker}")
