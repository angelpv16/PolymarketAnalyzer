"""Módulo de extracción de reglas operacionales para PolymarketAnalyzer.

Traduce features, patrones y arquetipo en reglas accionables de trading
agrupadas en 4 categorías: selección de mercado, entrada, gestión de
posición y salida. Cada regla es una decisión SÍ/NO en tiempo real.

Usa Gemini si hay API key; si no, genera reglas directamente de los datos.
"""

import json
import logging
import re
from typing import Optional

from src.config import GEMINI_API_KEY

logger = logging.getLogger(__name__)

CATEGORIAS = [
    "Selección de mercado",
    "Entrada",
    "Gestión de posición",
    "Salida",
]

SYSTEM_INSTRUCTION = (
    "Eres un analista cuantitativo que convierte datos de comportamiento de trading "
    "en reglas operacionales concretas y accionables.\n\n"
    "REGLAS ESTRICTAS:\n"
    "- Cada regla debe ser una decisión SÍ/NO que se pueda evaluar EN EL MOMENTO de ejecutar un trade\n"
    "- Cada regla debe contener al menos un umbral numérico extraído de los datos reales\n"
    "- NO describas qué hizo la wallet en el pasado — di qué HACER en cada nueva operación\n"
    "- INCORRECTO: 'Operó 358 mercados' → CORRECTO: 'No concentres más del 10% en un solo mercado'\n"
    "- INCORRECTO: 'Win rate fue 24%' → CORRECTO: 'Acepta posiciones con probabilidad implícita ≤26%'\n"
    "- Usa verbos en imperativo: 'Compra', 'Limita', 'No entres', 'Mantén', 'Espera'\n"
    "- Mínimo 3 reglas y máximo 5 reglas por categoría\n"
    "- Responde SOLO con JSON válido, sin markdown ni texto adicional\n\n"
    "Formato exacto de respuesta:\n"
    '{"Selección de mercado": ["regla1", "regla2", ...], '
    '"Entrada": ["regla1", ...], '
    '"Gestión de posición": ["regla1", ...], '
    '"Salida": ["regla1", ...]}'
)


def _construir_prompt(
    wallet: str,
    features: dict,
    patrones: dict,
    estilo: str,
    descripcion: str,
) -> str:
    """Construye el prompt con datos reales para Gemini."""
    secciones = [
        f"WALLET: {wallet}",
        f"ARQUETIPO: {estilo} — {descripcion}",
    ]

    # Features
    lineas = []
    for k, v in features.items():
        if v is not None:
            lineas.append(f"  {k}: {v}")
    if lineas:
        secciones.append("FEATURES AGREGADOS:\n" + "\n".join(lineas))

    # Patrones
    lineas_pat = []
    for grupo, datos in patrones.items():
        vals = [f"    {k}: {v}" for k, v in datos.items() if v is not None]
        if vals:
            lineas_pat.append(f"  [{grupo}]\n" + "\n".join(vals))
    if lineas_pat:
        secciones.append("PATRONES SECUENCIALES:\n" + "\n".join(lineas_pat))

    secciones.append(
        "Genera reglas operacionales accionables en tiempo real para las 4 categorías. "
        "Cada regla debe poder responderse SÍ/NO al momento de ejecutar un trade. "
        "Usa los números reales de arriba como umbrales."
    )

    return "\n\n".join(secciones)


def _extraer_reglas_gemini(
    wallet: str,
    features: dict,
    patrones: dict,
    estilo: str,
    descripcion: str,
) -> Optional[dict[str, list[str]]]:
    """Genera reglas usando Gemini. Retorna None si falla."""
    if GEMINI_API_KEY is None:
        return None

    user_prompt = _construir_prompt(wallet, features, patrones, estilo, descripcion)

    try:
        from google import genai

        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=user_prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.2,
                max_output_tokens=2000,
            ),
        )

        texto = response.text.strip()
        # Limpiar posibles bloques de código markdown
        texto = re.sub(r"^```(?:json)?\s*", "", texto)
        texto = re.sub(r"\s*```$", "", texto)

        reglas = json.loads(texto)

        # Validar estructura
        for cat in CATEGORIAS:
            if cat not in reglas or not isinstance(reglas[cat], list):
                logger.warning("Gemini omitió categoría '%s', usando fallback", cat)
                return None

        logger.info("Reglas generadas con Gemini para %s", wallet)
        return reglas

    except Exception as e:
        logger.error("Error generando reglas con Gemini: %s", e)
        return None


def _reglas_fallback(
    features: dict,
    patrones: dict,
    estilo: str,
) -> dict[str, list[str]]:
    """Genera reglas directamente de los datos sin LLM."""
    reglas: dict[str, list[str]] = {cat: [] for cat in CATEGORIAS}

    # ── Extraer valores ──
    price_media = features.get("price_entrada_media")
    dist = features.get("price_distribucion") or {}
    antelacion = features.get("antelacion_media_dias")
    hora_moda = features.get("hora_entrada_moda")
    importe_medio = features.get("importe_medio")
    coef_var = features.get("coef_variacion")
    hold_rate = features.get("hold_rate")
    pct_ult_sem = features.get("pct_entradas_ultima_semana_mercado")

    acc = patrones.get("acumulacion", {})
    ss = patrones.get("size_scaling", {})
    ses = patrones.get("sesiones", {})
    sal = patrones.get("salidas", {})
    conc = patrones.get("concentracion", {})
    ciclo = patrones.get("ciclo_mercado", {})

    # ── Selección de mercado ──
    if price_media is not None:
        precio_cents = round(price_media * 100)
        reglas["Selección de mercado"].append(
            f"Solo entra en mercados donde haya tokens disponibles a ≤{precio_cents}¢."
        )

    dist_0_20 = dist.get("0-20", 0)
    dist_40_60 = dist.get("40-60", 0)
    if dist_0_20 > 30 and dist_40_60 > 30:
        reglas["Selección de mercado"].append(
            f"Prioriza mercados con tokens en rango 0-20¢ ({dist_0_20:.0f}% de tus entradas) "
            f"o en rango 40-60¢ ({dist_40_60:.0f}%)."
        )
    elif dist_0_20 > 40:
        reglas["Selección de mercado"].append(
            f"Prioriza mercados con tokens en rango 0-20¢ (históricamente {dist_0_20:.0f}% de entradas)."
        )

    top_pct = conc.get("top_market_pct")
    if top_pct is not None:
        limite = round(top_pct + 2)
        reglas["Selección de mercado"].append(
            f"No concentres más del {limite}% del capital en un solo mercado."
        )

    top_3_pct = conc.get("top_3_markets_pct")
    if top_3_pct is not None:
        limite_3 = round(top_3_pct + 5)
        reglas["Selección de mercado"].append(
            f"Los 3 mercados con mayor posición no deben sumar más del {limite_3}% del capital total."
        )

    # ── Entrada ──
    if antelacion is not None:
        horas = round(antelacion * 24)
        if horas < 24:
            reglas["Entrada"].append(
                f"Entra solo en las últimas {max(horas, 1)}h antes del cierre del mercado."
            )
        else:
            reglas["Entrada"].append(
                f"Entra con un máximo de {antelacion:.1f} días de antelación al cierre."
            )

    if hora_moda is not None:
        reglas["Entrada"].append(
            f"Programa tus sesiones de trading alrededor de las {hora_moda}:00 UTC."
        )

    acc_type = acc.get("accumulation_type")
    pct_flat = acc.get("pct_flat_reentries")
    if acc_type == "flat_accumulator" and pct_flat is not None:
        reglas["Entrada"].append(
            f"Al re-entrar en un mercado, mantén el precio dentro de ±1¢ del precio anterior "
            f"(acumulación plana, {pct_flat:.0f}% de tus re-entries fueron así)."
        )
    elif acc_type == "dip_buyer":
        reglas["Entrada"].append(
            "Solo re-entra en un mercado si el precio actual es menor que tu última compra."
        )
    elif acc_type == "momentum_chaser":
        reglas["Entrada"].append(
            "Re-entra en un mercado solo si el precio ha subido desde tu última compra."
        )

    ratio_2nd = ss.get("avg_size_ratio_2nd_to_1st")
    if ratio_2nd is not None:
        reglas["Entrada"].append(
            f"En la 2ª entrada a un mercado, usa ~{ratio_2nd:.1f}x el tamaño de la 1ª entrada."
        )

    # ── Gestión de posición ──
    if importe_medio is not None:
        reglas["Gestión de posición"].append(
            f"Limita cada trade individual a ~${importe_medio:.0f} USDC como importe base."
        )

    if coef_var is not None and coef_var > 2:
        reglas["Gestión de posición"].append(
            f"Permite variación alta en tamaño de posición (CV: {coef_var:.1f}), "
            f"pero no superes {importe_medio * 5:.0f} USDC en un solo trade."
            if importe_medio is not None else
            f"Permite variación alta en tamaño de posición (CV actual: {coef_var:.1f})."
        )

    med_trades = ses.get("median_trades_per_session")
    dur_media = ses.get("avg_session_duration_min")
    mkts_sesion = ses.get("avg_markets_per_session")
    if med_trades is not None and dur_media is not None:
        reglas["Gestión de posición"].append(
            f"Limita cada sesión a ~{med_trades:.0f} trades en ~{dur_media:.0f} minutos."
        )
    if mkts_sesion is not None:
        reglas["Gestión de posición"].append(
            f"Cubre ~{mkts_sesion:.0f} mercados distintos por sesión de trading."
        )

    # ── Salida ──
    if hold_rate is not None and hold_rate > 80:
        reglas["Salida"].append(
            f"Mantén la posición hasta resolución del mercado (hold rate objetivo: >{hold_rate:.0f}%)."
        )

    pct_exit = sal.get("pct_markets_with_exit")
    if pct_exit is not None and pct_exit < 15:
        reglas["Salida"].append(
            f"No vendas antes de la resolución salvo en casos excepcionales "
            f"(actualmente solo {pct_exit:.1f}% de mercados tienen salida anticipada)."
        )
    elif pct_exit is not None:
        avg_exit_days = sal.get("avg_exit_days_before_end")
        if avg_exit_days is not None:
            reglas["Salida"].append(
                f"Si decides salir, hazlo ~{avg_exit_days:.0f} días antes del cierre."
            )

    exit_timing = sal.get("exit_timing")
    if exit_timing == "no_exit":
        reglas["Salida"].append(
            "No uses stop-loss ni salidas anticipadas; deja que el mercado resuelva."
        )
    elif exit_timing == "early_exit":
        reglas["Salida"].append(
            "Considera salir si la posición pierde más del 50% de su valor antes del cierre."
        )

    win_rate = features.get("win_rate")
    if win_rate is not None and win_rate < 40:
        reglas["Salida"].append(
            f"Acepta un win rate bajo (~{win_rate:.0f}%); la estrategia depende del volumen, "
            f"no de acertar cada trade."
        )

    # Garantizar mínimo 3 reglas por categoría
    defaults = {
        "Selección de mercado": [
            "Verifica que el mercado tenga liquidez suficiente antes de entrar.",
            "Evita mercados que cierren en menos de 1 hora si no los has analizado.",
            "Comprueba que el spread bid-ask sea razonable antes de colocar la orden.",
        ],
        "Entrada": [
            "Confirma que el precio actual está dentro de tu rango objetivo antes de comprar.",
            "No entres si el mercado ya ha movido más del 15% en la última hora.",
            "Verifica que tienes capital disponible suficiente antes de cada entrada.",
        ],
        "Gestión de posición": [
            "Revisa tus posiciones abiertas al inicio de cada sesión.",
            "No abras más de 15 posiciones nuevas en una misma sesión.",
            "Registra cada trade para evaluar el rendimiento periódicamente.",
        ],
        "Salida": [
            "No cierres una posición solo por impaciencia; respeta la tesis original.",
            "Si el mercado cambia de naturaleza (nueva información clave), re-evalúa.",
            "Revisa las posiciones resueltas semanalmente para ajustar la estrategia.",
        ],
    }

    for cat in CATEGORIAS:
        while len(reglas[cat]) < 3:
            idx = len(reglas[cat])
            if idx < len(defaults[cat]):
                reglas[cat].append(defaults[cat][idx])
            else:
                break

    return reglas


def extraer_reglas(
    wallet: str,
    features: dict,
    patrones: dict,
    estilo: str,
    descripcion: str,
) -> dict[str, list[str]]:
    """Extrae reglas operacionales accionables para replicar la estrategia.

    Intenta con Gemini primero; si no hay API key o falla, usa fallback
    basado en cálculos directos de los datos.

    Args:
        wallet: Dirección de la cartera.
        features: Dict de features de calcular_features().
        patrones: Dict de patrones de calcular_patrones().
        estilo: Arquetipo detectado.
        descripcion: Descripción del arquetipo.

    Returns:
        Dict con 4 categorías, cada una con lista de reglas (strings).
    """
    reglas = _extraer_reglas_gemini(wallet, features, patrones, estilo, descripcion)

    if reglas is None:
        logger.info("Usando reglas fallback (sin Gemini)")
        reglas = _reglas_fallback(features, patrones, estilo)

    return reglas


def exportar_reglas_txt(
    reglas: dict[str, list[str]],
    estilo: str,
    wallet: str,
) -> str:
    """Formatea las reglas como texto plano para exportación.

    Returns:
        String con las reglas formateadas listas para guardar como .txt.
    """
    lineas = [
        f"REGLAS OPERACIONALES — {estilo.upper()}",
        f"Wallet: {wallet[:10]}...{wallet[-4:]}",
        "=" * 50,
        "",
    ]

    for cat, lista in reglas.items():
        lineas.append(f"## {cat.upper()}")
        lineas.append("")
        for i, regla in enumerate(lista, 1):
            lineas.append(f"  [ ] {i}. {regla}")
        lineas.append("")

    lineas.append("=" * 50)
    lineas.append("Generado por PolymarketAnalyzer")

    return "\n".join(lineas)


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    sys.stdout.reconfigure(encoding="utf-8")

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    from src.analyzer import calcular_features
    from src.classifier import clasificar
    from src.pattern_analyzer import calcular_patrones
    from src.storage import get_connection

    con = get_connection()
    wallet = con.execute("SELECT DISTINCT wallet FROM trades LIMIT 1").fetchone()[0]
    con.close()

    features = calcular_features(wallet)
    patrones = calcular_patrones(wallet)
    estilo, descripcion, _ = clasificar(features)

    reglas = extraer_reglas(wallet, features, patrones, estilo, descripcion)

    print(exportar_reglas_txt(reglas, estilo, wallet))
