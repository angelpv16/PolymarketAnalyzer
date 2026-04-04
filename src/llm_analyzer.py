"""Módulo de análisis narrativo con Gemini para PolymarketAnalyzer.

Genera una interpretación narrativa del comportamiento de una wallet
usando Gemini 2.5 Flash. Si no hay API key configurada, retorna None.
"""

import logging

from src.config import GEMINI_API_KEY

logger = logging.getLogger(__name__)

SYSTEM_INSTRUCTION = (
    "Eres un analista cuantitativo de trading en mercados de predicción (Polymarket). "
    "Recibes datos estructurados del comportamiento de una wallet y debes producir "
    "una interpretación narrativa concisa y basada en datos, en español.\n\n"
    "Reglas:\n"
    "- 3-4 párrafos cortos en español\n"
    "- SIEMPRE cita números específicos de los datos\n"
    "- Párrafo 1: identidad de trading y arquetipo\n"
    "- Párrafo 2: patrones comportamentales más distintivos\n"
    "- Párrafo 3: perfil de riesgo y edge\n"
    "- Párrafo 4 (opcional): anomalías o lo que distingue esta wallet\n"
    "- Si un dato es None, ignóralo — no especules\n"
    "- Sin headers markdown, solo párrafos\n"
    "- Máximo 300 palabras"
)


def _construir_prompt(
    wallet: str,
    features: dict,
    patrones: dict,
    estilo: str,
    descripcion: str,
    scores: dict,
) -> tuple[str, str]:
    """Construye el system instruction y user prompt para Gemini.

    Returns:
        Tupla (system_instruction, user_prompt).
    """
    # --- User prompt ---
    secciones = []

    secciones.append(f"WALLET: {wallet}")
    secciones.append(f"ARCHETYPE: {estilo} — {descripcion}")

    # Aggregate features
    lineas_feat = []
    for k, v in features.items():
        if v is not None and k != "price_distribucion":
            lineas_feat.append(f"  {k}: {v}")
    if features.get("price_distribucion"):
        dist = features["price_distribucion"]
        lineas_feat.append(f"  price_distribucion: {dist}")
    if lineas_feat:
        secciones.append("AGGREGATE FEATURES:\n" + "\n".join(lineas_feat))

    # Sequential patterns
    lineas_pat = []
    for grupo, datos in patrones.items():
        vals = []
        for k, v in datos.items():
            if v is not None:
                vals.append(f"    {k}: {v}")
        if vals:
            lineas_pat.append(f"  [{grupo}]\n" + "\n".join(vals))
    if lineas_pat:
        secciones.append("SEQUENTIAL PATTERNS:\n" + "\n".join(lineas_pat))

    # Archetype scores
    lineas_scores = []
    for arq, score in sorted(scores.items(), key=lambda x: -x[1]):
        lineas_scores.append(f"  {arq}: {score:.4f}")
    if lineas_scores:
        secciones.append("ARCHETYPE SCORES:\n" + "\n".join(lineas_scores))

    user_prompt = "\n\n".join(secciones)
    return SYSTEM_INSTRUCTION, user_prompt


def generar_narrativa(
    wallet: str,
    features: dict,
    patrones: dict,
    estilo: str,
    descripcion: str,
    scores: dict,
) -> str | None:
    """Genera una narrativa interpretativa usando Gemini 2.5 Flash.

    Returns:
        String con la narrativa o None si no hay API key o falla la llamada.
    """
    if GEMINI_API_KEY is None:
        logger.info("GEMINI_API_KEY no configurada, omitiendo narrativa")
        return None

    system_instruction, user_prompt = _construir_prompt(
        wallet, features, patrones, estilo, descripcion, scores
    )

    try:
        from google import genai

        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=user_prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.3,
                max_output_tokens=1500,
            ),
        )
        narrativa = response.text
        logger.info("Narrativa generada para %s: %d chars", wallet, len(narrativa))
        return narrativa

    except Exception as e:
        logger.error("Error generando narrativa con Gemini: %s", e)
        return None


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    # Test rápido de construcción de prompt
    system, user = _construir_prompt(
        wallet="0xTEST",
        features={"n_trades": 100, "win_rate": 65.0, "importe_medio": 50.0},
        patrones={"acumulacion": {"accumulation_type": "flat_accumulator"}},
        estilo="hold-to-resolution",
        descripcion="Mantiene posiciones hasta resolución.",
        scores={"hold-to-resolution": 0.85, "value / mixed": 0.3},
    )
    print("=== SYSTEM ===")
    print(system[:200], "...")
    print("\n=== USER ===")
    print(user)
