"""Tests unitarios para el módulo de extracción de reglas operacionales."""

import json
import re
import unittest
from unittest.mock import patch, MagicMock

from src.rules_extractor import (
    CATEGORIAS,
    _reglas_fallback,
    _extraer_reglas_gemini,
    extraer_reglas,
    exportar_reglas_txt,
)


# ── Fixtures ────────────────────────────────────────────────────────────────

FEATURES_REALES = {
    "antelacion_media_dias": 0.38,
    "hora_entrada_moda": 16,
    "pct_entradas_ultima_semana_mercado": 100.0,
    "importe_medio": 33.85,
    "coef_variacion": 4.2449,
    "corr_size_price": -0.0552,
    "price_entrada_media": 0.2595,
    "price_std": 0.246,
    "categoria_top": "unknown",
    "pct_categoria_top": 100.0,
    "price_distribucion": {
        "0-20": 52.31, "20-40": 1.93, "40-60": 45.13,
        "60-80": 0.35, "80-100": 0.28,
    },
    "win_rate": 24.08,
    "roi_estimado": -1035.55,
    "hold_rate": 93.3,
    "n_trades": 1490,
    "n_mercados": 358,
}

PATRONES_REALES = {
    "acumulacion": {
        "accumulation_type": "flat_accumulator",
        "pct_flat_reentries": 96.61,
        "pct_dip_reentries": 1.56,
        "pct_momentum_reentries": 1.83,
        "avg_price_delta_cents": -0.0093,
        "n_reentries_analyzed": 1091,
    },
    "size_scaling": {
        "size_scaling_type": "escalating",
        "avg_size_ratio_2nd_to_1st": 2.6026,
        "avg_size_ratio_3rd_to_1st": 2.7555,
        "n_markets_multi_entry": 219,
    },
    "sesiones": {
        "n_sessions": 42,
        "median_trades_per_session": 5.5,
        "avg_session_duration_min": 67.9,
        "avg_markets_per_session": 9.2,
        "max_trades_single_session": 443,
    },
    "salidas": {
        "pct_markets_with_exit": 6.7,
        "n_markets_with_exit": 24,
        "exit_timing": "no_exit",
        "avg_exit_days_before_end": 0.1,
    },
    "ciclo_mercado": {
        "avg_entry_pct_lifecycle": 7.84,
        "pct_early_entries": 87.79,
        "pct_late_entries": 0.21,
        "adds_near_deadline": 1417,
    },
    "concentracion": {
        "gini_coefficient": 0.6059,
        "top_market_pct": 10.19,
        "top_3_markets_pct": 23.88,
        "n_markets_for_80pct": 147,
    },
}

ESTILO = "end-of-day"
DESCRIPCION = "Opera en los últimos 0.4 días antes del cierre."


# ── Tests ───────────────────────────────────────────────────────────────────


class TestFallbackSinApiKey(unittest.TestCase):
    """Test que sin API key se generan reglas calculadas directamente."""

    def test_sin_api_key_devuelve_reglas(self):
        with patch("src.rules_extractor.GEMINI_API_KEY", None):
            reglas = extraer_reglas(
                "0xTEST", FEATURES_REALES, PATRONES_REALES,
                ESTILO, DESCRIPCION,
            )
        self.assertIsInstance(reglas, dict)
        for cat in CATEGORIAS:
            self.assertIn(cat, reglas)
            self.assertGreaterEqual(len(reglas[cat]), 3)

    def test_gemini_extractor_retorna_none_sin_key(self):
        with patch("src.rules_extractor.GEMINI_API_KEY", None):
            result = _extraer_reglas_gemini(
                "0xTEST", FEATURES_REALES, PATRONES_REALES,
                ESTILO, DESCRIPCION,
            )
        self.assertIsNone(result)


class TestEstructura(unittest.TestCase):
    """Test que el output tiene las 4 categorías esperadas."""

    def test_fallback_tiene_4_categorias(self):
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        self.assertEqual(set(reglas.keys()), set(CATEGORIAS))

    def test_cada_categoria_tiene_minimo_3_reglas(self):
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        for cat in CATEGORIAS:
            self.assertGreaterEqual(
                len(reglas[cat]), 3,
                f"Categoría '{cat}' tiene solo {len(reglas[cat])} reglas",
            )

    def test_cada_categoria_tiene_maximo_5_reglas(self):
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        for cat in CATEGORIAS:
            self.assertLessEqual(
                len(reglas[cat]), 5,
                f"Categoría '{cat}' tiene {len(reglas[cat])} reglas (max 5)",
            )

    def test_todas_las_reglas_son_strings(self):
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        for cat, lista in reglas.items():
            for regla in lista:
                self.assertIsInstance(regla, str, f"Regla en '{cat}' no es string")


class TestUmbralesReales(unittest.TestCase):
    """Test que las reglas contienen valores numéricos reales de la wallet."""

    def setUp(self):
        self.reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)

    def test_reglas_contienen_numeros(self):
        """Al menos la mitad de las reglas deben tener un número real."""
        total = 0
        con_numero = 0
        for cat, lista in self.reglas.items():
            for regla in lista:
                total += 1
                if re.search(r"\d+", regla):
                    con_numero += 1
        self.assertGreater(
            con_numero, total / 2,
            f"Solo {con_numero}/{total} reglas contienen números",
        )

    def test_precio_entrada_en_reglas(self):
        """El precio de entrada (~26¢) debe aparecer en alguna regla."""
        todas = " ".join(
            r for lista in self.reglas.values() for r in lista
        )
        self.assertTrue(
            re.search(r"2[56]", todas),
            "El precio de entrada (~26¢) no aparece en ninguna regla",
        )

    def test_importe_medio_en_reglas(self):
        """El importe medio (~$34) debe aparecer en alguna regla."""
        todas = " ".join(
            r for lista in self.reglas.values() for r in lista
        )
        self.assertTrue(
            re.search(r"3[34]", todas),
            "El importe medio (~$34) no aparece en ninguna regla",
        )

    def test_hold_rate_en_reglas(self):
        """El hold rate (93%) debe aparecer en alguna regla."""
        todas = " ".join(
            r for lista in self.reglas.values() for r in lista
        )
        self.assertTrue(
            re.search(r"93", todas),
            "El hold rate (93%) no aparece en ninguna regla",
        )


class TestExportar(unittest.TestCase):
    """Test de la función de exportación a texto plano."""

    def test_exportar_contiene_categorias(self):
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        txt = exportar_reglas_txt(reglas, ESTILO, "0xc867f7b28a7cbe179e098dd07077f01f84e38b00")
        for cat in CATEGORIAS:
            self.assertIn(cat.upper(), txt)

    def test_exportar_contiene_checkboxes(self):
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        txt = exportar_reglas_txt(reglas, ESTILO, "0xTEST1234567890")
        self.assertIn("[ ]", txt)

    def test_exportar_contiene_wallet(self):
        wallet = "0xc867f7b28a7cbe179e098dd07077f01f84e38b00"
        reglas = _reglas_fallback(FEATURES_REALES, PATRONES_REALES, ESTILO)
        txt = exportar_reglas_txt(reglas, ESTILO, wallet)
        self.assertIn(wallet[:10], txt)


class TestGeminiIntegration(unittest.TestCase):
    """Test de integración con Gemini (mockeado)."""

    def test_gemini_success_retorna_reglas(self):
        reglas_json = json.dumps({
            "Selección de mercado": ["Regla 1", "Regla 2", "Regla 3"],
            "Entrada": ["Regla 1", "Regla 2", "Regla 3"],
            "Gestión de posición": ["Regla 1", "Regla 2", "Regla 3"],
            "Salida": ["Regla 1", "Regla 2", "Regla 3"],
        })
        mock_response = MagicMock()
        mock_response.text = reglas_json

        mock_genai = MagicMock()
        mock_genai.Client.return_value.models.generate_content.return_value = mock_response
        mock_google = MagicMock()
        mock_google.genai = mock_genai

        with patch("src.rules_extractor.GEMINI_API_KEY", "fake-key"), \
             patch.dict("sys.modules", {"google": mock_google, "google.genai": mock_genai}):
            result = _extraer_reglas_gemini(
                "0xTEST", FEATURES_REALES, PATRONES_REALES,
                ESTILO, DESCRIPCION,
            )
        self.assertIsNotNone(result)
        for cat in CATEGORIAS:
            self.assertIn(cat, result)

    def test_gemini_failure_retorna_none(self):
        mock_genai = MagicMock()
        mock_genai.Client.return_value.models.generate_content.side_effect = Exception("API error")
        mock_google = MagicMock()
        mock_google.genai = mock_genai

        with patch("src.rules_extractor.GEMINI_API_KEY", "fake-key"), \
             patch.dict("sys.modules", {"google": mock_google, "google.genai": mock_genai}):
            result = _extraer_reglas_gemini(
                "0xTEST", FEATURES_REALES, PATRONES_REALES,
                ESTILO, DESCRIPCION,
            )
        self.assertIsNone(result)

    def test_gemini_json_invalido_retorna_none(self):
        mock_response = MagicMock()
        mock_response.text = "esto no es json"

        mock_genai = MagicMock()
        mock_genai.Client.return_value.models.generate_content.return_value = mock_response
        mock_google = MagicMock()
        mock_google.genai = mock_genai

        with patch("src.rules_extractor.GEMINI_API_KEY", "fake-key"), \
             patch.dict("sys.modules", {"google": mock_google, "google.genai": mock_genai}):
            result = _extraer_reglas_gemini(
                "0xTEST", FEATURES_REALES, PATRONES_REALES,
                ESTILO, DESCRIPCION,
            )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
