"""Tests unitarios para el módulo de análisis narrativo con Gemini."""

import unittest
from unittest.mock import patch, MagicMock

from src.llm_analyzer import _construir_prompt, generar_narrativa


class TestConstruirPrompt(unittest.TestCase):

    def setUp(self):
        self.wallet = "0xTEST123"
        self.features = {
            "n_trades": 100,
            "n_mercados": 30,
            "win_rate": 65.0,
            "importe_medio": 50.0,
            "price_distribucion": {"0-20": 10, "20-40": 20, "40-60": 30, "60-80": 25, "80-100": 15},
        }
        self.patrones = {
            "acumulacion": {"accumulation_type": "flat_accumulator", "n_reentries_analyzed": 45},
            "sesiones": {"n_sessions": 12, "median_trades_per_session": 8.0},
        }
        self.estilo = "hold-to-resolution"
        self.descripcion = "Mantiene posiciones hasta resolución."
        self.scores = {"hold-to-resolution": 0.85, "value / mixed": 0.3}

    def test_devuelve_tupla_str_str(self):
        system, user = _construir_prompt(
            self.wallet, self.features, self.patrones,
            self.estilo, self.descripcion, self.scores,
        )
        self.assertIsInstance(system, str)
        self.assertIsInstance(user, str)

    def test_system_contiene_instrucciones(self):
        system, _ = _construir_prompt(
            self.wallet, self.features, self.patrones,
            self.estilo, self.descripcion, self.scores,
        )
        self.assertIn("analista cuantitativo", system)
        self.assertIn("español", system)

    def test_user_contiene_wallet(self):
        _, user = _construir_prompt(
            self.wallet, self.features, self.patrones,
            self.estilo, self.descripcion, self.scores,
        )
        self.assertIn("0xTEST123", user)

    def test_user_contiene_features(self):
        _, user = _construir_prompt(
            self.wallet, self.features, self.patrones,
            self.estilo, self.descripcion, self.scores,
        )
        self.assertIn("n_trades: 100", user)
        self.assertIn("AGGREGATE FEATURES", user)

    def test_user_contiene_patrones(self):
        _, user = _construir_prompt(
            self.wallet, self.features, self.patrones,
            self.estilo, self.descripcion, self.scores,
        )
        self.assertIn("SEQUENTIAL PATTERNS", user)
        self.assertIn("flat_accumulator", user)

    def test_user_contiene_scores(self):
        _, user = _construir_prompt(
            self.wallet, self.features, self.patrones,
            self.estilo, self.descripcion, self.scores,
        )
        self.assertIn("ARCHETYPE SCORES", user)
        self.assertIn("hold-to-resolution", user)


class TestGenerarNarrativa(unittest.TestCase):

    def test_sin_api_key_retorna_none(self):
        with patch("src.llm_analyzer.GEMINI_API_KEY", None):
            result = generar_narrativa(
                "0xTEST", {"n_trades": 10}, {}, "value / mixed", "Desc", {},
            )
            self.assertIsNone(result)

    def test_api_failure_retorna_none(self):
        mock_genai = MagicMock()
        mock_genai.Client.return_value.models.generate_content.side_effect = Exception("API error")
        mock_google = MagicMock()
        mock_google.genai = mock_genai

        with patch("src.llm_analyzer.GEMINI_API_KEY", "fake-key"), \
             patch.dict("sys.modules", {"google": mock_google, "google.genai": mock_genai}):
            result = generar_narrativa(
                "0xTEST", {"n_trades": 10}, {}, "value / mixed", "Desc", {},
            )
            self.assertIsNone(result)

    def test_api_success_retorna_string(self):
        mock_response = MagicMock()
        mock_response.text = "Esta wallet muestra un patrón de acumulación..."

        mock_genai = MagicMock()
        mock_genai.Client.return_value.models.generate_content.return_value = mock_response
        mock_google = MagicMock()
        mock_google.genai = mock_genai

        with patch("src.llm_analyzer.GEMINI_API_KEY", "fake-key"), \
             patch.dict("sys.modules", {"google": mock_google, "google.genai": mock_genai}):
            result = generar_narrativa(
                "0xTEST", {"n_trades": 10}, {}, "value / mixed", "Desc", {},
            )
            self.assertEqual(result, "Esta wallet muestra un patrón de acumulación...")


if __name__ == "__main__":
    unittest.main()
