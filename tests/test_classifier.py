"""Tests unitarios para el módulo de clasificación de arquetipos."""

import unittest

from src.classifier import clasificar


def _base_features(**overrides) -> dict:
    """Genera un dict de features con valores por defecto, sobreescribibles."""
    defaults = {
        "antelacion_media_dias": 5.0,
        "pct_entradas_ultima_semana_mercado": 0.4,
        "hold_rate": 0.80,
        "price_entrada_media": 0.50,
        "price_std": 0.15,
        "corr_size_price": 0.0,
        "price_distribucion": {
            "0-20": 20, "20-40": 20, "40-60": 20, "60-80": 20, "80-100": 20,
        },
        "importe_medio": 200,
        "coef_variacion": 0.5,
        "hora_entrada_moda": 14,
        "n_trades": 100,
        "n_mercados": 30,
        "win_rate": None,
        "roi_estimado": None,
        "categoria_top": "unknown",
        "pct_categoria_top": 1.0,
    }
    defaults.update(overrides)
    return defaults


class TestClasificador(unittest.TestCase):

    def test_end_of_day(self):
        features = _base_features(
            antelacion_media_dias=0.5,
            pct_entradas_ultima_semana_mercado=0.98,
            hold_rate=0.94,
            price_entrada_media=0.72,
            price_std=0.15,
            corr_size_price=-0.1,
            price_distribucion={
                "0-20": 5, "20-40": 5, "40-60": 10, "60-80": 20, "80-100": 60,
            },
            importe_medio=200,
            coef_variacion=0.3,
            hora_entrada_moda=16,
            n_trades=300,
            n_mercados=80,
        )
        estilo, desc, scores = clasificar(features)
        self.assertEqual(estilo, "end-of-day")

    def test_contrarian(self):
        features = _base_features(
            antelacion_media_dias=22.0,
            pct_entradas_ultima_semana_mercado=0.1,
            hold_rate=0.88,
            price_entrada_media=0.12,
            price_std=0.08,
            corr_size_price=0.1,
            price_distribucion={
                "0-20": 80, "20-40": 15, "40-60": 3, "60-80": 1, "80-100": 1,
            },
            importe_medio=500,
            coef_variacion=1.2,
            hora_entrada_moda=10,
            n_trades=90,
            n_mercados=40,
        )
        estilo, desc, scores = clasificar(features)
        self.assertEqual(estilo, "contrarian")

    def test_hold_to_resolution(self):
        features = _base_features(
            antelacion_media_dias=8.0,
            pct_entradas_ultima_semana_mercado=0.3,
            hold_rate=0.97,
            price_entrada_media=0.50,
            price_std=0.20,
            corr_size_price=0.05,
            price_distribucion={
                "0-20": 15, "20-40": 20, "40-60": 40, "60-80": 20, "80-100": 5,
            },
            importe_medio=300,
            coef_variacion=0.5,
            hora_entrada_moda=14,
            n_trades=200,
            n_mercados=60,
        )
        estilo, desc, scores = clasificar(features)
        self.assertEqual(estilo, "hold-to-resolution")

    def test_scores_son_dict_completo(self):
        features = _base_features()
        _, _, scores = clasificar(features)
        self.assertIsInstance(scores, dict)
        self.assertEqual(len(scores), 7)
        for v in scores.values():
            self.assertGreaterEqual(v, 0.0)
            self.assertLessEqual(v, 1.0)

    def test_none_features_no_crashea(self):
        features = {
            "antelacion_media_dias": None,
            "pct_entradas_ultima_semana_mercado": None,
            "hold_rate": None,
            "price_entrada_media": None,
            "price_std": None,
            "corr_size_price": None,
            "price_distribucion": None,
            "importe_medio": None,
            "coef_variacion": None,
            "hora_entrada_moda": None,
            "n_trades": None,
            "n_mercados": None,
            "win_rate": None,
            "roi_estimado": None,
            "categoria_top": None,
            "pct_categoria_top": None,
        }
        estilo, desc, scores = clasificar(features)
        self.assertEqual(estilo, "value / mixed")

    def test_devuelve_tuple_correcta(self):
        features = _base_features()
        resultado = clasificar(features)
        self.assertIsInstance(resultado, tuple)
        self.assertEqual(len(resultado), 3)
        estilo, desc, scores = resultado
        self.assertIsInstance(estilo, str)
        self.assertIsInstance(desc, str)
        self.assertIsInstance(scores, dict)


if __name__ == "__main__":
    unittest.main()
