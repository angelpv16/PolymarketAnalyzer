"""Tests unitarios para el módulo de análisis de comportamiento."""

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import duckdb

from src.analyzer import calcular_features


class _NoCloseCon:
    """Wrapper que delega todo a la conexion real pero ignora close()."""

    def __init__(self, real_con):
        self._con = real_con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def close(self):
        pass


class TestAnalyzer(unittest.TestCase):

    def setUp(self):
        self._real_con = duckdb.connect(":memory:")
        self.con = _NoCloseCon(self._real_con)

        # Crear tablas con el mismo schema que storage.py
        self._real_con.execute("""
            CREATE TABLE trades (
                id        VARCHAR PRIMARY KEY,
                wallet    VARCHAR,
                timestamp TIMESTAMP,
                market_id VARCHAR,
                asset_id  VARCHAR,
                side      VARCHAR,
                price     DOUBLE,
                size      DOUBLE,
                outcome   VARCHAR
            )
        """)
        self._real_con.execute("""
            CREATE TABLE markets (
                id       VARCHAR PRIMARY KEY,
                question VARCHAR,
                category VARCHAR,
                end_date TIMESTAMP
            )
        """)

        # Insertar 10 trades ficticios para 0xTEST
        base = datetime(2025, 6, 1, 12, 0, 0)
        trades = [
            ("t01", "0xTEST", base - timedelta(days=25, hours=2), "mkt_001", "a_1", "BUY",  0.35, 150.0, "Yes"),
            ("t02", "0xTEST", base - timedelta(days=22, hours=5), "mkt_001", "a_1", "BUY",  0.40, 200.0, "Yes"),
            ("t03", "0xTEST", base - timedelta(days=20, hours=8), "mkt_002", "a_2", "BUY",  0.55, 300.0, "No"),
            ("t04", "0xTEST", base - timedelta(days=18, hours=3), "mkt_001", "a_1", "SELL", 0.60, 100.0, "Yes"),
            ("t05", "0xTEST", base - timedelta(days=15, hours=6), "mkt_002", "a_2", "BUY",  0.50, 250.0, "No"),
            ("t06", "0xTEST", base - timedelta(days=12, hours=1), "mkt_003", "a_3", "BUY",  0.70, 400.0, "Yes"),
            ("t07", "0xTEST", base - timedelta(days=10, hours=4), "mkt_003", "a_3", "BUY",  0.75, 500.0, "Yes"),
            ("t08", "0xTEST", base - timedelta(days=5, hours=9),  "mkt_001", "a_1", "BUY",  0.80, 350.0, "Yes"),
            ("t09", "0xTEST", base - timedelta(days=3, hours=7),  "mkt_002", "a_2", "SELL", 0.65,  10.0, "No"),
            ("t10", "0xTEST", base - timedelta(days=1, hours=2),  "mkt_003", "a_3", "BUY",  0.85, 450.0, "Yes"),
        ]
        for t in trades:
            self._real_con.execute(
                "INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(t)
            )

        # Insertar 3 markets con end_date futuro
        markets = [
            ("mkt_001", "Will BTC exceed $100k?", "Crypto",   base + timedelta(days=30)),
            ("mkt_002", "Will it rain tomorrow?", "Weather",  base + timedelta(days=45)),
            ("mkt_003", "Next president?",        "Politics", base + timedelta(days=60)),
        ]
        for m in markets:
            self._real_con.execute(
                "INSERT INTO markets VALUES (?, ?, ?, ?)", list(m)
            )

        # Parchear get_connection para devolver nuestra conexión en memoria
        self.patcher = patch(
            "src.analyzer.get_connection", return_value=self.con
        )
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self._real_con.close()

    def test_calcular_features_devuelve_dict(self):
        result = calcular_features("0xTEST")
        self.assertIsInstance(result, dict)

    def test_n_trades_correcto(self):
        features = calcular_features("0xTEST")
        self.assertEqual(features["n_trades"], 10)

    def test_n_mercados_correcto(self):
        features = calcular_features("0xTEST")
        self.assertEqual(features["n_mercados"], 3)

    def test_price_entrada_media_rango_valido(self):
        features = calcular_features("0xTEST")
        self.assertGreater(features["price_entrada_media"], 0)
        self.assertLess(features["price_entrada_media"], 1)

    def test_price_distribucion_suma_100(self):
        features = calcular_features("0xTEST")
        total = sum(features["price_distribucion"].values())
        self.assertAlmostEqual(total, 100, delta=1)

    def test_none_features_son_none_o_valor(self):
        features = calcular_features("0xTEST")
        tipos_validos = (type(None), int, float, str, dict)
        for key, val in features.items():
            self.assertIsInstance(
                val, tipos_validos,
                msg=f"Feature '{key}' tiene tipo inesperado: {type(val).__name__}",
            )


if __name__ == "__main__":
    unittest.main()
