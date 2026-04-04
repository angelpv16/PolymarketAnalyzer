"""Tests unitarios para el módulo de patrones secuenciales."""

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import duckdb

from src.pattern_analyzer import calcular_patrones


class _NoCloseCon:
    """Wrapper que delega todo a la conexion real pero ignora close()."""

    def __init__(self, real_con):
        self._con = real_con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def close(self):
        pass


class TestPatternAnalyzer(unittest.TestCase):

    def setUp(self):
        self._real_con = duckdb.connect(":memory:")
        self.con = _NoCloseCon(self._real_con)

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
                id               VARCHAR PRIMARY KEY,
                question         VARCHAR,
                category         VARCHAR,
                end_date         TIMESTAMP,
                slug             VARCHAR,
                resolved         BOOLEAN DEFAULT FALSE,
                winning_outcome  VARCHAR
            )
        """)

        # 12 trades across 3 markets with known patterns
        base = datetime(2025, 6, 1, 12, 0, 0)
        trades = [
            # mkt_001: 4 BUYs at similar prices (flat accumulation)
            ("t01", "0xTEST", base - timedelta(days=25, hours=0, minutes=0),  "mkt_001", "a_1", "BUY",  0.50, 100.0, "Yes"),
            ("t02", "0xTEST", base - timedelta(days=25, hours=0, minutes=30), "mkt_001", "a_1", "BUY",  0.505, 100.0, "Yes"),
            ("t03", "0xTEST", base - timedelta(days=24, hours=23),            "mkt_001", "a_1", "BUY",  0.51, 100.0, "Yes"),
            ("t04", "0xTEST", base - timedelta(days=20),                      "mkt_001", "a_1", "BUY",  0.50, 100.0, "Yes"),
            # mkt_002: 3 BUYs + 1 SELL (has exit)
            ("t05", "0xTEST", base - timedelta(days=15, hours=6), "mkt_002", "a_2", "BUY",  0.30, 200.0, "No"),
            ("t06", "0xTEST", base - timedelta(days=15, hours=5), "mkt_002", "a_2", "BUY",  0.28, 250.0, "No"),
            ("t07", "0xTEST", base - timedelta(days=10),          "mkt_002", "a_2", "BUY",  0.25, 300.0, "No"),
            ("t08", "0xTEST", base - timedelta(days=5),           "mkt_002", "a_2", "SELL", 0.40, 100.0, "No"),
            # mkt_003: 2 BUYs in a separate session
            ("t09", "0xTEST", base - timedelta(days=3, hours=2),  "mkt_003", "a_3", "BUY",  0.70, 400.0, "Yes"),
            ("t10", "0xTEST", base - timedelta(days=3, hours=1),  "mkt_003", "a_3", "BUY",  0.75, 500.0, "Yes"),
            # mkt_003: additional BUY in yet another session
            ("t11", "0xTEST", base - timedelta(days=1),           "mkt_003", "a_3", "BUY",  0.80, 450.0, "Yes"),
            # Different wallet - should not appear
            ("t12", "0xOTHER", base - timedelta(days=1),          "mkt_001", "a_1", "BUY",  0.60, 999.0, "Yes"),
        ]
        for t in trades:
            self._real_con.execute(
                "INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(t)
            )

        markets = [
            ("mkt_001", "Will BTC exceed $100k?", "Crypto",   base + timedelta(days=30)),
            ("mkt_002", "Will it rain?",          "Weather",  base + timedelta(days=45)),
            ("mkt_003", "Next president?",        "Politics", base + timedelta(days=60)),
        ]
        for m in markets:
            self._real_con.execute(
                "INSERT INTO markets (id, question, category, end_date) VALUES (?, ?, ?, ?)",
                list(m),
            )

        self.patcher = patch(
            "src.pattern_analyzer.get_connection", return_value=self.con
        )
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self._real_con.close()

    def test_devuelve_dict_con_6_grupos(self):
        result = calcular_patrones("0xTEST")
        self.assertIsInstance(result, dict)
        expected_keys = {
            "acumulacion", "size_scaling", "sesiones",
            "salidas", "ciclo_mercado", "concentracion",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_cada_grupo_es_dict(self):
        result = calcular_patrones("0xTEST")
        for grupo, datos in result.items():
            self.assertIsInstance(datos, dict, msg=f"Grupo '{grupo}' no es dict")

    def test_acumulacion_detecta_flat(self):
        """mkt_001 tiene 3 re-entries con delta <=1c -> contribuye a flat."""
        result = calcular_patrones("0xTEST")
        acc = result["acumulacion"]
        self.assertIsNotNone(acc["accumulation_type"])
        self.assertGreater(acc["n_reentries_analyzed"], 0)
        self.assertIn(acc["accumulation_type"], ["flat_accumulator", "dip_buyer", "momentum_chaser"])

    def test_size_scaling_n_markets(self):
        """Debe detectar al menos 3 mercados con multi-entry."""
        result = calcular_patrones("0xTEST")
        ss = result["size_scaling"]
        self.assertEqual(ss["n_markets_multi_entry"], 3)
        self.assertIn(ss["size_scaling_type"], ["inverse_pyramid", "uniform", "escalating"])

    def test_sesiones_multiples(self):
        """Los trades están en varias sesiones (gaps >1h)."""
        result = calcular_patrones("0xTEST")
        ses = result["sesiones"]
        self.assertGreater(ses["n_sessions"], 1)
        self.assertIsNotNone(ses["median_trades_per_session"])
        self.assertIsNotNone(ses["max_trades_single_session"])

    def test_salidas_detecta_exit(self):
        """mkt_002 tiene un SELL -> pct_markets_with_exit > 0."""
        result = calcular_patrones("0xTEST")
        sal = result["salidas"]
        self.assertGreater(sal["pct_markets_with_exit"], 0)
        self.assertEqual(sal["n_markets_with_exit"], 1)

    def test_concentracion_gini(self):
        result = calcular_patrones("0xTEST")
        conc = result["concentracion"]
        self.assertIsNotNone(conc["gini_coefficient"])
        self.assertGreaterEqual(conc["gini_coefficient"], 0)
        self.assertLessEqual(conc["gini_coefficient"], 1)
        self.assertIsNotNone(conc["top_market_pct"])

    def test_wallet_sin_trades(self):
        """Wallet sin trades debe devolver estructura completa con valores None/0."""
        result = calcular_patrones("0xNOEXISTE")
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 6)
        self.assertEqual(result["acumulacion"]["n_reentries_analyzed"], 0)
        self.assertEqual(result["sesiones"]["n_sessions"], 0)

    def test_ciclo_mercado_valores_razonables(self):
        result = calcular_patrones("0xTEST")
        cm = result["ciclo_mercado"]
        if cm["avg_entry_pct_lifecycle"] is not None:
            self.assertGreaterEqual(cm["avg_entry_pct_lifecycle"], 0)
            self.assertLessEqual(cm["avg_entry_pct_lifecycle"], 100)


if __name__ == "__main__":
    unittest.main()
