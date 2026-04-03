"""Capa de almacenamiento DuckDB para PolymarketAnalyzer.

Gestiona conexión, esquema y operaciones CRUD sobre trades y markets.
"""

import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

from src.config import DB_PATH

logger = logging.getLogger(__name__)


# ── Conexión ──────────────────────────────────────────────────────────────────

def get_connection() -> duckdb.DuckDBPyConnection:
    """Abre (o crea) la base de datos DuckDB en DB_PATH."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Conectando a %s", DB_PATH)
    return duckdb.connect(DB_PATH)


# ── Esquema ───────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Crea las tablas trades y markets si no existen."""
    con = get_connection()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS trades (
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
        con.execute("""
            CREATE TABLE IF NOT EXISTS markets (
                id       VARCHAR PRIMARY KEY,
                question VARCHAR,
                category VARCHAR,
                end_date TIMESTAMP
            )
        """)
        logger.info("Tablas trades y markets listas")
    finally:
        con.close()


# ── Escritura ─────────────────────────────────────────────────────────────────

def guardar_trades(wallet: str, trades: list[dict]) -> int:
    """Inserta trades nuevos ignorando duplicados por id. Devuelve nº insertados."""
    if not trades:
        return 0
    con = get_connection()
    try:
        antes = con.execute("SELECT count(*) FROM trades").fetchone()[0]
        for t in trades:
            logger.debug("INSERT trade %s", t.get("id"))
            con.execute(
                """
                INSERT OR IGNORE INTO trades
                    (id, wallet, timestamp, market_id, asset_id, side, price, size, outcome)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    t["id"], wallet, t["timestamp"], t["market_id"],
                    t["asset_id"], t["side"], t["price"], t["size"], t["outcome"],
                ],
            )
        despues = con.execute("SELECT count(*) FROM trades").fetchone()[0]
        insertados = despues - antes
        logger.info("Trades guardados para %s: %d nuevos de %d recibidos", wallet, insertados, len(trades))
        return insertados
    finally:
        con.close()


def guardar_markets(markets: list[dict]) -> int:
    """Inserta markets nuevos ignorando duplicados por id. Devuelve nº insertados."""
    if not markets:
        return 0
    con = get_connection()
    try:
        antes = con.execute("SELECT count(*) FROM markets").fetchone()[0]
        for m in markets:
            logger.debug("INSERT market %s", m.get("id"))
            con.execute(
                """
                INSERT OR IGNORE INTO markets (id, question, category, end_date)
                VALUES (?, ?, ?, ?)
                """,
                [m["id"], m["question"], m["category"], m["end_date"]],
            )
        despues = con.execute("SELECT count(*) FROM markets").fetchone()[0]
        insertados = despues - antes
        logger.info("Markets guardados: %d nuevos de %d recibidos", insertados, len(markets))
        return insertados
    finally:
        con.close()


# ── Lectura ───────────────────────────────────────────────────────────────────

def obtener_trades_wallet(wallet: str) -> pd.DataFrame:
    """Devuelve todos los trades de una wallet como DataFrame."""
    con = get_connection()
    try:
        logger.debug("SELECT trades WHERE wallet = %s", wallet)
        df = con.execute(
            "SELECT * FROM trades WHERE wallet = ? ORDER BY timestamp",
            [wallet],
        ).fetchdf()
        logger.info("Trades obtenidos para %s: %d filas", wallet, len(df))
        return df
    finally:
        con.close()


def obtener_stats_wallet(wallet: str) -> dict:
    """Devuelve stats básicas: n_trades, n_mercados, primera_actividad, ultima_actividad."""
    con = get_connection()
    try:
        logger.debug("Stats query para wallet %s", wallet)
        row = con.execute(
            """
            SELECT
                count(*)                       AS n_trades,
                count(DISTINCT market_id)       AS n_mercados,
                min(timestamp)                 AS primera_actividad,
                max(timestamp)                 AS ultima_actividad
            FROM trades
            WHERE wallet = ?
            """,
            [wallet],
        ).fetchone()
        stats = {
            "n_trades": row[0],
            "n_mercados": row[1],
            "primera_actividad": str(row[2]),
            "ultima_actividad": str(row[3]),
        }
        logger.info("Stats wallet %s: %s", wallet, stats)
        return stats
    finally:
        con.close()


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s | %(name)s | %(message)s")

    print(">>> Inicializando DB...")
    init_db()

    test_wallet = "0xABC123TEST"

    test_trades = [
        {
            "id": "trade-001",
            "timestamp": "2025-01-15 10:30:00",
            "market_id": "market-aaa",
            "asset_id": "asset-111",
            "side": "BUY",
            "price": 0.65,
            "size": 100.0,
            "outcome": "Yes",
        },
        {
            "id": "trade-002",
            "timestamp": "2025-01-16 14:00:00",
            "market_id": "market-bbb",
            "asset_id": "asset-222",
            "side": "SELL",
            "price": 0.40,
            "size": 50.0,
            "outcome": "No",
        },
    ]

    test_market = [
        {
            "id": "market-aaa",
            "question": "Will BTC exceed $100k by March 2025?",
            "category": "Crypto",
            "end_date": "2025-03-31 23:59:59",
        },
    ]

    print(f"\n>>> Insertando {len(test_trades)} trades...")
    n = guardar_trades(test_wallet, test_trades)
    print(f"    Insertados: {n}")

    print(f"\n>>> Insertando {len(test_market)} market...")
    n = guardar_markets(test_market)
    print(f"    Insertados: {n}")

    print(f"\n>>> Trades de {test_wallet}:")
    df = obtener_trades_wallet(test_wallet)
    print(df.to_string(index=False))

    print(f"\n>>> Stats de {test_wallet}:")
    stats = obtener_stats_wallet(test_wallet)
    for k, v in stats.items():
        print(f"    {k}: {v}")

    print("\nDB inicializada y tests de inserción OK")
