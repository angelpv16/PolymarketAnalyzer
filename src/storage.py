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
                id               VARCHAR PRIMARY KEY,
                question         VARCHAR,
                category         VARCHAR,
                end_date         TIMESTAMP,
                slug             VARCHAR,
                resolved         BOOLEAN DEFAULT FALSE,
                winning_outcome  VARCHAR
            )
        """)
        # Migrar tabla markets existente si le faltan columnas
        _migrar_markets(con)
        logger.info("Tablas trades y markets listas")
    finally:
        con.close()


def _migrar_markets(con: duckdb.DuckDBPyConnection) -> None:
    """Añade columnas de resolución a markets si no existen."""
    columnas = {row[0] for row in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'markets'").fetchall()}
    if "slug" not in columnas:
        con.execute("ALTER TABLE markets ADD COLUMN slug VARCHAR")
        logger.info("Columna 'slug' añadida a markets")
    if "resolved" not in columnas:
        con.execute("ALTER TABLE markets ADD COLUMN resolved BOOLEAN DEFAULT FALSE")
        logger.info("Columna 'resolved' añadida a markets")
    if "winning_outcome" not in columnas:
        con.execute("ALTER TABLE markets ADD COLUMN winning_outcome VARCHAR")
        logger.info("Columna 'winning_outcome' añadida a markets")


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
                INSERT OR IGNORE INTO markets (id, question, category, end_date, slug)
                VALUES (?, ?, ?, ?, ?)
                """,
                [m["id"], m["question"], m["category"], m["end_date"], m.get("slug")],
            )
        despues = con.execute("SELECT count(*) FROM markets").fetchone()[0]
        insertados = despues - antes
        logger.info("Markets guardados: %d nuevos de %d recibidos", insertados, len(markets))
        return insertados
    finally:
        con.close()


# ── Resoluciones ─────────────────────────────────────────────────────────────


def poblar_slugs_desde_trades(slug_map: dict[str, str]) -> int:
    """Actualiza el slug de markets que no lo tienen, usando un mapa conditionId -> slug.

    Devuelve nº de markets actualizados.
    """
    if not slug_map:
        return 0
    con = get_connection()
    try:
        actualizados = 0
        for condition_id, slug in slug_map.items():
            result = con.execute(
                "UPDATE markets SET slug = ? WHERE id = ? AND (slug IS NULL OR slug = '')",
                [slug, condition_id],
            )
            actualizados += result.fetchone()[0] if hasattr(result, 'fetchone') else 0
        # Count how many now have slugs
        con.execute("SELECT 1")  # force flush
        total_con_slug = con.execute("SELECT COUNT(*) FROM markets WHERE slug IS NOT NULL").fetchone()[0]
        logger.info("Slugs poblados: %d markets ahora tienen slug", total_con_slug)
        return total_con_slug
    finally:
        con.close()


def obtener_markets_sin_resolver() -> list[dict]:
    """Devuelve markets con end_date pasada que no están resueltos.

    Retorna lista de dicts con id y slug.
    """
    con = get_connection()
    try:
        rows = con.execute("""
            SELECT id, slug FROM markets
            WHERE end_date < CURRENT_TIMESTAMP
            AND (resolved = FALSE OR resolved IS NULL)
            AND slug IS NOT NULL
        """).fetchall()
        result = [{"id": r[0], "slug": r[1]} for r in rows]
        logger.info("Markets sin resolver con end_date pasada: %d", len(result))
        return result
    finally:
        con.close()


def actualizar_resoluciones(markets_resueltos: list[dict]) -> int:
    """Actualiza markets resueltos en la DB. Devuelve nº actualizados.

    Cada dict debe tener: id, winning_outcome.
    """
    if not markets_resueltos:
        return 0
    con = get_connection()
    try:
        actualizados = 0
        for m in markets_resueltos:
            con.execute(
                """
                UPDATE markets
                SET resolved = TRUE, winning_outcome = ?
                WHERE id = ?
                """,
                [m["winning_outcome"], m["id"]],
            )
            actualizados += 1
        logger.info("Markets actualizados con resolución: %d", actualizados)
        return actualizados
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
