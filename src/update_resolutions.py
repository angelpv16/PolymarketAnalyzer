"""Script para actualizar resoluciones de mercados desde la Gamma API.

Consulta mercados con end_date pasada que no están resueltos,
obtiene su estado de resolución de Gamma y actualiza la DB.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from src import fetcher
from src import storage

logger = logging.getLogger(__name__)


def backfill_slugs() -> int:
    """Rellena slugs faltantes consultando trades recientes desde la Data API.

    Devuelve nº de markets actualizados.
    """
    con = storage.get_connection()
    try:
        # Obtener wallets y markets sin slug
        sin_slug = con.execute(
            "SELECT COUNT(*) FROM markets WHERE slug IS NULL"
        ).fetchone()[0]

        if sin_slug == 0:
            logger.info("Todos los markets ya tienen slug")
            return 0

        logger.info("Markets sin slug: %d — obteniendo slugs desde Data API", sin_slug)

        # Obtener wallets para re-fetch de trades con slugs
        wallets = [row[0] for row in con.execute(
            "SELECT DISTINCT wallet FROM trades"
        ).fetchall()]
    finally:
        con.close()

    slug_map: dict[str, str] = {}
    for wallet in wallets:
        trades = fetcher.obtener_trades(wallet)
        for t in trades:
            cid = t.get("market_id", "")
            slug = t.get("_slug", "")
            if cid and slug:
                slug_map[cid] = slug

    if slug_map:
        storage.poblar_slugs_desde_trades(slug_map)

    return len(slug_map)


def actualizar_resoluciones() -> dict:
    """Flujo principal: obtiene markets sin resolver, consulta Gamma, actualiza DB.

    Devuelve dict con estadísticas del proceso.
    """
    # Asegurar schema actualizado
    storage.init_db()

    # Backfill slugs si faltan
    backfill_slugs()

    # Obtener markets sin resolver
    pendientes = storage.obtener_markets_sin_resolver()
    total_consultados = len(pendientes)

    if not pendientes:
        logger.info("No hay markets pendientes de resolución")
        return {"consultados": 0, "resueltos": 0, "actualizados": 0}

    # Consultar Gamma API en batches de 20
    resueltos_total: list[dict] = []
    batch_size = 20
    for i in range(0, len(pendientes), batch_size):
        batch = pendientes[i:i + batch_size]
        resueltos = fetcher.fetch_resoluciones(batch)
        resueltos_total.extend(resueltos)

    # Actualizar DB
    actualizados = storage.actualizar_resoluciones(resueltos_total)

    stats = {
        "consultados": total_consultados,
        "resueltos": len(resueltos_total),
        "actualizados": actualizados,
    }
    logger.info(
        "Resoluciones: %d consultados, %d resueltos, %d actualizados en DB",
        stats["consultados"], stats["resueltos"], stats["actualizados"],
    )
    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    )

    print("=== Actualización de resoluciones de mercados ===\n")
    stats = actualizar_resoluciones()
    print(f"\nResumen:")
    print(f"  Markets consultados:    {stats['consultados']}")
    print(f"  Markets resueltos:      {stats['resueltos']}")
    print(f"  Actualizados en DB:     {stats['actualizados']}")
