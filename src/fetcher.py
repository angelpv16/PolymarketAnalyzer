"""Módulo de fetch de datos desde Polymarket Data API y Gamma API.

Obtiene historial de trades de una wallet y enriquece con info de mercados.
"""

import logging
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

# URLs base
DATA_API_URL = "https://data-api.polymarket.com/activity"
GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"

# Caché en memoria para market info (slug -> dict)
_market_cache: dict[str, dict | None] = {}

PAGE_LIMIT = 500
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # segundos


# ── Trades ───────────────────────────────────────────────────────────────────


def obtener_trades(wallet: str) -> list[dict]:
    """Obtiene todos los trades de una wallet desde la Data API con paginación offset.

    Reintenta hasta MAX_RETRIES veces por request con backoff.
    Si un request falla definitivamente, devuelve lo acumulado hasta ese momento.
    """
    trades: list[dict] = []
    offset = 0

    while True:
        params = {"user": wallet, "limit": PAGE_LIMIT, "offset": offset}
        data = _request_con_reintentos(DATA_API_URL, params)

        if data is None:
            logger.error("Fallo definitivo en offset %d — devolviendo %d trades parciales", offset, len(trades))
            break

        for item in data:
            if item.get("type") != "TRADE":
                continue
            trades.append(_mapear_trade(item))

        if len(data) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT

    logger.info("Wallet %s... → %d trades obtenidos", wallet[:10], len(trades))
    return trades


def _mapear_trade(item: dict) -> dict:
    """Convierte un item de la Data API al schema de storage."""
    ts = item.get("timestamp", 0)
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "id": item["transactionHash"],
        "timestamp": ts_iso,
        "market_id": item.get("conditionId", ""),
        "asset_id": item.get("asset", ""),
        "side": item.get("side", ""),
        "price": float(item.get("price", 0)),
        "size": float(item.get("size", 0)),
        "outcome": item.get("outcome", ""),
        # Campos extra útiles para enriquecimiento
        "_slug": item.get("slug", ""),
        "_title": item.get("title", ""),
        "_event_slug": item.get("eventSlug", ""),
    }


# ── Markets ──────────────────────────────────────────────────────────────────


def obtener_market_info(market_slug: str) -> dict | None:
    """Obtiene info de un mercado desde la Gamma API por slug.

    Cachea resultados en memoria para evitar llamadas repetidas.
    """
    if market_slug in _market_cache:
        return _market_cache[market_slug]

    try:
        resp = requests.get(GAMMA_API_URL, params={"slug": market_slug}, timeout=10)
        resp.raise_for_status()
        results = resp.json()

        if not results:
            logger.warning("Market no encontrado en Gamma: %s", market_slug)
            _market_cache[market_slug] = None
            return None

        m = results[0]
        market = {
            "id": m.get("conditionId", ""),
            "question": m.get("question", ""),
            "category": m.get("category", ""),
            "end_date": m.get("endDate", ""),
        }
        _market_cache[market_slug] = market
        return market

    except (requests.RequestException, ValueError, KeyError) as e:
        logger.warning("Error obteniendo market %s: %s", market_slug, e)
        _market_cache[market_slug] = None
        return None


# ── Enriquecimiento ──────────────────────────────────────────────────────────


def enriquecer_trades_con_mercados(trades: list[dict]) -> tuple[list[dict], list[dict]]:
    """Extrae mercados únicos de los trades y obtiene su info de Gamma.

    Devuelve (trades_originales, lista_de_markets).
    """
    # Mapear conditionId -> slug para lookups
    slug_por_condition: dict[str, str] = {}
    for t in trades:
        cid = t.get("market_id", "")
        slug = t.get("_slug", "")
        if cid and slug:
            slug_por_condition[cid] = slug

    markets: list[dict] = []
    for condition_id, slug in slug_por_condition.items():
        info = obtener_market_info(slug)
        if info is not None:
            markets.append(info)

    logger.info("Mercados distintos encontrados: %d de %d condition_ids", len(markets), len(slug_por_condition))
    return trades, markets


# ── Orquestación ─────────────────────────────────────────────────────────────


def fetch_completo(wallet: str) -> tuple[list[dict], list[dict]]:
    """Orquesta la obtención de trades y enriquecimiento con mercados.

    Devuelve (trades, markets).
    """
    trades = obtener_trades(wallet)
    trades, markets = enriquecer_trades_con_mercados(trades)

    logger.info("Resumen — trades totales: %d, mercados únicos: %d", len(trades), len(markets))
    return trades, markets


# ── Utilidad HTTP ────────────────────────────────────────────────────────────


def _request_con_reintentos(url: str, params: dict) -> list | None:
    """GET con reintentos y backoff. Devuelve la lista JSON o None si falla."""
    for intento in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            logger.warning("Intento %d/%d falló para %s: %s", intento, MAX_RETRIES, url, e)
            if intento < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF)
    return None


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

    from src.storage import guardar_markets, guardar_trades, init_db

    init_db()

    # Proxy wallet con actividad real confirmada
    wallet = "0xc867f7b28a7cbe179e098dd07077f01f84e38b00"

    trades, markets = fetch_completo(wallet)

    # Limpiar campos internos antes de guardar
    trades_para_db = [{k: v for k, v in t.items() if not k.startswith("_")} for t in trades]

    n_trades = guardar_trades(wallet, trades_para_db)
    n_markets = guardar_markets(markets)

    print(f"\nTrades obtenidos:    {len(trades)}")
    print(f"Mercados únicos:     {len(markets)}")
    print(f"Trades guardados:    {n_trades}")
    print(f"Markets guardados:   {n_markets}")
    if trades:
        print(f"\nPrimer trade: {trades_para_db[0]}")
        print(f"Último trade:  {trades_para_db[-1]}")
