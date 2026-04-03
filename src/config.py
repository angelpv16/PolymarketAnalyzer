"""Configuración central del proyecto PolymarketAnalyzer.

Lee variables de entorno desde .env y expone constantes de configuración.
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# --- Wallets a monitorear ---
_wallets_raw = os.getenv("WALLETS", "")
if _wallets_raw.strip():
    WALLETS: list[str] = [w.strip() for w in _wallets_raw.split(",") if w.strip()]
else:
    WALLETS = []
    logger.warning("WALLETS no definida en .env — usando lista vacía")

# --- Intervalo de actualización (horas) ---
INTERVALO_HORAS: int = int(os.getenv("INTERVALO_HORAS", "6"))

# --- Ruta de la base de datos ---
DB_PATH: str = "data/polymarket.db"


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print("=== Configuración cargada ===")
    print(f"  WALLETS:         {WALLETS}")
    print(f"  INTERVALO_HORAS: {INTERVALO_HORAS}")
    print(f"  DB_PATH:         {DB_PATH}")
