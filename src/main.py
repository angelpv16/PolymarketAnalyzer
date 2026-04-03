"""Orquestador del pipeline de PolymarketAnalyzer.

Ejecuta el análisis de behavioral fingerprinting para todas las wallets
configuradas y programa re-ejecuciones automáticas con APScheduler.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Asegurar que el directorio raíz del proyecto esté en sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Fix encoding en Windows para caracteres Unicode
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from src import config
from src import fetcher
from src import storage
from src import analyzer
from src import classifier

# ── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger("polymarket")
logger.setLevel(logging.DEBUG)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_fmt)
logger.addHandler(_console_handler)

os.makedirs("logs", exist_ok=True)
_file_handler = logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8")
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_fmt)
logger.addHandler(_file_handler)


# ── Pipeline ─────────────────────────────────────────────────────────────────


def analizar_wallet(wallet: str) -> dict | None:
    """Ejecuta el pipeline completo para una wallet.

    Devuelve dict con resultados o None si falla cualquier paso.
    """
    try:
        logger.info("Iniciando análisis: %s...%s", wallet[:10], wallet[-4:])

        trades, markets = fetcher.fetch_completo(wallet)

        # Limpiar campos internos (_slug, _title, etc.) antes de guardar
        trades_para_db = [{k: v for k, v in t.items() if not k.startswith("_")} for t in trades]

        nuevos_trades = storage.guardar_trades(wallet, trades_para_db)
        storage.guardar_markets(markets)

        features = analyzer.calcular_features(wallet)
        estilo, desc, scores = classifier.clasificar(features)

        logger.info(
            "✓ %s...%s → %s | trades: %d | score: %.2f",
            wallet[:10], wallet[-4:], estilo.upper(),
            features["n_trades"], scores[estilo],
        )

        return {
            "wallet": wallet,
            "estilo": estilo,
            "descripcion": desc,
            "features": features,
            "scores": scores,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error("Error analizando %s...%s: %s", wallet[:10], wallet[-4:], e)
        return None


def pipeline() -> list[dict]:
    """Ejecuta el pipeline para todas las wallets configuradas."""
    if not config.WALLETS:
        logger.warning(
            "No hay wallets configuradas. Añade wallets a .env (WALLETS=0x...,0x...)"
        )
        return []

    total = len(config.WALLETS)
    logger.info("=== Pipeline iniciado — %d wallets ===", total)

    resultados: list[dict] = []
    for wallet in config.WALLETS:
        resultado = analizar_wallet(wallet)
        if resultado is not None:
            resultados.append(resultado)

    exitosas = len(resultados)
    logger.info("=== Pipeline completado — %d/%d wallets OK ===", exitosas, total)
    return resultados


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("""
  ╔═══════════════════════════════════╗
  ║     POLYMARKET ANALYZER v1.0      ║
  ║   Behavioral Fingerprinting Bot   ║
  ╚═══════════════════════════════════╝
    """)

    print(f"  Wallets configuradas : {len(config.WALLETS)}")
    print(f"  Intervalo            : cada {config.INTERVALO_HORAS}h")
    print(f"  Base de datos        : {config.DB_PATH}")
    print(f"  Frontend             : streamlit run app.py")
    print()

    if not config.WALLETS:
        print("  ⚠ No hay wallets configuradas.")
        print()
        print("  Para empezar, crea un archivo .env con:")
        print("    WALLETS=0xTuWallet1,0xTuWallet2")
        print("    INTERVALO_HORAS=6")
        print()
        sys.exit(0)

    # Ejecución inicial
    logger.info("Ejecutando pipeline inicial...")
    resultados = pipeline()

    if resultados:
        print(f"\n  {'WALLET':<20s} {'ARQUETIPO':<22s} {'SCORE':>6s}")
        print(f"  {'─' * 20} {'─' * 22} {'─' * 6}")
        for r in resultados:
            wallet_short = f"{r['wallet'][:10]}...{r['wallet'][-4:]}"
            score = r["scores"][r["estilo"]]
            print(f"  {wallet_short:<20s} {r['estilo'].upper():<22s} {score:>6.2f}")
        print()

    # Scheduler
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(pipeline, "interval", hours=config.INTERVALO_HORAS, id="pipeline")

    proxima = datetime.now(timezone.utc) + timedelta(hours=config.INTERVALO_HORAS)
    print(f"\n  Próxima ejecución automática: {proxima.strftime('%Y-%m-%d %H:%M UTC')}")
    print("  (Ctrl+C para detener)\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n  Pipeline detenido por el usuario.")
        scheduler.shutdown()
