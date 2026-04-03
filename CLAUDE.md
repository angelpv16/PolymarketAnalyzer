# PolymarketAnalyzer

Sistema de behavioral fingerprinting para carteras de Polymarket.
Analiza el historial de trades para detectar arquetipos de estrategia de trading.

## Stack
Python 3.11 · DuckDB · pandas · Streamlit · APScheduler · requests · plotly

## Estructura
src/        → módulos principales del sistema
data/       → base de datos DuckDB (ignorada por git)
tests/      → tests unitarios
logs/       → logs del pipeline (ignorados por git)
app.py      → frontend Streamlit (raíz del proyecto)

## Comandos clave
- Instalar dependencias: pip install -r requirements.txt
- Ejecutar pipeline:     python src/main.py
- Lanzar frontend:       streamlit run app.py
- Correr tests:          python -m pytest tests/ -v

## Convenciones
- Type hints en todas las funciones públicas
- Cada módulo en src/ tiene docstring y bloque if __name__ == "__main__" para testing standalone
- Los errores se loggean con el módulo logging, nunca se silencian con pass
- Sin servicios externos de notificación: el output es solo Streamlit
