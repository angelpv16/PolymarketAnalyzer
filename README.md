# PolymarketAnalyzer

Sistema de behavioral fingerprinting para carteras de Polymarket.
Analiza el historial de trades de cualquier wallet para detectar su arquetipo de estrategia de trading.

## Que analiza

- **Timing de entrada**: antelacion media a la resolucion del mercado, patrones horarios, porcentaje de entradas en la ultima semana del mercado
- **Sizing**: importe medio por trade, variabilidad (coeficiente de variacion), correlacion entre tamano de posicion y precio
- **Seleccion de mercado**: distribucion de precios de entrada, categorias preferidas, concentracion tematica
- **Comportamiento de salida**: hold rate (porcentaje de posiciones mantenidas hasta resolucion), cierres anticipados

## Arquetipos detectados

| Arquetipo | Senal principal | Descripcion |
|---|---|---|
| **end-of-day** | Antelacion < 3 dias, >70% entradas en ultima semana | Opera justo antes de que cierre el mercado, apostando al resultado mas probable |
| **contrarian** | Price medio < 25%, antelacion > 10 dias | Compra tokens baratos contra el consenso, con alta antelacion |
| **insider-like** | Antelacion > 20 dias, hold rate > 90% | Entra muy pronto y mantiene hasta resolucion con alta conviccion |
| **momentum** | Correlacion size-price > 0.45 | Aumenta posiciones cuando el precio sube, siguiendo la tendencia |
| **hold-to-resolution** | Hold rate > 92%, antelacion >= 3 dias | Mantiene casi todas las posiciones hasta resolucion sin cierres anticipados |
| **bimodal / mixed** | >35% tokens baratos y >35% tokens medios | Distribucion de precios bimodal, estrategia mixta |
| **value / mixed** | No encaja en ningun patron especifico | Estrategia diversificada o de valor, sin patron dominante |

## Instalacion

```bash
git clone https://github.com/angelpv16/PolymarketAnalyzer.git
cd PolymarketAnalyzer
python -m venv venv
source venv/bin/activate        # Linux/macOS
# venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

## Configuracion

```bash
cp .env.example .env
```

Edita `.env` con tus valores:

```env
# Wallets a monitorear (separadas por coma, sin espacios)
WALLETS=0xc867f7b28a7cbe179e098dd07077f01f84e38b00,0xOtraWallet

# Intervalo de re-ejecucion automatica del pipeline (en horas)
INTERVALO_HORAS=6
```

| Variable | Descripcion | Ejemplo |
|---|---|---|
| `WALLETS` | Lista de direcciones Ethereum separadas por coma | `0xabc...,0xdef...` |
| `INTERVALO_HORAS` | Horas entre cada ejecucion automatica del pipeline | `6` |

## Uso

### Frontend (recomendado)

```bash
streamlit run app.py
```

Abre `http://localhost:8501`, introduce una direccion de wallet y pulsa **Analizar**.
El frontend muestra: arquetipo detectado, metricas clave, distribucion de precios, scores por arquetipo y ultimos trades.

### Pipeline automatico

```bash
python src/main.py
```

Analiza todas las wallets configuradas en `.env` y programa re-ejecuciones cada `INTERVALO_HORAS` horas con APScheduler.

### Tests

```bash
python -m pytest tests/ -v
```

## Estructura del proyecto

```
PolymarketAnalyzer/
├── app.py                  # Frontend Streamlit
├── src/
│   ├── __init__.py
│   ├── config.py           # Carga de variables de entorno (.env)
│   ├── fetcher.py          # Fetch de trades (Data API) y markets (Gamma API)
│   ├── storage.py          # Capa de almacenamiento DuckDB (schema, CRUD)
│   ├── analyzer.py         # Calculo de features de behavioral fingerprinting
│   ├── classifier.py       # Clasificacion en arquetipos con reglas y scores
│   ├── update_resolutions.py # Actualizacion de resoluciones de mercados (Gamma API)
│   └── main.py             # Orquestador del pipeline con APScheduler
├── tests/
│   ├── __init__.py
│   ├── test_analyzer.py    # Tests unitarios del analyzer (DuckDB en memoria)
│   └── test_classifier.py  # Tests unitarios del clasificador
├── data/                   # Base de datos DuckDB (ignorada por git)
├── logs/                   # Logs del pipeline (ignorados por git)
├── .env.example            # Plantilla de configuracion
├── CLAUDE.md               # Instrucciones del proyecto
├── requirements.txt        # Dependencias Python
└── .gitignore
```

## Notas tecnicas

- La API publica usada es `data-api.polymarket.com` (sin autenticacion). La info de mercados se obtiene de `gamma-api.polymarket.com`.
- **win_rate y ROI**: se calculan cruzando el outcome de cada trade con el resultado de resolucion del mercado obtenido de la Gamma API. El script `src/update_resolutions.py` consulta mercados con fecha pasada y actualiza la DB con el outcome ganador. Se ejecuta automaticamente al inicio de cada pipeline.
- Los datos se almacenan en `data/polymarket.db` (DuckDB local, ignorado por git).
- El clasificador usa reglas evaluadas en orden de prioridad. Los scores (0-1) dan una vision cuantitativa complementaria.
- El hold rate se calcula como porcentaje de mercados donde solo hay compras (sin ventas = mantuvo hasta resolucion).
