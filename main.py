from datetime import datetime
from extract_data_API import extract_data
from transform_data import (transform_prices, transform_details , transform_exchanges, transform_trending  ,
                            transform_market_history)

from load_data_API import  (crear_base_datos , cargar_datos_crypto , cargar_datos_details , 
                            cargar_datos_exchanges  , cargar_datos_market_history , cargar_datos_trending)
import logging , os
from pathlib import Path

LOGS_PATH = os.path.join('Logs')  
LOG_FILE = os.path.join(LOGS_PATH, 'coingecko-api.log') 

# Crear carpeta Logs dentro de BASE_PATH si no existe
if not os.path.exists(LOGS_PATH):
    os.makedirs(LOGS_PATH)
    print(f"Carpeta creada: {LOGS_PATH}")


# Configuracion para los Logs 
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',  
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def ejecutar_pipeline():
    """Pipeline ETL para crypto"""
    
    print("\n" + "="*60)
    print(" PIPELINE ETL: CoinGecko API Insertando en MySQL")
    print("="*60)
    print(f" Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    try:
        # PASO 1: Crear DB (solo primera vez)
        logger.info("Creando base de datos y tablas...")
        crear_base_datos()
        
        # PASO 2: Extract
        logger.info("FASE 1/3: Extraccion de API")
        raw_data = extract_data()
        
        if not raw_data:
            logger.error("No se pudieron extraer datos de la API")
            return
        
        # PASO 3: Transform
        logger.info("FASE 2/3: Transformacion")
        
        df_prices = transform_prices(raw_data.get("prices"))
        df_details = transform_details(raw_data.get("details"))
        df_exchanges = transform_exchanges(raw_data.get("exchanges"))
        df_market = transform_market_history(raw_data.get("market_history"))
        df_trending = transform_trending(raw_data.get("trending"))
        
        # PASO 4: Load
        logger.info("FASE 3/3: Carga a Base de Datos")

        # PASO 4: Load
        logger.info("FASE 3/3: Carga a Base de Datos")
        
        total_registros = 0
        
        # Cargar precios
        if df_prices is not None and not df_prices.empty:
            registros = cargar_datos_crypto(df_prices)
            total_registros += registros
            logger.info(f" Precios cargados: {registros} registros")
        else:
            logger.warning(" No hay datos de precios para cargar")
        
        # Cargar details
        if df_details is not None and not df_details.empty:
            registros = cargar_datos_details(df_details)
            total_registros += registros
            logger.info(f" Details cargados: {registros} registros")
        else:
            logger.warning(" No hay datos de details para cargar")
        
        # Cargar exchanges
        if df_exchanges is not None and not df_exchanges.empty:
            registros = cargar_datos_exchanges(df_exchanges)
            total_registros += registros
            logger.info(f" Exchanges cargados: {registros} registros")
        else:
            logger.warning(" No hay datos de exchanges para cargar")
        
        # Cargar market history
        if df_market is not None and not df_market.empty:
            registros = cargar_datos_market_history(df_market)
            total_registros += registros
            logger.info(f" Market history cargado: {registros} registros")
        else:
            logger.warning(" No hay datos de market history para cargar")
        
        # Cargar trending
        if df_trending is not None and not df_trending.empty:
            registros = cargar_datos_trending(df_trending)
            total_registros += registros
            logger.info(f" Trending cargado: {registros} registros")
        else:
            logger.warning(" No hay datos de trending para cargar")
            
        print("\n" + "="*60)
        print(" PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*60)
        print(f" Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
    except Exception as e:
        logger.error(f" ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    ejecutar_pipeline()