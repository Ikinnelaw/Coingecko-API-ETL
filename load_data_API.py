import mysql.connector
from mysql.connector import Error
from config import DB_config
import logging
import pandas as pd
logger = logging.getLogger(__name__)

def crear_base_datos():
    """Crea la base de datos y tabla para crypto"""
    
    try:
        print("="*60)
        print(" CREANDO BASE DE DATOS")
        print("="*60)
        
        # Conectar
        conn = mysql.connector.connect(
            host=DB_config['host'],
            user=DB_config['user'],
            password=DB_config['password']
        )
        cursor = conn.cursor()
        
        # Crear database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_config['database']}")
        cursor.execute(f"USE {DB_config['database']}")
        
        # Crear tabla de precios cyrpto
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS precios_crypto (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(50) NOT NULL,
                precio_usd DECIMAL(20, 2),
                precio_mxn DECIMAL(20, 2),
                precio_eur DECIMAL(20, 2),
                market_cap_usd BIGINT,
                market_cap_mxn BIGINT,
                market_cap_eur BIGINT,
                volumen_24h_usd BIGINT,
                volumen_24h_mxn BIGINT,
                volumen_24h_eur BIGINT,
                cambio_24h_usd DECIMAL(10, 2),
                cambio_24h_mxn DECIMAL(10, 2),
                cambio_24h_eur DECIMAL(10, 2),
                timestamp DATETIME,
                INDEX idx_nombre (nombre),
                INDEX idx_timestamp (timestamp)
            ) 
        """)


        # Tabla details
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                crypto_id VARCHAR(50) NOT NULL,
                name VARCHAR(100),
                symbol VARCHAR(20),
                categories TEXT,
                asset_platform_id VARCHAR(50),
                market_cap_rank VARCHAR(100),
                INDEX idx_crypto_id (crypto_id)
            )
        """)
        
        # Tabla exchanges
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_exchanges (
                id INT AUTO_INCREMENT PRIMARY KEY,
                crypto_id VARCHAR(50) NOT NULL,
                exchange VARCHAR(100),
                base VARCHAR(20),
                target VARCHAR(20),
                last_price DECIMAL(20, 8),
                volume DECIMAL(20, 8),
                timestamp DATETIME,
                INDEX idx_crypto_id (crypto_id),
                INDEX idx_timestamp (timestamp)
            )
        """)
        
        # Tabla market history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                crypto_id VARCHAR(50) NOT NULL,
                price_usd DECIMAL(20, 8),
                timestamp DATETIME,
                INDEX idx_crypto_id (crypto_id),
                INDEX idx_timestamp (timestamp)
            )
        """)
        
        # Tabla trending
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_trending (
                id INT AUTO_INCREMENT PRIMARY KEY,
                crypto_id VARCHAR(50) NOT NULL,
                name VARCHAR(100),
                symbol VARCHAR(20),
                market_cap_rank INT,
                timestamp DATETIME,
                INDEX idx_timestamp (timestamp)
            )
        """)

        conn.commit()
        
        logger.info(f"Base de datos '{DB_config['database']}' lista")
        logger.info("Tabla 'precios_crypto' creada")
        
        cursor.close()
        conn.close()
        return True
        
    except Error as e:
        logger.error(f"Error creando DB: {e}")
        return False


def cargar_datos_crypto(df):
    """Inserta datos del DataFrame a MySQL"""
    
    print("="*60)
    print("CARGANDO DATOS A BASE DE DATOS")
    print("="*60)
    
    try:
        # Conectar
        conn = mysql.connector.connect(
            host=DB_config['host'],
            user=DB_config['user'],
            password=DB_config['password'],
            database=DB_config['database']
        )
        cursor = conn.cursor()
        
        # Insertar cada fila
        insertados = 0
        for indice, row in df.iterrows():
            cursor.execute("""
                INSERT INTO precios_crypto 
                (nombre, precio_usd, precio_mxn,precio_eur, market_cap_usd,market_cap_mxn , market_cap_eur, 
                volumen_24h_usd, volumen_24h_mxn , volumen_24h_eur , cambio_24h_usd, cambio_24h_mxn,
                        cambio_24h_eur, timestamp)
                VALUES (%s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)
            """, (
                row['nombre'],
                float(row['precio_usd']),
                float(row['precio_mxn']),
                float(row['precio_eur']),
                int(row['market_cap_usd']),
                int(row['market_cap_mxn']),
                int(row['market_cap_eur']),
                int(row['volumen_24h_usd']),
                int(row['volumen_24h_mxn']),
                int(row['volumen_24h_eur']),
                float(row['cambio_24h_usd']),
                float(row['cambio_24h_mxn']),
                float(row['cambio_24h_eur']),
                row['timestamp']
            ))
            insertados += 1
        
        conn.commit()
        
        logger.info(f" Insertados {insertados} registros")
        
        cursor.close()
        conn.close()
        return insertados
        
    except Error as e:
        logger.error(f" Error insertando datos: {e}")
        return 0


def cargar_datos_details(df):
    """Inserta datos del DataFrame de details a MySQL"""
    
    if df is None or df.empty:
        logger.info("No hay datos de details para cargar")
        return 0
    
    print("="*60)
    print("CARGANDO DATOS DE DETAILS")
    print("="*60)
    
    try:
        conn = mysql.connector.connect(
            host=DB_config['host'],
            user=DB_config['user'],
            password=DB_config['password'],
            database=DB_config['database']
        )
        cursor = conn.cursor()
        
        insertados = 0
        for indice, row in df.iterrows():
            cursor.execute("""
                INSERT INTO crypto_details 
                (crypto_id, name, symbol, categories, asset_platform_id, market_cap_rank)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['crypto_id'],
                row['name'],
                row['symbol'],
                row['categories'],
                row['asset_platform_id'],
                row['market_cap_rank']
            ))
            insertados += 1
        
        conn.commit()
        logger.info(f"Insertados {insertados} registros de details")
        
        cursor.close()
        conn.close()
        return insertados
        
    except Error as e:
        logger.error(f"Error insertando details: {e}")
        return 0


def cargar_datos_exchanges(df):
    """Inserta datos del DataFrame de exchanges a MySQL"""
    
    if df is None or df.empty:
        logger.info("No hay datos de exchanges para cargar")
        return 0
    
    print("="*60)
    print("CARGANDO DATOS DE EXCHANGES")
    print("="*60)
    
    try:
        conn = mysql.connector.connect(
            host=DB_config['host'],
            user=DB_config['user'],
            password=DB_config['password'],
            database=DB_config['database']
        )
        cursor = conn.cursor()
        
        insertados = 0
        for indice, row in df.iterrows():
            cursor.execute("""
                INSERT INTO crypto_exchanges 
                (crypto_id, exchange, base, target, last_price, volume, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['crypto_id'],
                row['exchange'],
                row['base'],
                row['target'],
                float(row['last_price']),
                float(row['volume']),
                row['timestamp']
            ))
            insertados += 1
        
        conn.commit()
        logger.info(f"Insertados {insertados} registros de exchanges")
        
        cursor.close()
        conn.close()
        return insertados
        
    except Error as e:
        logger.error(f"Error insertando exchanges: {e}")
        return 0


def cargar_datos_market_history(df):
    """Inserta datos del DataFrame de market history a MySQL"""
    
    if df is None or df.empty:
        logger.info("No hay datos de market history para cargar")
        return 0
    
    print("="*60)
    print("CARGANDO DATOS DE MARKET HISTORY")
    print("="*60)
    
    try:
        conn = mysql.connector.connect(
            host=DB_config['host'],
            user=DB_config['user'],
            password=DB_config['password'],
            database=DB_config['database']
        )
        cursor = conn.cursor()
        
        insertados = 0
        for indice, row in df.iterrows():
            cursor.execute("""
                INSERT INTO market_history 
                (crypto_id, price_usd, timestamp)
                VALUES (%s, %s, %s)
            """, (
                row['crypto_id'],
                float(row['price_usd']),
                row['timestamp']
            ))
            insertados += 1
        
        conn.commit()
        logger.info(f"Insertados {insertados} registros de market history")
        
        cursor.close()
        conn.close()
        return insertados
        
    except Error as e:
        logger.error(f"Error insertando market history: {e}")
        return 0


def cargar_datos_trending(df):
    """Inserta datos del DataFrame de trending a MySQL"""
    
    if df is None or df.empty:
        logger.info("No hay datos de trending para cargar")
        return 0
    
    print("="*60)
    print("CARGANDO DATOS DE TRENDING")
    print("="*60)
    
    try:
        conn = mysql.connector.connect(
            host=DB_config['host'],
            user=DB_config['user'],
            password=DB_config['password'],
            database=DB_config['database']
        )
        cursor = conn.cursor()
        
        insertados = 0
        for indice, row in df.iterrows():
            cursor.execute("""
                INSERT INTO crypto_trending 
                (crypto_id, name, symbol, market_cap_rank, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['crypto_id'],
                row['name'],
                row['symbol'],
                int(row['market_cap_rank']) if row['market_cap_rank'] else None,
                row['timestamp']
            ))
            insertados += 1
        
        conn.commit()
        logger.info(f"Insertados {insertados} registros de trending")
        
        cursor.close()
        conn.close()
        return insertados
        
    except Error as e:
        logger.error(f"Error insertando trending: {e}")
        return 0
