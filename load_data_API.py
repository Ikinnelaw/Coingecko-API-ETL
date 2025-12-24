import mysql.connector
from mysql.connector import Error
from config import DB_config

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
        
        # Crear tabla
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
        
        conn.commit()
        
        print(f"Base de datos '{DB_config['database']}' lista")
        print("Tabla 'precios_crypto' creada")
        
        cursor.close()
        conn.close()
        return True
        
    except Error as e:
        print(f"Error creando DB: {e}")
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
        for _, row in df.iterrows():
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
        
        print(f" Insertados {insertados} registros")
        
        cursor.close()
        conn.close()
        return insertados
        
    except Error as e:
        print(f" Error insertando datos: {e}")
        return 0
