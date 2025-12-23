import pandas as pd
from datetime import datetime
from extract_data_API import extract_data

def transform_data(datos_json):

    if not datos_json: 
        print(f"No hay datos para transformar")
        return None

    # Se crea una lista con los diccionarios del registro 
    registros = []

    #crypto_name NOmbre de la moneda bitcoin, ethereum, etc
    #crypto_data = Toda la información de esa moneda 
    for crypto_name, crypto_data in datos_json.items():
        try:
            registro = {
                'nombre': crypto_name,
                'precio_usd': crypto_data.get('usd', 0),
                'precio_eur': crypto_data.get('eur', 0),
                'precio_mxn': crypto_data.get('mxn', 0),
                'market_cap_usd': crypto_data.get('usd_market_cap', 0),
                'market_cap_mxn': crypto_data.get('mxn_market_cap', 0),
                'market_cap_eur': crypto_data.get('eur_market_cap', 0),
                'volumen_24h_usd': crypto_data.get('usd_24h_vol', 0),
                'volumen_24h_mxn': crypto_data.get('mxn_24h_vol', 0),
                'volumen_24h_eur': crypto_data.get('eur_24h_vol', 0),
                'cambio_24h_usd': crypto_data.get('usd_24h_change', 0),
                'cambio_24h_mxn': crypto_data.get('mxn_24h_change', 0),
                'cambio_24h_eur': crypto_data.get('eur_24h_change', 0),
                'timestamp': datetime.now()
            }
            registros.append(registro)


        except Exception as e:
            print(f"Error al procesar los datos de la criptomoneda {crypto_name}: {e}")
            continue  # Continúa con el siguiente registro en caso de error
    
    try:
        # Crear DataFrame
        df = pd.DataFrame(registros)

        # Redondear números
        df['precio_usd'] = df['precio_usd'].round(2)
        df['precio_mxn'] = df['precio_mxn'].round(2)
        df['precio_eur'] = df['precio_eur'].round(2)
        df['cambio_24h_usd'] = df['cambio_24h_usd'].round(2)
        df['cambio_24h_mxn'] = df['cambio_24h_mxn'].round(2)
        df['cambio_24h_eur'] = df['cambio_24h_eur'].round(2)
    
    except Exception as e:
        print(f"Error al crear el DataFrame: {e}")
        return None

    return df

datos = extract_data()
df = transform_data(datos)
#print(df)