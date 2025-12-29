import pandas as pd
from datetime import datetime , timezone
from extract_data_API import extract_data
import logging

logger = logging.getLogger(__name__)

def transform_prices(prices_json):

    if not prices_json: 
        logger.info(f"No hay datos para transformar")
        return None

    # Se crea una lista con los diccionarios del registro 
    registros = []

    #crypto_name NOmbre de la moneda bitcoin, ethereum, etc
    #crypto_data = Toda la información de esa moneda 
    for crypto_name, crypto_data in prices_json.items():
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
            logger.error(f"Error al procesar los datos de la criptomoneda {crypto_name}: {e}")
            continue  # Continúa con el siguiente registro en caso de error
    
    try:
        # Crear DataFrame
        dataframe = pd.DataFrame(registros)

        # Redondear números
        dataframe['precio_usd'] = dataframe['precio_usd'].round(2)
        dataframe['precio_mxn'] = dataframe['precio_mxn'].round(2)
        dataframe['precio_eur'] = dataframe['precio_eur'].round(2)
        dataframe['cambio_24h_usd'] = dataframe['cambio_24h_usd'].round(2)
        dataframe['cambio_24h_mxn'] = dataframe['cambio_24h_mxn'].round(2)
        dataframe['cambio_24h_eur'] = dataframe['cambio_24h_eur'].round(2)
    
    except Exception as e:
        logger.error(f"Error al crear el DataFrame: {e}")
        return None

    return dataframe


def transform_details(details_json):

    if not details_json:
        logger.info("No hay datos para transformar en details")
        return None

    registros = []

    try:
        for crypto_id, data in details_json.items():
            registro = {
                "crypto_id": crypto_id,
                "name": data.get("name"),
                "symbol": data.get("symbol"),
                "categories": ",".join(data.get("categories", [])),
                "asset_platform_id": data.get("asset_platform_id"),
                "market_cap_rank": data.get("market_cap_rank"),
            }

            registros.append(registro)

        dataframe = pd.DataFrame(registros)
        return dataframe

    except Exception as e:
        logger.error(f"Error transformando details: {e}")
        return None


#Transformar data para el dataframe de exchange 
def transform_exchanges(exchanges_json):
    # Verificar si los datos de exchanges están vacíos
    if not exchanges_json:
        logger.info("No hay datos para transformar para el exchange.")
        return None

    registros = []  # Lista para almacenar los registros

    for crypto_id, data in exchanges_json.items():
        for ticker in data.get("tickers", []):
            try:
                registro = {
                    "crypto_id": crypto_id,
                    "exchange": ticker["market"]["name"],
                    "base": ticker.get("base"),
                    "target": ticker.get("target"),
                    "last_price": ticker.get("last"),
                    "volume": ticker.get("volume"),
                    "timestamp": datetime.now(timezone.utc)
                }
                registros.append(registro)

            except KeyError as e:
                logger.error(f"Faltó una clave en {crypto_id}: {e}")
                continue
    if not registros:  # Si no se crearon registros, retornar None
        logger.info("No se pudieron crear registros para el exchange.")
        return None
    
    try:
        # Intentar crear el DataFrame con los registros
        dataframe = pd.DataFrame(registros)
    except Exception as e:
        logger.error(f"Error al crear el DataFrame para el exchange: {e}")
        return None

    return dataframe


#Funcion para extraer el market historico 

def transform_market_history(market_json):

    if not market_json:
        logger.info("No hay datos para transformar del market history.")
        return None

    registros = []

    for crypto_id, prices_list in market_json.items():

        for price in prices_list:
            try:
                registro = {
                    "crypto_id": crypto_id,
                    "price_usd": price["price_usd"],
                    "timestamp": datetime.fromtimestamp(
                        price["timestamp"] / 1000,
                        tz=timezone.utc
                    )
                }

                registros.append(registro)

            except (IndexError, TypeError) as e:
                logger.error(
                    f"Error procesando market history de {crypto_id}: {e}"
                )
                continue

    if not registros:
        logger.info("No se pudieron crear registros para market history.")
        return None

    try:
        dataframe = pd.DataFrame(registros)
        return dataframe

    except Exception as e:
        logger.error(f"Error al crear DataFrame de market history: {e}")
        return None


#Funcion para extraer las monedas mas populares en su df 
def transform_trending(trending_json):

    if not trending_json:
        logger.info("No se transformaron registros para el trending json ")
        return None
    
    registros = []

    for item in trending_json.get("coins", []):
        coin = item["item"]
        try:
            registro = ({
                "crypto_id": coin["id"],
                "name": coin["name"],
                "symbol": coin["symbol"],
                "market_cap_rank": coin.get("market_cap_rank"),
                "timestamp": datetime.utcnow()
            })
            registros.append(registro)
        
        except KeyError as e:
            logger.error(f"Falto una clave en el coin: {e} - {coin}")
            continue  # Si falta alguna clave, saltar al siguiente ticker


    if not registros:  # Si no se crearon registros, retornar None
        logger.info("No se pudieron crear registros para el trending.")
        return None
    
    try:
        # Intentar crear el DataFrame con los registros
        dataframe = pd.DataFrame(registros)
    except Exception as e:
        logger.error(f"Error al crear el DataFrame para el trending: {e}")
        return None

    return dataframe


#Logs para probar que funcionan
#raw_data = extract_data()

#df_prices = transform_prices(raw_data["prices"])
#df_details = transform_details(raw_data["details"])
#df_exchanges = transform_exchanges(raw_data["exchanges"])
#df_market = transform_market_history(raw_data["market_history"])
#df_trending = transform_trending(raw_data["trending"])
#df_global = transform_global_market(raw_data["global_market"])

#print(df_market)
#print(type(df_market))