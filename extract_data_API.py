import requests , time
import logging
from datetime import datetime, timezone

API_URL = 'https://api.coingecko.com/api/v3/simple/price'
API_URL_DETAILED = 'https://api.coingecko.com/api/v3/coins/'
API_TRENDING = "https://api.coingecko.com/api/v3/search/trending"
API_MARKET = "https://api.coingecko.com/api/v3/coins/{}/market_chart"
API_MARKET_GLOBAL = "https://api.coingecko.com/api/v3/global"

cryptos = ['bitcoin', 'ethereum', 'tether']
currencies = ['usd','eur','mxn']
HEADERS = {"accept": "application/json"}

parametros = {
    'ids' : ','.join(cryptos),
    'vs_currencies' : ','.join(currencies),
    'include_market_cap': 'true',
    'include_24hr_vol': 'true', 
    'include_24hr_change' : 'true',
    'include_last_updated_at' : 'true'
}

parametros_detalle = {
    'localization': 'false',  # Para evitar la traducción de nombres
}

days = 30
params_market = {
    "vs_currency": 'usd',  
    "days": days
}

logger = logging.getLogger(__name__)

# Función para obtener los precios
def get_prices():
    try:
        response_price = requests.get(API_URL, params=parametros, timeout=10)
        
        if response_price.status_code == 200:
            simple_data = response_price.json()
            print(f"Datos obtenidos: {len(simple_data)} criptomonedas")
            print(f"Cryptos: {list(simple_data.keys())}")
            print("Precios de las criptomonedas:", simple_data)

            return simple_data
        else:
            print(f"Error en la API de precios: {response_price.status_code}")
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")

# Función para obtener detalles de la criptomoneda
def get_details():
    try:

        resultados = {}

        for crypto in cryptos:
            response = requests.get(f"{API_URL_DETAILED}{crypto}",params=parametros_detalle,timeout=10)

            if response.status_code == 200:
                resultados[crypto] = response.json()
                #print(f"JSON para {crypto}: {resultados[crypto]}")

            elif response.status_code == 429:
                logger.warning(f"Rate limit alcanzado para {crypto}")

            else:
                logger.error(f"Error {response.status_code} para {crypto}")

            time.sleep(1.5)  # nos esperamos un minuto

        return resultados if resultados else None 
            
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error en get_details: {e}")
        return None


# Función para obtener los exchanges
def get_exchanges():

    try:
        resultados = {} 
    
        for crypto in cryptos:
            response = requests.get(
                f"{API_URL_DETAILED}{crypto}/tickers",
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()

                tickers_limitados = []
                contador = 0

                for ticker in data.get("tickers", []):

                    if contador == 5:
                        break

                    tickers_limitados.append(ticker)
                    contador += 1

                # Reemplazamos los tickers originales por los limitados
                data["tickers"] = tickers_limitados

                resultados[crypto] = data

                print(f"JSON para {crypto}: {resultados[crypto]}")
            else:
                logger.error(f"Error {response.status_code} para {crypto}")

            time.sleep(1.5)  # evita 429 (rate limit)

        return resultados if resultados else None
        
    except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")

# Función para obtener el historial de mercado
def get_market_history():
    try:
        resultados = {}

        for crypto in cryptos:
            market_url = API_MARKET.format(crypto)
            response_market = requests.get(market_url,params=params_market,timeout=10)

            if response_market.status_code == 200:
                data_market = response_market.json()

                precios_limitados = []
                contador = 0

                for entry in data_market.get("prices", []):

                    if contador == 5:
                        break

                    precios_limitados.append({
                        "timestamp": entry[0],
                        "price_usd": entry[1]
                    })

                    contador += 1

                resultados[crypto] = precios_limitados
            
                #Para debug ver los resultados 
                #print(f"JSON para {crypto}: {resultados[crypto]}")
                print(f"{crypto} → precios cargados: {contador}")
            
                time.sleep(1.5)  # evitar 429
            
            elif response_market.status_code == 429:
                logger.warning(
                    f"Rate limit alcanzado para {crypto}, esperando..."
                )
                time.sleep(5)

            else:
                logger.error(
                    f"Error {response_market.status_code} al obtener market de {crypto}"
                )

        return resultados if resultados else None
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")

# Función para obtener las criptomonedas en tendencia
def get_trending():
    try:
        response_trending = requests.get(API_TRENDING, timeout=10)
        
        if response_trending.status_code == 200:
            trending_data = response_trending.json()
            trending_coins = trending_data['coins'][:10]
            print("\nTop 10 criptomonedas en tendencia:")
            position = 1
            for crypto in trending_coins:
                coin = crypto['item']  # Información de la criptomoneda
                print(f"Posición: {position}")
                print(f"Nombre: {coin['name']}")
                print(f"Símbolo: {coin['symbol']}")
                print(f"ID: {coin['id']}")
                position += 1

            return trending_data
        else:
            print(f"Error en la API de tendencias: {response_trending.status_code}")
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")

# Función para obtener datos globales del mercado
def get_global_market():
    try:
        response_market_global = requests.get(API_MARKET_GLOBAL, headers=HEADERS, timeout=10)
        
        if response_market_global.status_code == 200:

            logger.info("Global market extraído")
            data_global = response_market_global.json()
            print(f"La data global es: \n {data_global}")
            time_now = datetime.now(timezone.utc)
            print(f"El tiempo actual es : {time_now}")

            return data_global
        
        else:
            print(f"Error global market: {response_market_global.status_code}")
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")

# Función principal para llamar todas las funciones
def extract_data():
    return {
        #"prices": get_prices(),
        #"details": get_details(),
        #"exchanges": get_exchanges(),
        #"market_history": get_market_history(),
        #"trending": get_trending(),
        #"global_market": get_global_market()
    }

# Ejecutar el script
extract_data()
