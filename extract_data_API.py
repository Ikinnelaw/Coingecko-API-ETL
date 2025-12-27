import requests
import logging
from datetime import datetime, timezone

API_URL = 'https://api.coingecko.com/api/v3/simple/price'
API_URL_DETAILED = 'https://api.coingecko.com/api/v3/coins/'
API_TRENDING = "https://api.coingecko.com/api/v3/search/trending"
API_MARKET = "https://api.coingecko.com/api/v3/coins/{}/market_chart"
API_MARKET_GLOBAL = "https://api.coingecko.com/api/v3/global"

cryptos = ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana']
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
        else:
            print(f"Error en la API de precios: {response_price.status_code}")
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")

# Función para obtener detalles de la criptomoneda
def get_details():
    try:
        for crypto in cryptos:
            response_detailed = requests.get(f"{API_URL_DETAILED}{crypto}", params=parametros_detalle, timeout=10)
            
            if response_detailed.status_code == 200:
                data = response_detailed.json()
                print(f"\nDetalles de {crypto}:")
                print(f"Nombre: {data['name']}")
                print(f"Símbolo: {data['symbol']}")
                print(f"Categorías: {', '.join(data['categories'])}")
                print(f"Descripción: {data['description']['en']}")
            else:
                print(f"Error en la API de detalles para {crypto}: {response_detailed.status_code}")
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")


# Función para obtener los exchanges
def get_exchanges():
    for crypto in cryptos:
        try:
            response = requests.get(f"{API_URL_DETAILED}{crypto}/tickers", timeout=10)
            if response.status_code == 200:
                tickers_data = response.json()
                if 'tickers' in tickers_data and tickers_data['tickers']:
                    print(f"\nExchanges para {crypto}:")

                    count = 0  # Contador para limitar a 10 resultados cada moneda para evitar que saturemos la API
                    for exchange in tickers_data['tickers']:
                        if count == 10:  # Si ya se han mostrado 10, sale del bucle
                            break
                        print(f"  Exchange: {exchange['market']['name']}")
                        print(f"  Par de comercio: {exchange['market']['identifier']}")
                        print(f"  Precio: {exchange.get('last', 'No disponible')} USD")
                        count += 1  # Incrementar el contador
                else:
                    print(f"No hay exchanges disponibles para {crypto}")
            else:
                print(f"Error {response.status_code} al obtener exchanges para {crypto}")
        except requests.exceptions.Timeout:
            print("Error: La solicitud ha superado el tiempo de espera")
        except Exception as e:
            print(f"Ocurrió un error inesperado: {e}")

# Función para obtener el historial de mercado
def get_market_history():
    try:
        # recorremos cada moneda  de nuestra lista para obtener su market 
        for crypto in cryptos:
            market_url = API_MARKET.format(crypto)
            response_market = requests.get(market_url, params=params_market, timeout=10)
            
            if response_market.status_code == 200:
                data_market = response_market.json()
                print(f"\nHistorial del mercado para {crypto}:")
                print("Primeros 5 datos del historial de precios (últimos 30 días):")
                position = 1
                
                #Validamos sl precio 
                for entry in data_market['prices']:
                    if position > 5:  # Limita la iteración a los primeros 5 elementos
                        break
                    timestamp = entry[0]
                    price = entry[1]
                    print(f"Posición {position} - Fecha: {timestamp}, Precio: {price} USD")
                    position += 1
            elif response_market.status_code == 429:
                print(f"Demasiadas solicitudes a la API, esperando antes de volver a intentar con {crypto}...")
            else:
                print(f"Error al obtener el historial de mercado de {crypto}: {response_market.status_code}")
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
        else:
            print(f"Error global market: {response_market_global.status_code}")
    except requests.exceptions.Timeout:
        logger.error("Error: La solicitud ha superado el tiempo de espera")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")

# Función principal para llamar todas las funciones
def extract_data():
    #get_prices()
    #get_details()
    get_exchanges()
    #get_market_history()
    #get_trending()
    #get_global_market()

# Ejecutar el script
extract_data()
