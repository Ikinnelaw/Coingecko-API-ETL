import requests

API_URL = 'https://api.coingecko.com/api/v3/simple/price'

cryptos = ['bitcoin', 'ethereum','cardano','solana','dogecoin']
currencies = ['usd','eur','mxn']

parametros = {
    'ids' : ','.join(cryptos),
    'names' : cryptos ,
    'vs_currencies' : ','.join(currencies),
    'include_market_cap': 'true',
    'include_24hr_change' : 'true',
    'include_last_updated_at' : 'true'

}

def extract_data():

    try :

        # Hacemos la solicitud Get
        response = requests.get(API_URL, params= parametros , timeout=10)

        

        if response.status_code == 200 :

            #COnvertimos la informacion a json
            datos = response.json()

            print(f"Datos obtenidos : {len(datos)} criptomonedas")
            print(f"Cryptos : {list(datos.keys())}")
            print(datos)
            return datos
        else :
            print(f"Error en API : Status code {response.status_code}")

        
    except requests.exceptions.Timeout:
        print("Error : La solicitud ha superado el tiempo de espera")
    except Exception as e :
        print(f"Ocurrio un error inesperado : {e}")

extract_data()