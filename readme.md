# **Crypto Tracker - ETL Pipeline Batch**

Pipeline ETL por lotes (Batch Processing) que extrae, procesa y almacena datos de criptomonedas desde la API de CoinGecko en una base de datos MySQL.

## API para este proyecto

https://docs.coingecko.com/v3.0.1/reference/endpoint-overview  

## Descripción del Proyecto

Este proyecto implementa un sistema ETL batch que ejecuta procesamiento por lotes de forma periódica. El pipeline:

Extrae datos en tiempo real de criptomonedas desde CoinGecko API
Transforma los datos crudos en estructuras organizadas usando Pandas
Carga la información procesada en una base de datos MySQL relacional

El sistema recopila información completa sobre Bitcoin, Ethereum y Tether, incluyendo precios en múltiples monedas (USD, EUR, MXN), datos históricos de mercado, información de exchanges, tendencias y métricas globales del mercado crypto.

## ¿Qué Logra Este Proyecto?

### Datos Recopilados:

**Precios:** en tiempo real en USD, EUR y MXN
Market Cap y Volumen de las últimas 24 horas Cambios porcentuales del mercado  
**Detalles técnicos:** ( categorías , plataforma de block chain , posición de la moneda segun su capitalización de mercado )
**Información de exchanges :** (5 exchanges principales por cripto)  
**Historial de mercado :** (últimos 30 días)  
**Trending :** (Top 10 criptomonedas más populares)


## Tecnologías Utilizadas 


**Python** 3.x  
**Pandas:** Procesamiento y transformación de datos  
**MySQL:**: Base de datos relacional  
**Requests:**: Consumo de API REST  
**Logging:**: Sistema de logs para monitoreo

## Arquitectura del proyecto 

``` 
crypto-tracker/
│
├── Logs/                     # Carpeta de logs
│   └── coingecko-api.log    # Registro de ejecuciones
├── extract_data_API.py      # Extracción de datos desde 
├── transform_data.py         # Transformación de datos con 
├── load_data_API.py          # Carga de datos a MySQL
├── main.py                   # Pipeline ETL completo
├── config.py                 # Configuración de la base de datos
|── requirements.txt          # Archivo con las librerias Utilizadas 
|── README.md 
```

## Estructura  de la Base de Datos 

### El proyecto crea 6 tablas en MySQL:
**1. precios_crypto**  
Precios actuales en múltiples monedas con market cap y volumen  
**2. crypto_details**   
Información técnica y de origen de cada criptomoneda  
**3. crypto_exchanges**  
Datos de trading de los principales exchanges  
**4. market_history**  
Historial de precios de los últimos 30 días  
**5. crypto_trending**  
Top 10 criptomonedas más buscadas  

## Installation

### Primero cambiar las credenciales del archivo config.py 

```
config.py
    'host': 'localhost', # Poner la ip de tu servidor BD  
    'user' : 'youruser', # pon aqui el usuario de tu DB  
    'password': 'yourpassword', # Agrega aqui tu contraseña  
    'database' : 'crypto_tracker' #El nombre que quieras que tenga la DB lo puedes cambiar por otro  
}
```
    

Ejecutar los siguientes comandos en Windows (si utiliza un entorno Vritual ), caso contrario empezar desde el paso 3

```bash
  1.- Python -m venv venv
  2.-  .\venv\Scripts\activate  
  3.- pip install -r requirements.txt
  4.- Ejecutar el archivo main.py
```

Ejecutar los siguientes comandos si usa Linux (si utiliza un entorno Vritual ), caso contrario empezar desde el paso 3
```bash
   1.- Python3 -m venv venv
  2.- cd source venv/bin/activate  
  3.- pip install -r requirements.txt
  4.- Ejecutar el archivo main.py
```


### Logs 
El sistema genera logs automáticos en Logs/coingecko-api.log con:  

Timestamp de cada ejecución  
Éxitos y errores en la extracción  
Número de registros insertados  
Warnings de rate limits 
 
## Authors

- Erick DZ
- Mi perfil de github es : [@Ikinnelaw](https://github.com/Ikinnelaw)
