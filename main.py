from datetime import datetime
from extract_data_API import extract_data
from transform_data import transform_data
from load_data_API import crear_base_datos , cargar_datos_crypto

def ejecutar_pipeline():
    """Pipeline ETL para crypto"""
    
    print("\n" + "="*60)
    print(" PIPELINE ETL: CoinGecko API → MySQL")
    print("="*60)
    print(f" Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    try:
        # PASO 1: Crear DB (solo primera vez)
        crear_base_datos()
        
        # PASO 2: Extract
        print("🔹 FASE 1/3: Extracción de API")
        datos = extract_data()
        
        if not datos:
            print(" No se pudieron extraer datos")
            return
        
        # PASO 3: Transform
        print(" FASE 2/3: Transformacion")
        dataframe = transform_data(datos)
        
        if dataframe is None or len(dataframe) == 0:
            print(" No hay datos para cargar")
            return
        
        # PASO 4: Load
        print("\n FASE 3/3: Carga a Base de Datos")
        cargar_datos_crypto(dataframe)
        
        print("\n" + "="*60)
        print(" PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*60)
        print(f" Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
    except Exception as e:
        print(f" ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    ejecutar_pipeline()