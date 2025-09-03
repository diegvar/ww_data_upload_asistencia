from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from google.cloud import bigquery
import json
from datetime import datetime
from typing import Optional
import pandas as pd
import os

app = FastAPI()

# Configuración
API_LOCAL_URL = "https://cl.controlroll.com/ww01/ServiceUrl.aspx"
PROJECT_ID = "pruebas-463316"
DATASET_ID = "ww_data_upload_asistencia"
TABLE_ID = "Asistencias_ControlRoll"
TOKEN = os.getenv("TOKEN_CR")

# Lista de turnos a excluir (ajusta según tus necesidades)
TURNOS_NO = []  # Agrega aquí los turnos que quieres excluir

# Inicializar cliente de BigQuery
client = bigquery.Client(project=PROJECT_ID)

def process_and_format_data(data_json):
    """
    Procesa y formatea los datos antes de cargarlos en BigQuery
    """
    try:
        # Convertir a DataFrame
        df = pd.DataFrame(data_json)
        
        # Filtrar turnos no deseados
        if TURNOS_NO:
            df = df.loc[~df['Turno'].isin(TURNOS_NO)]
        
        # Formatear campo HrTotRol (reemplazar coma por punto y convertir a float)
        if 'HrTotRol' in df.columns:
            df['HrTotRol'] = df['HrTotRol'].str.replace(',', '.')
            df['HrTotRol'] = df['HrTotRol'].astype(float)
        
        # Convertir campos de fecha
        date_columns = ['Her', 'Hsr', 'FlogAsi','FechaMarcaEntrada','FechaMarcaSalida']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d-%m-%Y %H:%M:%S')
        
        

        df['corresponde_turno'] = df['corresponde_turno'].astype(bool)
        df['Marca_turno'] = df['Marca_turno'].astype(bool)
        df['Atraso_en_entrada'] = df['Atraso_en_entrada'].astype(bool)

        #Renombrar Columnas
        df.rename(columns={'RutRol': 'rut_rol'
        , 'NombreRol': 'nombre_rol'
        , 'Instalación Rol': 'instalacion_rol'
        , 'Her': 'hora_ingreso_oficial'
        , 'FlogAsi': 'ult_log_marca_asistencia'
        , 'UlogAsi': 'metodo_usuario_marcaje'
        , 'Cliente Rol': 'cliente_rol'
        , 'Tipo de Turno': 'tipo_turno'
        , 'Turno': 'cod_turno'
        , 'Hsr': 'hora_salida_oficial'
        , 'HrTotRol': 'horas_totales_turno'
        , 'Hr. Tot. Asi.': 'horas_totales_asistencia'
        , 'FechaMarcaEntrada': 'fecha_marca_entrada'
        , 'FechaMarcaSalida': 'fecha_marca_salida'
        }
        , inplace=True)

        # Convertir DataFrame de vuelta a lista de diccionarios
        processed_data = df.to_dict('records')
        
        print(f"Datos procesados: {len(processed_data)} registros después del filtrado")
        return processed_data
        
    except Exception as e:
        print(f"Error al procesar datos: {str(e)}")
        raise e

def create_table_if_not_exists():
    """
    Crea la tabla si no existe
    """
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Verificar si la tabla existe
        try:
            client.get_table(table_ref)
            print(f"Tabla {table_ref} ya existe")
            return True
        except Exception:
            print(f"Tabla {table_ref} no existe, creándola...")
        
        # Definir esquema de la tabla
        schema = [
        bigquery.SchemaField("rut_rol", "INTEGER"),
        bigquery.SchemaField("nombre_rol", "STRING"),
        bigquery.SchemaField("instalacion_rol", "STRING"),
        bigquery.SchemaField("hora_ingreso_oficial", "DATETIME"),
        bigquery.SchemaField("ult_log_marca_asistencia", "DATETIME"),
        bigquery.SchemaField("metodo_usuario_marcaje", "STRING"),
        bigquery.SchemaField("cliente_rol", "STRING"),
        bigquery.SchemaField("tipo_turno", "STRING"),
        bigquery.SchemaField("cod_turno", "STRING"),
        bigquery.SchemaField("hora_salida_oficial", "DATETIME"),
        bigquery.SchemaField("horas_totales_turno", "INTEGER"),
        bigquery.SchemaField("horas_totales_asistencia", "INTEGER"),
        bigquery.SchemaField("fecha_carga", "DATETIME"),
        bigquery.SchemaField("origen_datos", "STRING"),
        bigquery.SchemaField("fecha_marca_entrada", "DATETIME"),
        bigquery.SchemaField("fecha_marca_salida", "DATETIME")
    ]
        
        # Crear tabla
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        
        print(f"Tabla {table_ref} creada exitosamente")
        return True
        
    except Exception as e:
        print(f"Error al crear tabla: {str(e)}")
        return False

def replace_table_data(data):
    """
    Reemplaza todos los datos de la tabla
    """
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Eliminar todos los datos existentes
        delete_query = f"DELETE FROM `{table_ref}` WHERE 1=1"
        client.query(delete_query).result()
        print("Datos anteriores eliminados")
        
        # Insertar nuevos datos
        errors = client.insert_rows_json(table_ref, data)
        
        if errors:
            raise Exception(f"Error al insertar datos: {errors}")
        
        print(f"Datos reemplazados exitosamente: {len(data)} registros")
        return True
        
    except Exception as e:
        print(f"Error al reemplazar datos: {str(e)}")
        return False

@app.get("/")
def health_check():
    """Endpoint de salud para verificar que la API funciona"""
    return {"status": "healthy", "message": "API funcionando correctamente"}

@app.post("/sync-to-bigquery")
def sync_to_bigquery(
    empresa: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None
):
    """
    Consume la API local y reemplaza los datos en BigQuery
    """
    try:
        # Preparar parámetros para la API local
        params = {}
        headers = {
            "method": "report",
            "token": TOKEN
        }

        # Llamar a la API local
        print(f"Llamando a API local: {API_LOCAL_URL}")
        response = requests.get(API_LOCAL_URL, headers=headers)
        response.raise_for_status()
        
        # Obtener datos como texto y convertirlos a JSON
        data_text = response.text
        data_json = json.loads(data_text)
        print(f"Datos obtenidos: {len(data_json)} registros")

        if not data_json:
            return JSONResponse(content={
                "status": "success",
                "message": "No hay datos para cargar",
                "rows_inserted": 0
            })

        # Procesar y formatear datos
        print("Procesando y formateando datos...")
        processed_data = process_and_format_data(data_json)
        
        # Crear tabla si no existe
        if not create_table_if_not_exists():
            raise HTTPException(
                status_code=500,
                detail="Error al crear/verificar la tabla"
            )

        # Agregar timestamp de carga
        for row in processed_data:
            row['fecha_carga'] = datetime.now().isoformat()
            row['origen_datos'] = 'ControlRoll'

        # Reemplazar datos en BigQuery
        if not replace_table_data(processed_data):
            raise HTTPException(
                status_code=500,
                detail="Error al reemplazar datos en BigQuery"
            )

        return JSONResponse(content={
            "status": "success",
            "message": "Datos procesados y reemplazados exitosamente en BigQuery",
            "rows_inserted": len(processed_data),
            "table": f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
            "original_records": len(data_json),
            "processed_records": len(processed_data)
        })

    except requests.RequestException as e:
        raise HTTPException(
            status_code=502, 
            detail=f"Error al conectar con la API local: {str(e)}"
        )
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al parsear JSON de la respuesta: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error inesperado: {str(e)}"
        )

@app.get("/data-status")
def get_data_status():
    """
    Verifica el estado de los datos en BigQuery
    """
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Verificar si la tabla existe
        try:
            client.get_table(table_ref)
        except Exception:
            return JSONResponse(content={
                "table": table_ref,
                "status": "table_not_exists",
                "message": "La tabla no existe aún"
            })
        
        # Consulta para contar registros
        query = f"""
        SELECT 
            COUNT(*) as total_registros,
            MAX(fecha_carga) as ultima_carga,
            COUNT(DISTINCT empresa) as empresas_unicas
        FROM `{table_ref}`
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            return JSONResponse(content={
                "table": table_ref,
                "status": "table_exists",
                "total_registros": row.total_registros,
                "ultima_carga": row.ultima_carga,
                "empresas_unicas": row.empresas_unicas
            })
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error al consultar BigQuery: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    # Para Cloud Run, usar puerto 8080
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
