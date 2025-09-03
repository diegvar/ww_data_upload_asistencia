# WW Data Upload Asistencia - BigQuery Integration

API de FastAPI para sincronizar datos de ControlRoll con BigQuery en Google Cloud Platform.

## Características

- Consume API de ControlRoll
- Procesa y formatea datos usando pandas
- Sincroniza datos con BigQuery
- Despliegue en Cloud Run
- Filtrado de turnos configurable
- Formateo automático de fechas y campos numéricos

## Endpoints

- `GET /` - Health check
- `POST /sync-to-bigquery` - Sincroniza datos con BigQuery
- `GET /data-status` - Estado de los datos en BigQuery

## Despliegue en Cloud Run

### Opción 1: Despliegue automático desde GitHub (GitHub Actions)

Para habilitar el despliegue automático, puedes crear un workflow de GitHub Actions.
Necesitarás configurar estos secrets en tu repositorio:
- `GCP_SA_KEY`: Clave de cuenta de servicio de GCP (JSON)
- `TOKEN_CR`: Token de ControlRoll

### Opción 2: Despliegue manual desde la UI

1. **Subir código a GitHub**
2. **En Google Cloud Console:**
   - Ve a **Cloud Run**
   - Haz clic en **"Create Service"**
   - Selecciona **"Deploy one revision from an existing container image"**
   - Conecta tu repositorio de GitHub
   - Configura las variables de entorno

### Opción 3: Despliegue manual con Docker

```bash
# Construir imagen
docker build -t gcr.io/$PROJECT_ID/ww-data-upload-asistencia .

# Subir a Container Registry
docker push gcr.io/$PROJECT_ID/ww-data-upload-asistencia

# Desplegar en Cloud Run
gcloud run deploy ww-data-upload-asistencia \
    --image gcr.io/$PROJECT_ID/ww-data-upload-asistencia \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1Gi \
    --cpu 1 \
    --max-instances 10 \
    --timeout 300 \
    --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET_ID=ww_data_upload_asistencia,TABLE_ID=Asistencias_ControlRoll,TOKEN_CR=$TOKEN_CR
```

## Variables de Entorno

- `TOKEN_CR`: Token de autenticación para la API de ControlRoll
- `PROJECT_ID`: ID del proyecto de GCP
- `DATASET_ID`: ID del dataset de BigQuery
- `TABLE_ID`: ID de la tabla de BigQuery
- `PORT`: Puerto del servidor (por defecto 8080)

## Desarrollo Local

1. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

2. **Configurar variables de entorno:**
```bash
export TOKEN_CR="tu_token_aqui"
export PROJECT_ID="tu_proyecto_id"
```

3. **Ejecutar:**
```bash
python main.py
```

## Estructura del Proyecto

```
.
├── main.py                      # Código principal de la API
├── requirements.txt             # Dependencias de Python
├── Dockerfile                   # Configuración de Docker
├── .dockerignore                # Archivos a ignorar en Docker
├── .gcloudignore                # Archivos a ignorar en GCP
└── README.md                    # Documentación
```

## Configuración de BigQuery

La API crea automáticamente la tabla con el siguiente esquema:

- `identificador_rut`: STRING
- `Nombre`: STRING
- `apellido`: STRING
- `corresponde_turno`: BOOL
- `hora_entrada`: DATETIME
- `hora_salida`: DATETIME
- `Hora_marca_entrada`: DATETIME
- `Hora_marca_salida`: DATETIME
- `Marca_turno`: BOOL
- `Atraso_en_entrada`: BOOL
- `Empresa`: STRING
- `mail_empresa`: STRING
- `fecha_carga`: DATETIME
- `origen_datos`: STRING

## Notas Importantes

- La API filtra automáticamente turnos no deseados (configurable en `TURNOS_NO`)
- Los campos de fecha se formatean automáticamente
- El campo `HrTotRol` se convierte de string a float
- Los datos se reemplazan completamente en cada sincronización
- Timeout configurado a 300 segundos para operaciones largas
