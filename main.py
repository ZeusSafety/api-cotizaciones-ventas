import logging
import functions_framework
import pymysql
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.cloud import storage
import requests

# =========================================================
# CONFIGURACIÓN GLOBAL
# =========================================================
BUCKET_NAME = "archivos_sistema"
GCS_FOLDER = "cotizaciones_general"
UPLOAD_METHOD = "no_encriptar"
API_SUBIDA_URL = "https://api-subida-archivos-2946605267.us-central1.run.app"
API_TOKEN_VERIFY = "https://api-verificacion-token-2946605267.us-central1.run.app"
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

# =========================================================
# CONEXIÓN A MYSQL
# =========================================================
def get_connection():
    conn = pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        cursor.execute("SET time_zone = '-05:00'")
    return conn

# =========================================================
# FUNCIÓN DE SUBIDA DE ARCHIVOS A GCS
# =========================================================
def upload_to_external_api(file_stream, object_name):
    """
    Consume la API externa para subir el archivo.
    """
    params = {
        "bucket_name": BUCKET_NAME,
        "folder_bucket": GCS_FOLDER,
        "method": UPLOAD_METHOD
    }
    
    try:
        file_stream.seek(0)
        # El endpoint externo espera el archivo en la llave 'file'
        files = {'file': (object_name, file_stream, 'application/pdf')}
        
        logging.info(f"Enviando archivo {object_name} a API de subida...")
        response = requests.post(API_SUBIDA_URL, params=params, files=files, timeout=30)
        
        if response.status_code == 200:
            res_data = response.json()
            return res_data.get("url")
        else:
            logging.error(f"Error en API Subida: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error de conexión con API Subida: {e}")
        return None

# =========================================================
# GENERAR CÓDIGO DE COTIZACIÓN
# =========================================================
def generar_codigo_cotizacion(cursor):
    cursor.execute("SELECT COD_COTIZACION FROM cotizaciones_general ORDER BY ID_COTI DESC LIMIT 1")
    result = cursor.fetchone()
    nuevo = int(result['COD_COTIZACION'].split('-')[1]) + 1 if result else 1
    return f"C001-{str(nuevo).zfill(8)}"

# =========================================================
# POST - GUARDAR COTIZACIÓN
# =========================================================
def guardar_cotizacion_handler(request, headers):
    conn = None
    try:
        data_source = request.form
        pdf_file = request.files.get("pdf_file")

        nombre_cliente = data_source.get("nombre_cliente")
        monto_total = data_source.get("monto_total")
        
        if not nombre_cliente or not monto_total:
            return (json.dumps({"error": "Faltan datos obligatorios"}), 400, headers)

        conn = get_connection()
        cursor = conn.cursor()
        
        codigo_cotizacion = generar_codigo_cotizacion(cursor)
        
        ruta_pdf = ""
        if pdf_file:
            nombre_archivo = f"{codigo_cotizacion}.pdf"
            ruta_pdf = upload_to_external_api(pdf_file, nombre_archivo)

        # 3. Insertar en DB cotizaciones_general (Sin CAMPANIA)
        query = """
            INSERT INTO cotizaciones_general (
                FECHA_EMISION, COD_COTIZACION, NOMBRE_CLIENTE, REGION, 
                DISTRITO, ATENDIDO_POR, MONTO_TOTAL, RUTA_PDF, ESTADO
            ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, 'PENDIENTE')
        """
        cursor.execute(query, (
            codigo_cotizacion, nombre_cliente, data_source.get("region"),
            data_source.get("distrito"), data_source.get("atendido_por"),
            float(monto_total), ruta_pdf
        ))
        
        id_generado = cursor.lastrowid
        
        # --- Lógica de posibles_clientes ELIMINADA ---
        
        conn.commit()
        return (json.dumps({"success": True, "id": id_generado, "url": ruta_pdf}), 201, headers)

    except Exception as e:
        if conn: conn.rollback()
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        if conn: conn.close()

# =========================================================
# POST - ACTUALIZAR ESTADO DE COTIZACIÓN
# =========================================================
def actualizar_estado_cotizacion_handler(request, headers):
    conn = None
    try:
        data = request.get_json(silent=True) or request.form
        id_coti = data.get("id_coti") or data.get("ID_COTI")
        nuevo_estado = data.get("estado") or data.get("ESTADO")
        
        tipo_cliente = data.get("tipo_cliente")
        canal_origen = data.get("canal_origen")

        if not id_coti or not nuevo_estado:
            return (json.dumps({"error": "Faltan datos: id_coti o estado"}), 400, headers)

        conn = get_connection()
        cursor = conn.cursor()

        # 1. Obtener datos de la cotización (JOIN con posibles_clientes ELIMINADO)
        cursor.execute("""
            SELECT * FROM cotizaciones_general 
            WHERE ID_COTI = %s
        """, (id_coti,))
        cotizacion = cursor.fetchone()

        if not cotizacion:
            return (json.dumps({"error": "Cotización no encontrada"}), 404, headers)

        # 2. Lógica de Conversión a Cliente
        if nuevo_estado == 'ACEPTADO':
            cursor.execute("SELECT 1 FROM clientes_ventas WHERE CLIENTE = %s", (cotizacion['NOMBRE_CLIENTE'],))
            cliente_existe = cursor.fetchone()

            if not cliente_existe:
                if not tipo_cliente or not canal_origen:
                    return (json.dumps({
                        "success": True,
                        "action": "SHOW_MODAL",
                        "message": "Cliente nuevo detectado. Se requieren datos adicionales."
                    }), 200, headers)
                
                query_nuevo_cliente = """
                    INSERT INTO clientes_ventas (
                        FECHA, CLIENTE, REGION, DISTRITO, TIPO_CLIENTE, CANAL_ORIGEN
                    ) VALUES (NOW(), %s, %s, %s, %s, %s)
                """
                cursor.execute(query_nuevo_cliente, (
                    cotizacion['NOMBRE_CLIENTE'], cotizacion['REGION'], 
                    cotizacion['DISTRITO'], tipo_cliente, canal_origen
                ))

        # 3. Actualizar el estado en el Historial
        cursor.execute("UPDATE cotizaciones_general SET ESTADO = %s WHERE ID_COTI = %s", (nuevo_estado, id_coti))
        
        conn.commit()
        return (json.dumps({
            "success": True, 
            "message": f"Estado actualizado a {nuevo_estado} correctamente."
        }), 200, headers)

    except Exception as e:
        if conn: conn.rollback()
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        if conn: conn.close()

# =========================================================
# GET - REGIONES
# =========================================================
def obtener_regiones_handler(request, headers):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT ID_REGION, REGION FROM region ORDER BY REGION ASC")
        data = cursor.fetchall()
        cursor.close()
        return (json.dumps({"success": True, "data": data}), 200, headers)
    except Exception as e:
        return (json.dumps({"success": False, "error": str(e)}), 500, headers)
    finally:
        if conn: conn.close()

# =========================================================
# GET - DISTRITOS POR REGIÓN
# =========================================================
def obtener_distritos_por_region_handler(request, headers):
    conn = None
    try:
        id_region = request.args.get("id_region")
        if not id_region:
            return (json.dumps({"success": False, "error": "El parámetro id_region es requerido"}), 400, headers)

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT ID_DISTRITO, DISTRITO FROM distrito WHERE ID_REGION = %s ORDER BY DISTRITO ASC", (id_region,))
        data = cursor.fetchall()
        cursor.close()
        return (json.dumps({"success": True, "data": data}), 200, headers)
    except Exception as e:
        return (json.dumps({"success": False, "error": str(e)}), 500, headers)
    finally:
        if conn: conn.close()

# =========================================================
# GET - LISTAR COTIZACIONES
# =========================================================
def obtener_cotizaciones_handler(request, headers):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Se elimina CAMPANIA de la consulta
        query = """
            SELECT ID_COTI, FECHA_EMISION, COD_COTIZACION, NOMBRE_CLIENTE, 
                   REGION, DISTRITO, ATENDIDO_POR, MONTO_TOTAL, RUTA_PDF, ESTADO
            FROM cotizaciones_general
            ORDER BY FECHA_EMISION DESC
        """
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()

        for row in data:
            if row.get('FECHA_EMISION'):
                row['FECHA_EMISION'] = row['FECHA_EMISION'].isoformat() 
            if row.get('MONTO_TOTAL') is not None:
                row['MONTO_TOTAL'] = float(row['MONTO_TOTAL']) 

        return (json.dumps({"success": True, "data": data}), 200, headers)
    except Exception as e:
        return (json.dumps({"success": False, "error": str(e)}), 500, headers)
    finally:
        if conn: conn.close()

# =========================================================
# ROUTER PRINCIPAL
# =========================================================
@functions_framework.http
def ventasCotiza(request):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Content-Type": "application/json"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    # --- Verificación de Token ---
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "Token no proporcionado"}), 401, headers)
    
    try:
        token_headers = {"Content-Type": "application/json", "Authorization": auth_header}
        response = requests.post(API_TOKEN, headers=token_headers, timeout=10)
        if response.status_code != 200:
            return (json.dumps({"error": "Token no autorizado"}), 401, headers)
    except Exception as e:
        return (json.dumps({"error": f"Error auth: {str(e)}"}), 503, headers)

    # --- Enrutamiento ---
    path = request.path.rstrip('/')
    if not path or path == "": path = "/"

    if request.method == "GET":
        if path == "/":
            return (json.dumps({"success": True, "message": "API Ventas Cotizaciones"}), 200, headers)
        elif path.endswith("/regiones"):
            return obtener_regiones_handler(request, headers)
        elif path.endswith("/distritos"):
            return obtener_distritos_por_region_handler(request, headers)
        elif path.endswith("/historial_cotizaciones"):
            return obtener_cotizaciones_handler(request, headers)

    elif request.method == "POST":
        if path.endswith("/cotizacion"):
            return guardar_cotizacion_handler(request, headers)
        elif path.endswith("/actualizar_estado_cotizacion"):
            return actualizar_estado_cotizacion_handler(request, headers)

    return (json.dumps({"error": "Ruta no encontrada"}), 404, headers)