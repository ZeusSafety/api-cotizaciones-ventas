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
GCS_FOLDER = "historial_cotizacion"
UPLOAD_METHOD = "no_encriptar"
API_SUBIDA_URL = "https://api-subida-archivos-2946605267.us-central1.run.app"
API_TOKEN_VERIFY = "https://api-verificacion-token-2946605267.us-central1.run.app"

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


## Función de Subida a de archivos a GCS
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
    cursor.execute("SELECT COD_COTIZACION FROM historial_cotizacion ORDER BY ID_COTI DESC LIMIT 1")
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

        # --- NUEVA LÓGICA: Captura de datos adicionales ---
        nombre_cliente = data_source.get("nombre_cliente")
        monto_total = data_source.get("monto_total")
        # Estos vienen de los inputs del front
        cel = data_source.get("cel")
        ruc = data_source.get("ruc")
        dni = data_source.get("dni")
        
        # Lógica para producto interesado (enviado como string o json desde el front)
        # Suponiendo que el front envía el nombre del producto con más cantidad directamente
        producto_interesado = data_source.get("producto_interesado", "No especificado")

        if not nombre_cliente or not monto_total:
            return (json.dumps({"error": "Faltan datos obligatorios"}), 400, headers)

        conn = get_connection()
        cursor = conn.cursor()
        
        codigo_cotizacion = generar_codigo_cotizacion(cursor)
        
        ruta_pdf = ""
        if pdf_file:
            nombre_archivo = f"{codigo_cotizacion}.pdf"
            ruta_pdf = upload_to_external_api(pdf_file, nombre_archivo)

        # 3. Insertar en DB historial_cotizacion
        query = """
            INSERT INTO historial_cotizacion (
                FECHA_EMISION, COD_COTIZACION, NOMBRE_CLIENTE, REGION, 
                DISTRITO, ATENDIDO_POR, MONTO_TOTAL, RUTA_PDF, CAMPANIA, ESTADO
            ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, 'PENDIENTE')
        """
        cursor.execute(query, (
            codigo_cotizacion, nombre_cliente, data_source.get("region"),
            data_source.get("distrito"), data_source.get("atendido_por"),
            float(monto_total), ruta_pdf, data_source.get("campania")
        ))
        
        id_generado = cursor.lastrowid

        # --- NUEVA LÓGICA: Actualizar posibles_clientes con datos extra ---
        # El trigger ya creó el registro básico, aquí lo enriquecemos con los datos que no están en historial
        update_posible = """
            UPDATE posibles_clientes 
            SET CEL = %s, RUC = %s, DNI = %s, PRODUCTO_INTERESADO = %s
            WHERE ID_COTI_REF = %s
        """
        cursor.execute(update_posible, (cel, ruc, dni, producto_interesado, id_generado))
        
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
        
        # Datos capturados desde el Modal en el Front-end
        tipo_cliente = data.get("tipo_cliente")
        canal_origen = data.get("canal_origen")

        if not id_coti or not nuevo_estado:
            return (json.dumps({"error": "Faltan datos: id_coti o estado"}), 400, headers)

        conn = get_connection()
        cursor = conn.cursor()

        # 1. Obtener datos de la cotización y los datos vinculados del posible cliente
        cursor.execute("""
            SELECT h.*, p.CEL, p.RUC, p.DNI 
            FROM historial_cotizacion h
            LEFT JOIN posibles_clientes p ON h.ID_COTI = p.ID_COTI_REF
            WHERE h.ID_COTI = %s
        """, (id_coti,))
        cotizacion = cursor.fetchone()

        if not cotizacion:
            return (json.dumps({"error": "Cotización no encontrada"}), 404, headers)

        # 2. Lógica de Conversión a Cliente (Si se acepta la cotización)
        if nuevo_estado == 'ACEPTADO':
            # Verificamos si ya existe en clientes_ventas para no duplicar
            cursor.execute("SELECT 1 FROM clientes_ventas WHERE RUC = %s OR DNI = %s", 
                          (cotizacion.get('RUC'), cotizacion.get('DNI')))
            cliente_existe = cursor.fetchone()

            if not cliente_existe:
                # Si es cliente nuevo y NO tenemos datos del modal, solicitamos al front mostrarlo
                if not tipo_cliente or not canal_origen:
                    return (json.dumps({
                        "success": True,
                        "action": "SHOW_MODAL",
                        "message": "Cliente nuevo detectado. Se requieren datos adicionales."
                    }), 200, headers)
                
                # Si ya tenemos los datos del modal, insertamos al nuevo cliente oficial
                query_nuevo_cliente = """
                    INSERT INTO clientes_ventas (
                        FECHA, CLIENTE, TELEFONO, RUC, DNI, REGION, DISTRITO, TIPO_CLIENTE, CANAL_ORIGEN
                    ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query_nuevo_cliente, (
                    cotizacion['NOMBRE_CLIENTE'], cotizacion['CEL'], cotizacion['RUC'], 
                    cotizacion['DNI'], cotizacion['REGION'], cotizacion['DISTRITO'],
                    tipo_cliente, canal_origen
                ))

        # 3. Actualizar el estado en el Historial
        # Nota: El TRIGGER de la base de datos actualizará automáticamente 'posibles_clientes'
        cursor.execute("UPDATE historial_cotizacion SET ESTADO = %s WHERE ID_COTI = %s", (nuevo_estado, id_coti))
        
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

        cursor.execute("""
            SELECT ID_REGION, REGION
            FROM region
            ORDER BY REGION ASC
        """)

        data = cursor.fetchall()
        cursor.close()

        return (json.dumps({
            "success": True,
            "data": data
        }), 200, headers)

    except Exception as e:
        return (json.dumps({
            "success": False,
            "error": str(e)
        }), 500, headers)

    finally:
        if conn:
            conn.close()


# =========================================================
# GET - DISTRITOS POR REGIÓN
# =========================================================
def obtener_distritos_por_region_handler(request, headers):
    conn = None
    try:
        # Obtener id_region de los query parameters
        id_region = request.args.get("id_region")
        
        # Si no está en args, intentar desde el path completo
        if not id_region and hasattr(request, 'url'):
            from urllib.parse import urlparse, parse_qs
            parsed_url = urlparse(request.url)
            query_params = parse_qs(parsed_url.query)
            id_region = query_params.get("id_region", [None])[0]
        
        # Si aún no está, intentar desde el path directamente
        if not id_region and '?' in request.path:
            query_string = request.path.split('?')[1]
            params = dict(param.split('=') for param in query_string.split('&') if '=' in param)
            id_region = params.get("id_region")

        if not id_region:
            return (json.dumps({
                "success": False,
                "error": "El parámetro id_region es requerido"
            }), 400, headers)

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT ID_DISTRITO, DISTRITO
            FROM distrito
            WHERE ID_REGION = %s
            ORDER BY DISTRITO ASC
        """, (id_region,))

        data = cursor.fetchall()
        cursor.close()

        return (json.dumps({
            "success": True,
            "data": data
        }), 200, headers)

    except Exception as e:
        return (json.dumps({
            "success": False,
            "error": str(e)
        }), 500, headers)

    finally:
        if conn:
            conn.close()

# =========================================================
# GET - LISTAR COTIZACIONES
# =========================================================
def obtener_cotizaciones_handler(request, headers):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                ID_COTI,
                FECHA_EMISION,
                COD_COTIZACION,
                NOMBRE_CLIENTE,
                REGION,
                DISTRITO,
                ATENDIDO_POR,
                MONTO_TOTAL,
                RUTA_PDF,
                CAMPANIA,
                ESTADO
            FROM historial_cotizacion
            ORDER BY FECHA_EMISION DESC
        """
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()

        # CONVERSIÓN DE TIPOS PARA SERIALIZACIÓN JSON
        # Iteramos sobre cada fila (cotización) para convertir los tipos problemáticos
        for row in data:
            # 1. Convertir FECHA_EMISION (datetime) a string
            if 'FECHA_EMISION' in row and row['FECHA_EMISION']:
                # Formato de fecha y hora ISO 8601
                row['FECHA_EMISION'] = row['FECHA_EMISION'].isoformat() 
            
            # 2. Convertir MONTO_TOTAL (Decimal) a float
            if 'MONTO_TOTAL' in row and row['MONTO_TOTAL'] is not None:
                # El objeto Decimal se convierte a float para ser serializable por JSON
                row['MONTO_TOTAL'] = float(row['MONTO_TOTAL']) 

        return (json.dumps({
            "success": True,
            "data": data
        }), 200, headers)

    except Exception as e:
        # Manejo de errores
        return (json.dumps({
            "success": False,
            "error": "Error al obtener el listado de cotizaciones: " + str(e)
        }), 500, headers)

    finally:
        if conn:
            conn.close()

# =========================================================
# GET - LISTAR POSIBLES CLIENTES (CRM)
# =========================================================
def obtener_posibles_clientes_handler(request, headers):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Traemos datos de posibles_clientes y unimos con historial para ver el PDF y Monto
        query = """
            SELECT 
                p.ID_POSIBLE,
                p.ID_COTI_REF,
                p.COD_COTIZACION_REF,
                p.FECHA,
                p.NOMBRE_CLIENTE,
                p.CEL,
                p.RUC,
                p.DNI,
                p.REGION,
                p.DISTRITO,
                p.PRODUCTO_INTERESADO,
                p.CAMPANIA,
                p.OBSERVACIONES,
                p.ESTADO,
                h.RUTA_PDF,
                h.MONTO_TOTAL
            FROM posibles_clientes p
            LEFT JOIN historial_cotizacion h ON p.ID_COTI_REF = h.ID_COTI
            ORDER BY p.FECHA DESC
        """
        cursor.execute(query)
        data = cursor.fetchall()

        # Serialización de datos para JSON
        for row in data:
            if row.get('FECHA'):
                row['FECHA'] = row['FECHA'].isoformat()
            if row.get('MONTO_TOTAL'):
                row['MONTO_TOTAL'] = float(row['MONTO_TOTAL'])

        return (json.dumps({"success": True, "data": data}), 200, headers)

    except Exception as e:
        logging.error(f"Error en obtener_posibles_clientes: {e}")
        return (json.dumps({"success": False, "error": str(e)}), 500, headers)
    finally:
        if conn: conn.close()



API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

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

    try:
        # Obtener el token del header Authorization
        auth_header = request.headers.get("Authorization")
        
        # Validar que el token exista
        if not auth_header:
            return (json.dumps({"error": "Token no proporcionado"}), 401, headers)
        
        # Preparar headers para la verificación del token
        token_headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header
        }
        
        # Verificar el token con la API de autenticación
        try:
            response = requests.post(API_TOKEN, headers=token_headers, timeout=10)
            
            if response.status_code != 200:
                error_response = response.json() if response.text else {"error": f"Error desconocido en auth (Status: {response.status_code})"}
                error_msg = error_response.get("error", "Token no autorizado.")
                
                logging.warning(f"Token no autorizado: {error_msg}")
                return (json.dumps({"error": error_msg}), 401, headers)
                
        except requests.exceptions.RequestException as e:
            # Error de conexión o timeout
            logging.error(f"Error al verificar token: {str(e)}")
            return (json.dumps({"error": f"Error al verificar token: {str(e)}"}), 503, headers)
            
    except Exception as e:
        # Error general durante el proceso de auth/token
        logging.error(f"Error inesperado en auth: {str(e)}")
        return (json.dumps({"error": str(e)}), 500, headers)

    # Obtener el path sin query parameters
    path = request.path
    
    # Normalizar el path (eliminar barras finales y query parameters)
    if '?' in path:
        path = path.split('?')[0]  # Eliminar query parameters del path
    path = path.rstrip('/')
    
    # Si el path está vacío, es la raíz
    if not path:
        path = "/"

    # ---------- GET ----------
    if request.method == "GET":
        if path == "/":
            return (json.dumps({
                "success": True,
                "message": "API de Configuración de Marketing",
                "endpoints": {
                    "regiones": "/regiones",
                    "distritos": "/distritos?id_region=<id_region>",
                    "cotizacion": "/cotizacion (POST)",
                    "historial_cotizaciones": "/historial_cotizaciones (GET)",
                    "posibles_clientes": "/posibles_clientes (GET)"
                }
            }), 200, headers)

        elif path == "/regiones" or path.endswith("/regiones"):
            return obtener_regiones_handler(request, headers)

        elif path == "/distritos" or path.endswith("/distritos"):
            return obtener_distritos_por_region_handler(request, headers)
            
        elif path == "/historial_cotizaciones" or path.endswith("/historial_cotizaciones"):
            return obtener_cotizaciones_handler(request, headers)
        
        elif path == "/posibles_clientes" or path.endswith("/posibles_clientes"):
            return obtener_posibles_clientes_handler(request, headers)

        return (json.dumps({
            "success": False,
            "error": "Ruta GET no encontrada o no implementada"
        }), 404, headers)

    # ---------- POST ----------
    if request.method == "POST":
        if path == "/cotizacion" or path.endswith("/cotizacion"):
            return guardar_cotizacion_handler(request, headers)
        elif path == "/actualizar_estado_cotizacion" or path.endswith("/actualizar_estado_cotizacion"):
            return actualizar_estado_cotizacion_handler(request, headers)

        return (json.dumps({
            "success": False,
            "error": "Ruta POST no encontrada o no implementada"
        }), 404, headers)

    return (json.dumps({
        "success": False,
        "error": "Método o ruta no soportada"
    }), 405, headers)