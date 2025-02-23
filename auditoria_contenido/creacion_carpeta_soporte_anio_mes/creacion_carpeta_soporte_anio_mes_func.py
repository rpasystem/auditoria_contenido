import os
from datetime import datetime, date
from sqlalchemy import create_engine, text
import func_global

def obtener_fechas_unicas(engine):
    """
    Consulta la tabla listar.listar_ruta_compartida_depurada y retorna los valores únicos 
    de la columna 'fecha_soporte'.
    
    :param engine: Motor de conexión a la base de datos configurado con SQLAlchemy.
    :return: Lista de diccionarios con la clave 'fecha_soporte'
    """
    query = text("SELECT DISTINCT fecha_soporte FROM listar.listar_ruta_compartida_depurada")
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            # Se extrae la fecha (se asume que es la primera columna de cada fila)
            fechas = [row[0] for row in result]
        return [{"fecha_soporte": fecha} for fecha in fechas]
    except Exception as e:
        print(f"❌ Error al consultar la tabla: {e}")
        func_global.enviar_correo_error("Error en consulta", "Error al consultar las fechas únicas", error=str(e))
        return []

def crear_carpetas_soportes(data, base_path):
    """
    Recorre los registros recibidos y, para cada fecha extraída, crea una carpeta
    en la ruta base, utilizando el año y mes. Por ejemplo, para '2025-01-01'
    crea la carpeta base_path/2025/01.
    
    :param data: Lista de diccionarios con la clave 'fecha_soporte'
    :param base_path: Ruta base donde se crearán las carpetas (compatible con Windows y Linux)
    """
    # Extraer las fechas únicas
    fechas = {registro["fecha_soporte"] for registro in data}
    
    for fecha in fechas:
        try:
            # Si la fecha es de tipo datetime.date, la usamos directamente, 
            # de lo contrario, la convertimos (en caso de que sea string)
            if isinstance(fecha, date):
                dt = fecha
            elif isinstance(fecha, str):
                dt = datetime.strptime(fecha, "%Y-%m-%d")
            else:
                raise ValueError("Tipo de dato de fecha desconocido")
            
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
            year_month = f'{year} {month}'
            carpeta_destino = os.path.join(base_path, year_month)
            os.makedirs(carpeta_destino, exist_ok=True)
            print(f"✅ Carpeta creada: {carpeta_destino}")
            return carpeta_destino
        except Exception as e:
            print(f"❌ Error al procesar la fecha {fecha}: {e}")
            func_global.enviar_correo_error("Error al crear carpeta", f"Error al procesar la fecha {fecha}", error=str(e))
