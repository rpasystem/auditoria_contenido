# en func.py

import os
import time
import pandas as pd
import platform
import config
import func_global
from datetime import datetime
from pathlib import Path


from sqlalchemy import Table, MetaData, select, create_engine, text
from sqlalchemy.orm import sessionmaker

def obtener_ultimo_archivo(directorio):
    archivos = os.listdir(directorio)
    archivos_excel = [archivo for archivo in archivos if archivo.endswith('.xlsx')]

    def extraer_consecutivo(nombre_archivo):
        # Ejemplo de nombre de archivo: '125 - 2024 07 31 - 01 31 38 PM REPORTE AUDITORIA RESULTADO.xlsx'
        try:
            consecutivo_str = nombre_archivo.split(' - ')[0]
            return int(consecutivo_str)
        except (IndexError, ValueError):
            return -1  # Retornar un valor negativo para evitar que sea considerado como válido

    archivos_excel.sort(key=extraer_consecutivo, reverse=True)

    # Obtener el archivo con el consecutivo más alto y retornar la ruta completa
    if archivos_excel:
        archivo_reciente = archivos_excel[0]
        return os.path.join(directorio, archivo_reciente)
    else:
        return None

def obtener_fecha_modificacion(ruta_excel):
    timestamp = os.path.getmtime(ruta_excel)
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

def extrae_data_excel(ruta_excel, sheets_config):
    """
    Extrae solo dos DataFrames: 'Cargos Facturacion' y 'Indice'. Luego divide 'Indice' en varios DataFrames.
    """

    ruta_excel = Path(ruta_excel) if isinstance(ruta_excel, str) else ruta_excel
    if not ruta_excel.exists():
        raise FileNotFoundError(f"❌ El archivo no se encontró: {ruta_excel}")

    dataframes = {}

    for sheet_name, config in sheets_config.items():
        print(f"📄 Cargando la hoja '{sheet_name}'...")

        skip_rows = config["skip_rows"]
        usecols = config["usecols"]

        while True:  # 🔄 Intentar en bucle hasta que se pueda acceder al archivo
            try:
                df = pd.read_excel(
                    ruta_excel,
                    sheet_name=sheet_name,
                    skiprows=skip_rows - 1,  # 🔹 Esto hará que pandas lea desde la fila correcta
                    usecols=usecols,
                    header=0,  # 🔹 Ahora pandas usará la primera fila leída como nombres de columna
                    engine="openpyxl"
                )

                print(f"✅ Hoja '{sheet_name}' cargada con {len(df)} filas y {df.shape[1]} columnas.")
                dataframes[sheet_name] = df  # Guardar el DataFrame completo
                break  # 🔹 Salir del bucle si la lectura fue exitosa

            except PermissionError as e:
                error_msg = str(e)
                print(f"⚠️ Error de permiso: {error_msg}")
                
                # Enviar un correo de alerta
                func_global.enviar_correo_error(
                    asunto="🚨 Error: Permiso Denegado en Excel Base Auditoria",
                    mensaje="No se pudo acceder al archivo Excel. Verifica si está abierto y vuelve a intentarlo.",
                    error=error_msg
                )

                # Esperar 1 minuto antes de volver a intentar
                print("⏳ Esperando 1 minuto antes de volver a intentar...")
                time.sleep(60)  # 🔹 Espera 60 segundos antes de reintentar

    return dataframes if len(dataframes) > 1 else list(dataframes.values())[0]



def obtener_primer_valor(engine, tabla, columna, esquema="auditoria_soportes"):
    """
    Obtiene el primer valor de una columna específica en una tabla específica.

    :param engine: Conexión a la base de datos con SQLAlchemy
    :param tabla: Nombre de la tabla a consultar
    :param columna: Nombre de la columna de la que se extraerá el primer valor
    :param esquema: Esquema en el que se encuentra la tabla (por defecto: 'auditoria_soportes')
    :return: El primer valor encontrado o un mensaje si la tabla está vacía
    """

    # Asegurar que estás usando el mismo esquema
    metadata = MetaData(schema=esquema)

    # Definir la tabla dinámicamente
    try:
        tabla_bd = Table(tabla, metadata, autoload_with=engine)
    except Exception as e:
        print(f"❌ Error: No se pudo cargar la tabla '{esquema}.{tabla}'. Detalle: {e}")
        return None

    # Crear sesión con SQLAlchemy
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Consultar la primera fila de la columna especificada
        query = select(tabla_bd.c[columna]).limit(1)
        resultado = session.execute(query).fetchone()

        # Verificar si hay resultados
        if resultado:
            primer_valor = resultado[0]
            print(f"🔹 Primer valor de '{columna}' en '{esquema}.{tabla}': {primer_valor}")
            return primer_valor
        else:
            print(f"⚠️ La tabla '{esquema}.{tabla}' se encuentra vacía.")
            return None

    except Exception as e:
        print(f"❌ Error al consultar '{esquema}.{tabla}'. Detalle: {e}")
        return None

    finally:
        session.close()  # Cerrar la sesión


def construir_ruta_soportes(engine, ruta_facturacion_caprecom):
    """
    Construye la ruta de soportes usando la fecha de servicio.

    :param ruta_facturacion_caprecom: Ruta base donde están almacenados los soportes.
    :param fecha_servicio_base_auditoria: Objeto datetime.date (o datetime) de donde se extraerá el año y el mes.
    :return: Ruta completa hasta la carpeta del año y mes.
    """

    # Si llega como date (o datetime), no necesitas strptime()
    # Simplemente extrae anio y mes.
    # Si por alguna razón podría llegar como string, podrías chequear con isinstance().
    # Pero si siempre es un date/datetime, basta con lo siguiente:
    
    fecha_servicio_base_auditoria = obtener_primer_valor(engine, "base_auditoria", "fecha_archivo_facturacion")

    anio = str(fecha_servicio_base_auditoria.year)
    mes = str(fecha_servicio_base_auditoria.month)  # Se mantiene sin ceros a la izquierda

    # Construir la ruta final
    ruta_final = os.path.join(ruta_facturacion_caprecom, anio, mes)

    # Aquí podrías validar la ruta si quieres
    # func_global.validar_ruta(ruta_final)

    return ruta_final


def convertir_ruta(ruta_raiz):
    """
    Basado en la ruta raíz, devuelve la ruta equivalente del otro volumen en el mismo sistema operativo.

    - Si la ruta raíz es 'V:\\' en Windows, devuelve 'Y:\\'
    - Si la ruta raíz es 'Y:\\' en Windows, devuelve 'V:\\'
    - Si la ruta raíz es '/mnt/FACTURACION CAPRECOM2' en Linux, devuelve '/mnt/Y'
    - Si la ruta raíz es '/mnt/Y' en Linux, devuelve '/mnt/FACTURACION CAPRECOM2'

    Args:
        ruta_raiz (str): Ruta base en Windows o Linux.

    Returns:
        str: La ruta equivalente en el otro volumen del mismo sistema operativo.
    """
    sistema = platform.system()

    # Normalizar la ruta eliminando cualquier barra final innecesaria
    ruta_raiz = ruta_raiz.rstrip("/\\")

    # Definir el mapeo de rutas según el sistema operativo
    if sistema == "Windows":
        if ruta_raiz == "V:":
            return "Y:\\"
        elif ruta_raiz == "Y:":
            return "V:\\"
    
    elif sistema == "Linux":
        if ruta_raiz == "/mnt/FACTURACION CAPRECOM2":
            return "/mnt/Y"
        elif ruta_raiz == "/mnt/Y":
            return "/mnt/FACTURACION CAPRECOM2"

    return ruta_raiz  # Si no coincide con ninguna de las rutas conocidas, devolver la misma