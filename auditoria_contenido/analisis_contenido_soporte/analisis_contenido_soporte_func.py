from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text  # Importar text para consultas en crudo
import os
import subprocess
import fitz
import re
import time
import json
import shutil
from datetime import datetime
from PyPDF2 import PdfReader

from tqdm import tqdm  # 🔹 Para mostrar progreso
from datetime import datetime
from pytz import timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# 🔹 Obtener la zona horaria del sistema (ajústala si es necesario)
local_tz = timezone("America/Bogota")  # Cambia esto según la zona horaria correcta

def convertir_ruta(ruta):
    """
    Convierte una ruta de Windows a Linux y viceversa automáticamente.
    
    - Si estás en Windows y la ruta es de Linux (/mnt/FACTURACION CAPRECOM2/ o /mnt/Y/), 
      la convierte a V:/ o Y:/ respectivamente.
    - Si estás en Linux y la ruta es de Windows (V:/ o Y:/), 
      la convierte a /mnt/FACTURACION CAPRECOM2/ o /mnt/Y/ respectivamente.
    """
    sistema_operativo = os.name  # 'nt' = Windows, 'posix' = Linux/Mac

    # Convertir de Linux a Windows
    if sistema_operativo == "nt":    
        if ruta.startswith("V:\\") or ruta.startswith("V:/"):
            ruta = ruta.replace("V:\\", "Y:\\").replace("V:/", "Y:/")        
        ruta = ruta.replace("\\", "/")  # Formato Linux    
    
    # Convertir de Windows a Linux
        if ruta.startswith("/mnt/FACTURACION CAPRECOM2/"):
            ruta = ruta.replace("/mnt/FACTURACION CAPRECOM2/", "V:/")
        elif ruta.startswith("/mnt/Y/"):
            ruta = ruta.replace("/mnt/Y/", "Y:/")
        ruta = ruta.replace("/", "\\")  # Formato Windows
    return ruta

def mapear_ruta(ruta):
    """
    Convierte una ruta de Windows a Linux y viceversa automáticamente.
    
    - Si estás en Windows y la ruta es de Linux (/mnt/FACTURACION CAPRECOM2/ o /mnt/Y/), 
      la convierte a V:/ o Y:/ respectivamente.
    - Si estás en Linux y la ruta es de Windows (V:/ o Y:/), 
      la convierte a /mnt/FACTURACION CAPRECOM2/ o /mnt/Y/ respectivamente.
    """
    sistema_operativo = os.name  # 'nt' = Windows, 'posix' = Linux/Mac

    # Convertir de Linux a Windows
    if sistema_operativo == "nt":    
        if ruta.startswith("V:\\") or ruta.startswith("V:/"):
            ruta = ruta.replace("V:\\", "Y:\\").replace("V:/", "Y:/")        
        
    
    # Convertir de Windows a Linux
        if ruta.startswith("/mnt/FACTURACION CAPRECOM2/"):
            ruta = ruta.replace("/mnt/FACTURACION CAPRECOM2/", "/mnt/Y/")        
    return ruta



from sqlalchemy.sql import text

from sqlalchemy.sql import text

from sqlalchemy.sql import text

def soporte_a_procesar_con_novedades(engine):
    """
    Obtiene todas las filas de listar.control_soportes donde:
    - `resultado_analisis_contenido` NO es 'EJECUTADO SIN NOVEDAD'.
    - Al menos una de las siguientes columnas NO es 'EJECUTADO SIN NOVEDAD' o está NULL:
      - `resultado_analisis_contenido`
      - `convertido_parametros_resolucion`
      - `resultado_copia`
    """
    query = text("""
        SELECT * FROM listar.control_soportes 
        WHERE 
            (resultado_analisis_contenido IS NULL 
            OR resultado_analisis_contenido = '')             
        OR 
            NOT (
                COALESCE(resultado_analisis_contenido, '') = 'EJECUTADO SIN NOVEDAD' 
                AND COALESCE(convertido_parametros_resolucion, '') = 'EJECUTADO SIN NOVEDAD' 
                AND COALESCE(resultado_copia, '') = 'EJECUTADO SIN NOVEDAD'
            )
    """)

    with engine.begin() as connection:
        result = connection.execute(query)
        registros = result.fetchall()
    
    return registros


def soporte_a_procesar_sin_novedades(engine):
    """
    Obtiene todas las filas de listar.control_soportes donde:
    - `resultado_analisis_contenido`, `convertido_parametros_resolucion` y `resultado_copia`
      son exactamente 'EJECUTADO SIN NOVEDAD' (sin NULL ni valores vacíos).
    """
    query = text("""
        SELECT * FROM listar.control_soportes 
        WHERE 
            COALESCE(resultado_analisis_contenido, '') = 'EJECUTADO SIN NOVEDAD' 
            AND COALESCE(convertido_parametros_resolucion, '') = 'EJECUTADO SIN NOVEDAD' 
            AND COALESCE(resultado_copia, '') = 'EJECUTADO SIN NOVEDAD';
    """)

    with engine.begin() as connection:
        result = connection.execute(query)
        registros = result.fetchall()
    
    return registros


    with engine.begin() as connection:
        result = connection.execute(query)
        registros = result.fetchall()
    
    return registros


def soportes_en_bd(engine):
    """
    Obtiene todas las filas de listar.control_soportes donde:
    - `resultado_analisis_contenido` NO es 'EJECUTADO SIN NOVEDAD'.
    - Al menos una de las siguientes columnas NO es 'EJECUTADO SIN NOVEDAD' o está NULL:
      - `resultado_analisis_contenido`
      - `convertido_parametros_resolucion`
      - `resultado_copia`
    """
    query = text("""
        SELECT nombre_archivo_destino FROM listar.control_soportes 
        WHERE resultado_analisis_contenido LIKE 'EJECUTADO SIN NOVEDAD'
        OR convertido_parametros_resolucion LIKE 'EJECUTADO SIN NOVEDAD'
        OR resultado_copia LIKE 'EJECUTADO SIN NOVEDAD';
    """)

    with engine.begin() as connection:
        result = connection.execute(query)
        return [fila[0] for fila in result.fetchall()]  # 🔹 Devuelve directamente una lista de strings



def validar_peso(ruta):
    try:
        tamaño = os.path.getsize(ruta)
        return "SI TIENE PESO" if tamaño > 0 else "NO TIENE PESO"
    except OSError:
        return "NO TIENE PESO"


def validar_corrupto(ruta,sistema_operativo):
    try:
        # Comandos según el sistema operativo
        if sistema_operativo == "nt":
            command = ['pdftk', ruta, 'cat', 'output', 'temp.pdf']
        else:  # Linux o Mac
            # Primero intentamos con 'pdftk'
            command = ['pdftk', ruta, 'cat', 'output', 'temp.pdf']

            # Si 'pdftk' no está disponible, usamos 'pdftk-java'
            if subprocess.run(['which', 'pdftk'], stdout=subprocess.PIPE).returncode != 0:
                command[0] = 'pdftk-java'  # Cambiar el comando

        # Ejecutar el comando
        archivo_valido = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Verificar código de salida
        if archivo_valido.returncode != 0:
            mensaje_error = archivo_valido.stderr.decode('utf-8')
            return "CORRUPTO"

        # Si se genera correctamente, el archivo es válido
        os.remove("temp.pdf")  # Borrar el archivo temporal generado
        return "NO CORRUPTO"
    
    except Exception as e:
        return "CORRUPTO"
    


def corregir_pdf(ruta_soporte_original, ruta_soporte_destino,ruta_qpdf,sistema_operativo):
    try:

        # Definir la ruta del ejecutable de qpdf según el sistema operativo
        if sistema_operativo == "nt":
            ruta_qpdf = ruta_qpdf
        else:  # Linux o Mac
            ruta_qpdf = "qpdf"  # En Linux, se asume que qpdf está en el PATH

        # Verificar si qpdf está disponible
        if subprocess.run(["which", ruta_qpdf], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode != 0:
            return f"ERROR: qpdf no encontrado en {sistema_operativo}"

        # Intentar corregir el archivo
        command = [ruta_qpdf, "--decrypt", ruta_soporte_original, ruta_soporte_destino]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Verificar si el comando fue exitoso
        if result.returncode == 0:
            print(f"✅ Archivo corregido: {ruta_soporte_destino}")
            return ruta_soporte_destino
        else:
            print(f"❌ ERROR al corregir el archivo {ruta_soporte_original}: {result.stderr.decode('utf-8')}")
            return "NO CORREGIDO"

    except Exception as e:
        print(f"⚠️ Excepción al intentar corregir el archivo {ruta_soporte_original}: {str(e)}")
        return "NO CORREGIDO"



def validar_escaneado(ruta):
    documento = fitz.open(ruta)
    texto_pagina = ""

    if len(documento) > 0:
        pagina = documento[0]
        texto_pagina = pagina.get_text()
        documento.close()
        if len(texto_pagina.strip()) > 0:
            return "NO ESCANEADO"
        else:
            return "ESCANEADO"
        
def extraer_texto_pdf(file):
    documento = fitz.open(file)
    texto_completo = ""

    for num_pagina in range(len(documento)):
        pagina = documento[num_pagina]
        texto_pagina = pagina.get_text()
        texto_completo += texto_pagina + "\n"  # Agregar un salto de línea entre páginas

    documento.close()
    return texto_completo


def formatear_fecha(fecha_str):
    """
    Toma una cadena de texto que contiene una fecha y la formatea como AAAA-MM.

    Args:
    - fecha_str (str): La cadena de texto que contiene la fecha.

    Returns:
    - str: La fecha formateada como AAAA-MM.
           Retorna None si no se puede parsear la fecha.
    """
    # Posibles formatos de fecha
    formatos = [
        "%d/%m/%Y %H:%M",    # 03/07/2024 09:04
        "%Y/%m/%d %H:%M",    # 2024/07/03 09:04
        "%d/%m/%Y %H:%M:%S", # 17/07/2024 14:43:50
        "%d/%m/%Y",          # 03/07/2024
        "%Y/%m/%d",          # 2024/07/03
    ]
    
    for formato in formatos:
        try:
            # Intentar parsear la fecha usando el formato actual
            fecha = datetime.strptime(fecha_str, formato)
            # Retornar la fecha formateada como AAAA-MM
            return fecha.strftime("%Y-%m")
        except ValueError:
            continue
    
    # Retornar None si ningún formato coincide
    return None


def extraer_texto_entre(texto, palabra_inicio, palabra_fin):
    """
    Extrae el texto entre las palabras `palabra_inicio` y `palabra_fin` en el `texto`.

    Args:
    - texto (str): El texto de donde se extraerá la información.
    - palabra_inicio (str): La palabra o frase que marca el inicio del texto a extraer.
    - palabra_fin (str): La palabra o frase que marca el fin del texto a extraer.

    Returns:
    - str: El texto encontrado entre `palabra_inicio` y `palabra_fin`. 
           Retorna None si no se encuentra el texto entre las palabras indicadas.
    """
    # Encontrar las posiciones de las palabras de inicio y fin
    inicio = texto.find(palabra_inicio)
    fin = texto.find(palabra_fin, inicio + len(palabra_inicio))

    # Verificar que ambas palabras se encuentren en el texto
    if inicio != -1 and fin != -1:
        # Calcular la posición real de inicio del texto a extraer
        inicio_real = inicio + len(palabra_inicio)
        # Extraer el texto entre las dos palabras
        return texto[inicio_real:fin].strip()
    else:
        return None
    
    import re


def extraer_numeros_inicio(texto, texto_inicial, cantidad_caracteres_buscar):
    """
    Extrae los números que se encuentran dentro de un rango de caracteres después de un texto inicial.

    Args:
    - texto (str): La cadena de texto en la que se buscarán los números.
    - texto_inicial (str): El texto a partir del cual se iniciará la búsqueda.
    - cantidad_caracteres_buscar (int): La cantidad de caracteres a considerar después del texto_inicial.

    Returns:
    - str: Los números encontrados dentro del rango especificado. Retorna una cadena vacía si no se encuentran números.
    """
    # Encontrar la posición inicial del texto inicial
    inicio = texto.find(texto_inicial)
    if inicio == -1:
        return ""  # Retorna cadena vacía si el texto inicial no se encuentra

    # Calcular la posición final del rango de búsqueda
    inicio += len(texto_inicial)
    fin = inicio + cantidad_caracteres_buscar

    # Extraer el segmento del texto dentro del rango especificado
    segmento = texto[inicio:fin]

    # Utilizar una expresión regular para encontrar números en el segmento
    numeros_encontrados = re.findall(r'\d+', segmento)

    # Unir los números encontrados y devolverlos como una cadena
    return "".join(numeros_encontrados)

def validar_documento_y_fecha(nombre_soporte, servicio, texto_pdf, documento_paciente,cliente,ruta_carpeta_destino):
    
    # Extraer año y mes usando regex
    match = re.search(r'(\d{4})\s(\d{2})', ruta_carpeta_destino)

    if match:
        fecha_a_validar = f"{match.group(1)}-{match.group(2)}"
    
    
    
    validacion_documento_paciente = "NO APLICA"
    resultado_documento_paciente = ""
    validacion_fecha = "NO APLICA"
    fecha_resultado = ""

    if "Advertencias de ejecución" in texto_pdf:
        validacion_documento_paciente = "ADVERTENCIA DE EJECUCION"
        validacion_fecha = "ADVERTENCIA DE EJECUCION"
    
    elif "HISTORIA CLÍNICA" in texto_pdf and "Nota Aclaratoria" in texto_pdf:
        palabra_inicio = "Paciente: "
        palabra_fin = "Tipo y número de identificación"
        resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if len(resultado_documento_paciente) < 1:
            validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
        elif len(resultado_documento_paciente) > 100:
            palabra_inicio = "Tipo y número de identificación"
            palabra_fin = "Paciente: "
            resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if len(resultado_documento_paciente) < 1:
            validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
        else:
            documento_resultado = re.findall(r'\d+', resultado_documento_paciente)
            if documento_resultado and documento_resultado[0] == documento_paciente:
                validacion_documento_paciente = "OK"
            else:
                validacion_documento_paciente = "ERRADA"
        
        palabra_inicio = "Fecha: "
        palabra_fin = " - Ambulatoria - Sede"
        
        resultado_fecha = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if resultado_fecha is None:
            palabra_inicio = "Fecha: "
            palabra_fin = " - Sede"
            resultado_fecha = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if resultado_fecha is None:
            validacion_fecha = "NO SE ENCONTRO LA CADENA PARA EXTRAER LA FECHA"            
        else:
            fecha_resultado = formatear_fecha(resultado_fecha)
            if fecha_resultado == fecha_a_validar:
                validacion_fecha = "OK"
            else:
                validacion_fecha = "ERRADA"

    elif "HISTORIA CLÍNICA" in texto_pdf and "NOTAS MÉDICAS" in texto_pdf:
        palabra_inicio = "Paciente: "
        palabra_fin = "Tipo y número de identificación"
        resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if len(resultado_documento_paciente) < 1:
            validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
        elif len(resultado_documento_paciente) > 100:
            palabra_inicio = "Tipo y número de identificación: "
            palabra_fin = "Paciente: "
            resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if len(resultado_documento_paciente) < 1:
            validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
        else:
            resultado_documento_paciente = resultado_documento_paciente.replace(" ","")
            documento_resultado = re.findall(r'\d+', resultado_documento_paciente)
            

            if documento_resultado and documento_resultado[0] == documento_paciente:
                validacion_documento_paciente = "OK"
            else:
                validacion_documento_paciente = "ERRADA"
        
        palabra_inicio = "Fecha: "
        palabra_fin = " - Ambulatoria - Sede"
        
        resultado_fecha = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if resultado_fecha is None:
            palabra_inicio = "Fecha: "
            palabra_fin = " - Sede"
            resultado_fecha = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if resultado_fecha is None:
            validacion_fecha = "NO SE ENCONTRO LA CADENA PARA EXTRAER LA FECHA"            
        else:
            fecha_resultado = formatear_fecha(resultado_fecha)
            if fecha_resultado == fecha_a_validar:
                validacion_fecha = "OK"
            else:
                validacion_fecha = "ERRADA"
    
    elif "INFORMACIÓN BÁSICA DEL PACIENTE" in texto_pdf and "INFORMACIÓN GENERAL" in texto_pdf:            
        palabra_inicio = "Grupo y RH:"
        palabra_fin = " Tipo y número de identificación:"
        resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if len(resultado_documento_paciente) < 1:
            validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
        else:
            documento_resultado = re.findall(r'\d+', resultado_documento_paciente)
            if documento_resultado and documento_resultado[0] == documento_paciente:
                validacion_documento_paciente = "OK"
            else:
                validacion_documento_paciente = "ERRADA"
        
        
        palabra_inicio = "INFORMACIÓN GENERAL "
        palabra_fin = " Fecha de la admisión:"
        resultado_fecha = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
        if resultado_fecha is None:
            validacion_fecha = "NO SE ENCONTRO LA CADENA PARA EXTRAER LA FECHA"
        else:
            fecha_resultado = resultado_fecha
            if fecha_a_validar in resultado_fecha:
                validacion_fecha = "OK"
            else:
                validacion_fecha = "ERRADA"
    
    
    # PARA CARECLOUD
    elif "Consulta de Protección Renal" in texto_pdf or "Consulta de Transplante Renal" in texto_pdf:
        if "SaludTools" in texto_pdf or "saludtools" in texto_pdf:
         
            "BUSCA EL DOCUMENTO DEL PACIENTE"
            palabra_inicio = "No. de Identificación:"
            palabra_fin = "Fecha de Nacimiento:"
            resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
            if len(resultado_documento_paciente) < 1:
                validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
            else:
                documento_resultado = re.findall(r'\d+', resultado_documento_paciente)
                if documento_resultado and documento_resultado[0] == documento_paciente:
                    validacion_documento_paciente = "OK"
                else:
                    validacion_documento_paciente = "ERRADA"
            
            
            ## BUSCA LA FECHA DE ATENCION"
            palabra_inicio = "Fecha y hora de inicio de la atención: "
            palabra_fin = " Nombre de consulta:"
            resultado_fecha = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
            if resultado_fecha is None:
                validacion_fecha = "NO SE ENCONTRO LA CADENA PARA EXTRAER LA FECHA"
            else:
                fecha_resultado = formatear_fecha(resultado_fecha)
                if fecha_resultado == fecha_a_validar:
                    validacion_fecha = "OK"
                else:
                    validacion_fecha = "ERRADA"

    
    elif "AUTORIZACION" in nombre_soporte and cliente == "COOSALUD":
        texto_inicial = "Teléfono"
        cantidad_caracteres_buscar = 20
        resultado_documento_paciente = extraer_numeros_inicio(texto_pdf, texto_inicial, cantidad_caracteres_buscar)
        if len(resultado_documento_paciente) < 1:
                validacion_documento_paciente = "NO SE ENCONTRO LA CADENA PARA EXTRAER EL DOCUMENTO"
        else:
            documento_resultado = re.findall(r'\d+', resultado_documento_paciente)
            if documento_resultado[0] in documento_paciente:
                validacion_documento_paciente = "OK"
            else:                
                palabra_inicio = "documento:"
                palabra_fin = " Estado"
                resultado_documento_paciente = extraer_texto_entre(texto_pdf, palabra_inicio, palabra_fin)
                documento_resultado = re.findall(r'\d+', resultado_documento_paciente)
                if documento_resultado[0] in documento_paciente:
                    validacion_documento_paciente = "OK"
                else:
                    validacion_documento_paciente = "ERRADA"       
    

    return validacion_documento_paciente, resultado_documento_paciente, validacion_fecha, fecha_resultado



def validaciones (sistema_operativo, ruta_soporte_original,ruta_soporte_destino, nombre_soporte, ruta_qpdf,servicio,documento_paciente,cliente):
    
    mensaje = "EJECUTADO SIN NOVEDAD"

    validacion_peso = validar_peso(ruta_soporte_original)
    if validacion_peso == "NO TIENE PESO":
        mensaje = f'RECHAZO: El soporte: {nombre_soporte} no tiene peso, revisar y volver a cargar'                
        return mensaje
        

    validacion_corrupto = validar_corrupto(ruta_soporte_original,sistema_operativo)
    if os.path.exists('temp.pdf'):
        os.remove('temp.pdf')
    
    if validacion_corrupto == "CORRUPTO":                                                            
        archivo_corregido = corregir_pdf(ruta_soporte_original, ruta_soporte_destino,ruta_qpdf,sistema_operativo)
        if archivo_corregido == "NO CORREGIDO":
            mensaje = f'RECHAZO: El soporte: {nombre_soporte} esta corrupto o no tiene los permisos necesario para modificarse, volver a descargarlo'
            return mensaje
        
    validacion_escaneado = validar_escaneado(ruta_soporte_original)

    if validacion_escaneado == "ESCANEADO":                
        mensaje = "EJECUTADO SIN NOVEDAD"

    else:
        texto_pdf_original = extraer_texto_pdf(ruta_soporte_original)
        texto_pdf = re.sub(r'\\n|[-]{2,}|\s+', ' ', texto_pdf_original).strip()
        
        validacion_documento_paciente, resultado_documento_paciente, validacion_fecha, fecha_resultado = validar_documento_y_fecha(
            nombre_soporte, servicio, texto_pdf, documento_paciente,cliente,ruta_soporte_destino)
        
        if validacion_documento_paciente == "ERRADA":
            mensaje = f'RECHAZO: El soporte: {nombre_soporte} es del paciente {resultado_documento_paciente} y se busca el del paciente {documento_paciente}'
        
        elif validacion_fecha == "ERRADA":
            mensaje = f'NOTIFICACION: El soporte {nombre_soporte} tiene una fecha de prestacion del servicio de {fecha_resultado}'                
                
        elif validacion_documento_paciente == "ADVERTENCIA DE EJECUCION":
            mensaje = f'NOTIFICACION: El soporte {nombre_soporte} tiene una inconsistencia en el contenido'                
        
    return mensaje


def actualizar_resultados(engine, nombre_archivo_destino, resultado_analisis_contenido, resultado_conversion_resolucion, resultado_copia):
    """
    Actualiza las columnas en la tabla 'listar.control_soportes' usando la llave única.
    """
    # Crear una sesión con SQLAlchemy    
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Actualizar con el nombre correcto de la columna
        session.execute(
            text("""
                UPDATE listar.control_soportes
                SET resultado_analisis_contenido = :resultado_analisis,
                    convertido_parametros_resolucion = :resultado_conversion,  -- Nombre correcto
                    resultado_copia = :resultado_copia
                WHERE nombre_archivo_destino = :llave
            """),
            {
                "resultado_analisis": resultado_analisis_contenido,
                "resultado_conversion": resultado_conversion_resolucion,  # Ajustado al nombre correcto
                "resultado_copia": resultado_copia,
                "llave": nombre_archivo_destino
            }
        )
        
        # Confirmar cambios en la base de datos
        session.commit()
        print(f"✅ Se actualizaron correctamente los resultados para la llave_unica {nombre_archivo_destino}")
    
    except Exception as e:
        session.rollback()  # Deshacer cambios en caso de error
        print(f"❌ Error al actualizar la base de datos: {str(e)}")
    
    finally:
        session.close()  # Cerrar la sesión



def verificar_pdf(ruta_soporte_destino,nombre_soporte,extension_soporte_destino):
    """Verifica si el archivo PDF existe y tiene al menos una página válida."""
    if not os.path.exists(ruta_soporte_destino):
        mensaje = f"RECHAZO: El soporte {nombre_soporte} no se copio"
        return mensaje
    if "PDF" not in  extension_soporte_destino:
        mensaje = "EJECUTADO SIN NOVEDAD"
        return mensaje

    try:
        
        reader = PdfReader(ruta_soporte_destino)
        if len(reader.pages) > 0:
            mensaje = "EJECUTADO SIN NOVEDAD"
            return mensaje
        else:
            mensaje = f"RECHAZO: El soporte {nombre_soporte} está vacio"
            return mensaje
    except Exception as e:
        mensaje = f"Error al abrir el soporte {nombre_soporte}"
        return mensaje
    

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def listar_archivos(ruta_base):
    """
    Lista únicamente los nombres de los archivos en la ruta proporcionada
    usando threading para mejorar la velocidad.
    """
    archivos = []

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(obtener_nombres_archivos, root, files) for root, _, files in os.walk(ruta_base)]
        
        for future in as_completed(futures):
            archivos.extend(future.result())

    return archivos

def obtener_nombres_archivos(root, files):
    """ Devuelve solo los nombres de los archivos en un directorio. """
    return [file for file in files]


from datetime import datetime

def crear_archivo_cuv(ruta_soporte_destino, documento, fecha_radicacion, cuv):
    """
    Crea un archivo TXT en la ruta especificada con la información de la factura, 
    la fecha de radicación formateada y el CUV.
    """
    # Formatear la fecha de radicación a 'YYYY-MM-DD HH:MM:SS'
    fecha_radicacion_formateada = fecha_radicacion.strftime('%Y-%m-%d %H:%M:%S')

    contenido = f"""No. Factura : {documento}
Fecha Radicación: {fecha_radicacion_formateada}
CUV: {cuv}
    """

    try:
        with open(ruta_soporte_destino, "w", encoding="utf-8") as archivo:
            archivo.write(contenido)
        print(f"✅ Archivo TXT creado correctamente en: {ruta_soporte_destino}")
    except Exception as e:
        print(f"❌ Error al crear el archivo TXT: {e}")



def descarga_cuv(engine, llave_unica,ruta_carpeta_destino,nombre_soporte):
    """
    Consulta la tabla `api.validacion_respuesta_principal` para obtener el estado de descarga,
    la ruta de descarga y la fecha de modificación donde `num_factura` coincida.
    """
    query = text("""
        SELECT estado_descarga, ruta_descarga_cuv, fecha_modificacion
        FROM api.validacion_respuesta_principal
        WHERE num_factura = :num_factura
        AND result_state IS TRUE
    """)

    with engine.begin() as connection:
        result = connection.execute(query, {"num_factura": llave_unica})  # Coincidencia exacta
        registros = result.fetchall()
    
    resultado = None  # Inicializar variable resultado

    for registro in registros:
        estado_descarga = registro[0]   # Estado de descarga
        ruta_descarga_cuv = registro[1] # Ruta del archivo CUV
        fecha_modificacion = registro[2]  # Fecha de modificación del archivo

        ruta_archivo = convertir_ruta_bidireccional(ruta_descarga_cuv)
        
        # Obtener el nombre del archivo desde la ruta original
        nombre_archivo = os.path.basename(ruta_archivo)

        _, extension = os.path.splitext(ruta_archivo)
        

        # Construir la ruta completa en la carpeta destino
        ruta_destino = os.path.join(ruta_carpeta_destino, nombre_archivo)

        # Copiar el archivo
        shutil.copy(ruta_archivo, ruta_destino)
        
        
        # Verificar si el archivo PDF se generó correctamente
        resultado = verificar_pdf(ruta_destino, nombre_archivo, extension)
    
    return resultado  # Retornar el último resultado encontrado (o None si no hubo registros)



# def descarga_cuv(engine, num_factura,ruta_soporte_destino,nombre_soporte,extension_soporte_destino):
#     """
#     Consulta la tabla `api.validacion_rips` para obtener el documento, CUV y fecha de radicación
#     donde `documento` coincida exactamente con `num_factura`.
#     """
#     query = text("""
#         SELECT documento, cuv, fecha_radicacion 
#         FROM api.validacion_rips
#         WHERE documento = :num_factura
#     """)

#     with engine.begin() as connection:
#         result = connection.execute(query, {"num_factura": num_factura})  # Coincidencia exacta
#         registros = result.fetchall()
    
#     for registro in registros:
#         documento = registro[0]  # Primer elemento: documento
#         cuv = registro[1]        # Segundo elemento: cuv (hash o identificador)
#         fecha_radicacion = registro[2]  # Tercer elemento: fecha de radicación (datetime)
#         crear_archivo_cuv(ruta_soporte_destino, documento, fecha_radicacion, cuv)
#         resultado = verificar_pdf(ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
        

#     return resultado


def descarga_json(engine, num_factura,ruta_soporte_destino,nombre_soporte,extension_soporte_destino):
    """
    Consulta la tabla `api.validacion_rips` para obtener el documento, CUV y fecha de radicación
    donde `documento` coincida exactamente con `num_factura`.
    """
    query = text("""
        SELECT data_json  -- 🔹 Aquí estaba el error, antes decía "daja_json"
        FROM xml.data_json
        WHERE nombre_archivo = :num_factura
    """)

    with engine.begin() as connection:
        result = connection.execute(query, {"num_factura": num_factura})
        json_completo = result.fetchall()
    
        exportar_data_json(ruta_soporte_destino, json_completo)
        resultado = verificar_pdf(ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
    
    return resultado


def exportar_data_json(ruta_soporte_destino, json_completo):
    """
    Exporta el contenido de json_completo a un archivo JSON en la ruta especificada.
    """
    try:
        # Extraer el diccionario de la estructura (json_completo es una lista con una tupla dentro)
        data_json_content = json_completo[0][0] if json_completo else {}

        # Escribir el JSON en el archivo
        with open(ruta_soporte_destino, 'w', encoding='utf-8') as f:
            json.dump(data_json_content, f, ensure_ascii=False, indent=4)

        print(f"✅ Archivo JSON exportado: {ruta_soporte_destino}")
        return True
    except Exception as e:
        print(f"❌ Error al exportar archivo JSON: {e}")
        return False


import os
import platform

import os
import platform

def convertir_ruta_bidireccional(ruta):
    """
    Convierte una ruta entre Windows y Linux dependiendo del sistema operativo en el que se ejecuta.

    - En Windows: Convierte rutas de Linux (`/mnt/Y/`) a Windows (`Y:\`).
    - En Linux: Convierte rutas de Windows (`Y:\`) a Linux (`/mnt/Y/`).

    Args:
        ruta (str): Ruta en cualquier formato (Windows o Linux).

    Returns:
        str: Ruta convertida al formato del sistema operativo en el que se ejecuta.
    """
    sistema = platform.system()

    # 🔹 Convertir de Linux a Windows si el código corre en Windows
    if sistema == "Windows" and ruta.startswith("/mnt/Y/"):
        ruta_convertida = ruta.replace("/mnt/Y/", "Y:\\")
        return ruta_convertida.replace("/", "\\")  # Convertir separadores a Windows

    # 🔹 Convertir de Windows a Linux si el código corre en Linux
    elif sistema == "Linux" and ruta.startswith("Y:\\"):
        ruta_convertida = ruta.replace("Y:\\", "/mnt/Y/")
        return ruta_convertida.replace("\\", "/")  # Convertir separadores a Linux

    # 🔹 Si ya está en el formato correcto, devolver la ruta sin cambios
    return ruta





