from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text  # Importar text para consultas en crudo
import os
import subprocess
import fitz
import re
from datetime import datetime
from PyPDF2 import PdfReader



def convertir_ruta(ruta):
    """
    Convierte una ruta de Windows a Linux y viceversa automáticamente.
    
    - Si estás en Windows y la ruta es de Linux (/mnt/FACTURACION CAPRECOM2/), la convierte a V:/.
    - Si estás en Linux y la ruta es de Windows (V:/), la convierte a /mnt/FACTURACION CAPRECOM2/.
    """
    sistema_operativo = os.name  # 'nt' = Windows, 'posix' = Linux/Mac

    # Convertir de Linux a Windows
    if sistema_operativo == "nt":
        if ruta.startswith("/mnt/FACTURACION CAPRECOM2/"):
            ruta = ruta.replace("/mnt/FACTURACION CAPRECOM2/", "V:/")
            ruta = ruta.replace("/", "\\")  # Formato Windows
    
    # Convertir de Windows a Linux
    elif sistema_operativo == "posix":
        if ruta.startswith("V:\\") or ruta.startswith("V:/"):
            ruta = ruta.replace("V:\\", "/mnt/FACTURACION CAPRECOM2/").replace("V:/", "/mnt/FACTURACION CAPRECOM2/")
            ruta = ruta.replace("\\", "/")  # Formato Linux

    return ruta


from sqlalchemy.sql import text

from sqlalchemy.sql import text

def soporte_a_procesar(engine):
    """
    Obtiene todas las filas de listar.control_soportes donde:
    - `resultado_analisis_contenido` NO es 'VALIDACION EXITOSA'.
    - Al menos una de las siguientes columnas NO es 'EJECUTADO SIN NOVEDAD' o está NULL:
      - `resultado_analisis_contenido`
      - `convertido_parametros_resolucion`
      - `resultado_copia`
    """
    query = text("""
        SELECT * FROM listar.control_soportes 
        WHERE 
            (resultado_analisis_contenido IS NULL 
            OR resultado_analisis_contenido = '' 
            OR resultado_analisis_contenido NOT LIKE 'VALIDACION EXITOSA')
        AND 
            NOT (
                COALESCE(resultado_analisis_contenido, '') = 'EJECUTADO SIN NOVEDAD' 
                AND COALESCE(convertido_parametros_resolucion, '') = 'EJECUTADO SIN NOVEDAD' 
                AND COALESCE(resultado_copia, '') = 'EJECUTADO SIN NOVEDAD'
            );
    """)

    with engine.begin() as connection:
        result = connection.execute(query)
        registros = result.fetchall()
    
    return registros




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
    elif "Consulta de Protección Renal" in texto_pdf and "SaludTools" in texto_pdf:
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


def actualizar_resultados(engine, llave_unica, resultado_analisis_contenido, resultado_conversion_resolucion, resultado_copia):
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
                WHERE llave_unica = :llave
            """),
            {
                "resultado_analisis": resultado_analisis_contenido,
                "resultado_conversion": resultado_conversion_resolucion,  # Ajustado al nombre correcto
                "resultado_copia": resultado_copia,
                "llave": llave_unica
            }
        )
        
        # Confirmar cambios en la base de datos
        session.commit()
        print(f"✅ Se actualizaron correctamente los resultados para la llave_unica {llave_unica}")
    
    except Exception as e:
        session.rollback()  # Deshacer cambios en caso de error
        print(f"❌ Error al actualizar la base de datos: {str(e)}")
    
    finally:
        session.close()  # Cerrar la sesión



def verificar_pdf(ruta_soporte_destino,nombre_soporte):
    """Verifica si el archivo PDF existe y tiene al menos una página válida."""
    if not os.path.exists(ruta_soporte_destino):
        mensaje = f"RECHAZO: El soporte {nombre_soporte} no se copio"
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