import os
import subprocess
from PyPDF2 import PdfReader, PdfWriter
import openpyxl
from openpyxl import Workbook


def validar_metadatos(ruta_pdf):
    """
    Verifica si los metadatos del PDF cumplen con los requisitos esperados.
    """
    try:
        reader = PdfReader(ruta_pdf)
        metadatos = reader.metadata

        # Verificar los metadatos requeridos
        if (
            metadatos.get("/Title") == "Archivo convertido a escala de grises y 300 DPI" and
            metadatos.get("/Author") == "Davita" and
            metadatos.get("/Subject") == "Conversión con Ghostscript" and
            metadatos.get("/Keywords") == "Escala de grises, 300 DPI, PDF"
        ):
            return True  # Los metadatos cumplen con los requisitos
        else:
            return False  # Los metadatos no cumplen
    except Exception as e:
        print(f"Error al leer los metadatos de {ruta_pdf}: {e}")
        return False





# Las funciones escribir_metadatos y generar_informe_excel permanecen sin cambios

def escribir_metadatos(ruta_pdf, ruta_pdf_salida):    
    reader = PdfReader(ruta_pdf)
    writer = PdfWriter()

    # Copiar las páginas del archivo original
    writer.append_pages_from_reader(reader)

    # Modificar los metadatos
    writer.add_metadata({
        "/Title": "Archivo convertido a escala de grises y 300 DPI",
        "/Author": "Davita",
        "/Subject": "Conversión con Ghostscript",
        "/Keywords": "Escala de grises, 300 DPI, PDF"
    })

    # Guardar el archivo con los nuevos metadatos
    with open(ruta_pdf_salida, "wb") as salida:
        writer.write(salida)
    

def generar_informe_excel(errores, no_modificados, ruta_informe):
    # Crear un nuevo archivo Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "Resultados"

    # Escribir encabezados
    ws.append(["Archivo", "Estado", "Detalle"])

    # Escribir los errores
    for archivo, error in errores:
        ws.append([archivo, "Error", error])

    # Escribir los archivos no modificados
    for archivo in no_modificados:
        ws.append([archivo, "No modificado", "El archivo ya cumplía con los requisitos"])

    # Guardar el archivo Excel
    wb.save(ruta_informe)
