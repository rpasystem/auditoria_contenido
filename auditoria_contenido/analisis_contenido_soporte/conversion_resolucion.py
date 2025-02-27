import subprocess
import subprocess
import platform
import time

from analisis_contenido_soporte.conversion_resolucion_func import *

def conversion_resolucion(ruta_soporte_original, ruta_soporte_destino, llave_unica):
    """
    Convierte un archivo PDF a escala de grises y 300 DPI usando Ghostscript.
    Funciona tanto en Windows como en Linux.
    """

    # Validar si los metadatos cumplen con los requisitos
    metadatos = validar_metadatos(ruta_soporte_original)
    if metadatos:
        copiar_archivo(ruta_soporte_original, ruta_soporte_destino)
        return "EJECUTADO SIN NOVEDAD"

    # Detectar sistema operativo
    sistema_operativo = platform.system()

    # Definir comando de Ghostscript según el sistema operativo
    gs_executable = "gswin64c" if sistema_operativo == "Windows" else "gs"

    # Comando Ghostscript para convertir el archivo
    gs_command = [
    gs_executable,
    "-q",
    "-dNOPAUSE",
    "-sDEVICE=pdfwrite",
    "-sColorConversionStrategy=Gray",
    "-dProcessColorModel=/DeviceGray",
    "-dModifyMetadata",  # Habilita la modificación de metadatos
    f"-sOutputFile={ruta_soporte_destino}",
    ruta_soporte_original,
    "-c",
    "quit"
    ]

    try:
        # Ejecutar Ghostscript redirigiendo stdout y stderr para capturar mensajes de error
        result = subprocess.run(gs_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)                
        escribir_metadatos(ruta_soporte_destino, ruta_soporte_destino)
        return "EJECUTADO SIN NOVEDAD"

    except subprocess.CalledProcessError as e:
        # Capturar el mensaje de error de Ghostscript (stderr y stdout)
        mensaje_error = e.stderr.strip() if e.stderr else "Error desconocido"
        print(f"❌ Error al procesar {llave_unica}: {mensaje_error}")
        
        # También imprimimos stdout en caso de ser útil
        print("Salida:", e.stdout.strip() if e.stdout else "Sin salida")
        return f"ERROR: {mensaje_error}"

    except Exception as ex:
        # Capturar cualquier otro error inesperado
        print(f"⚠ Error inesperado: {ex}")
        return f"ERROR INESPERADO: {str(ex)}"