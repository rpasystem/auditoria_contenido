import time
import re
import traceback

from analisis_contenido_soporte.analisis_contenido_soporte_func import *
from analisis_contenido_soporte.conversion_resolucion import *

def analisis_contenido_soporte(engine,ruta_carpeta_destino,ruta_qpdf):
    
    registros = soporte_a_procesar(engine)
    
    # Detectar el sistema operativo
    sistema_operativo = os.name  # 'nt' para Windows, 'posix' para Linux/Mac

    resultados = []
    
    for fila in registros:
        try:
            
            origen_soporte = fila[1]             
            ruta_soporte_original = fila[2]
            ruta_soporte_original = convertir_ruta(ruta_soporte_original)    
            _, extension = os.path.splitext(ruta_soporte_original)               
            extension = extension.upper()
            nombre_soporte = fila[3]
            llave_unica =  fila[4]
            unidad_renal = fila[5]            
            servicio = fila[6].upper() if fila[6] else ""
            cliente = fila[7].upper() if fila[6] else ""
            documento_paciente = fila[8]
            nombre_archivo = "-".join(str(fila[i]) if fila[i] is not None else "N/A" for i in range(9, 14)) + extension
            

            ruta_soporte_destino = os.path.join(ruta_carpeta_destino, nombre_archivo)

            if servicio is None:
                continue

            print(f"Se inicia el analisis del archivo {nombre_archivo}")

            resultado_analisis_contenido = "PTE INICIAR EL PROCESO"
            resultado_conversion_resolucion = "PTE INICIAR EL PROCESO"
            resultado_copia = "PTE INICIAR EL PROCESO"

            if origen_soporte == "UNIDAD RENAL":
                resultado_analisis_contenido = validaciones (sistema_operativo, ruta_soporte_original,
                                                            ruta_soporte_destino, nombre_soporte, ruta_qpdf,
                                                            servicio,documento_paciente,cliente)
                
                
                if "RECHAZO"  in resultado_analisis_contenido:
                    continue

                resultado_conversion_resolucion = conversion_resolucion(ruta_soporte_original, ruta_soporte_destino, llave_unica)
                actualizar_resultados(engine, llave_unica, resultado_analisis_contenido, resultado_conversion_resolucion, resultado_copia)
            
            else:
                continue

        except Exception as e:
            print(f"❌ Error en el procesamiento del archivo: {nombre_archivo}")
            print(f"🔹 Llave única: {llave_unica}")
            print(f"🔹 Ruta original: {ruta_soporte_original}")
            print(f"🔹 Ruta destino: {ruta_soporte_destino}")
            print(f"🔹 Detalles del error: {str(e)}")
            print("🔍 **Stack Trace**:")
            traceback.print_exc()  # Muestra la traza completa del error para depuración
            time.sleep(10)

            

    