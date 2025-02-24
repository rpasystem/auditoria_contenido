import time
import re
import traceback

from analisis_contenido_soporte.analisis_contenido_soporte_func import *
from analisis_contenido_soporte.conversion_resolucion import *

def analisis_contenido_soporte(engine,ruta_carpeta_destino,ruta_qpdf):
    
    listar_soportes_en_carpeta_local = listar_archivos(ruta_carpeta_destino)
    listar_soportes_en_bd = soportes_en_bd(engine)
    registros_con_novedades = soporte_a_procesar_con_novedades(engine)
    registros_sin_novedades = soporte_a_procesar_sin_novedades(engine)
    
    # Convertimos listar_soportes_en_carpeta_local en un set para búsquedas rápidas
    archivos_en_carpeta = set(listar_soportes_en_carpeta_local)

    # Filtrar los registros de registros_sin_novedades que no están en la carpeta local
    for registro in registros_sin_novedades:
        nombre_archivo_destino = registro[5]  # Columna con el nombre del archivo

        if nombre_archivo_destino not in archivos_en_carpeta:
            registros_con_novedades.append(registro)  # Agregar a registros_con_novedades

    # ✅ Ahora registros_con_novedades incluye los archivos faltantes en la carpeta local
    
    # Detectar el sistema operativo
    sistema_operativo = os.name  # 'nt' para Windows, 'posix' para Linux/Mac

    for fila in registros_con_novedades:
        try:
            
            origen_soporte = fila[1]             
            ruta_soporte_original = fila[2]
            ruta_soporte_original = convertir_ruta(ruta_soporte_original)    
            _, extension = os.path.splitext(ruta_soporte_original)               
            extension_soporte_original = extension.upper()
            nombre_soporte = fila[3]
            llave_unica =  fila[4]
            nombre_archivo_destino = fila[5]
            _, extension = os.path.splitext(nombre_archivo_destino)
            extension_soporte_destino = extension.upper()
            unidad_renal = fila[6]            
            servicio = fila[7].upper() if fila[7] else ""
            cliente = fila[8].upper() if fila[8] else ""
            documento_paciente = fila[9]
                        
            nombre_archivo = "-".join(str(fila[i]) if fila[i] is not None else "N/A" for i in range(9, 14)) + extension_soporte_original
            

            ruta_soporte_destino = os.path.join(ruta_carpeta_destino, nombre_archivo_destino)

            if servicio is None:
                continue

            print(f"Se inicia el analisis del archivo {nombre_archivo_destino}")

            resultado_analisis_contenido = None
            resultado_conversion_resolucion = None
            resultado_copia = None

            
            if origen_soporte == "ADMON":
                if nombre_soporte == "FACTURA" or nombre_soporte == "ANEXO" :                    
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = conversion_resolucion(ruta_soporte_original, ruta_soporte_destino, llave_unica)
                    resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte, extension_soporte_destino)
                
                elif nombre_soporte == "XML":                    
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = "EJECUTADO SIN NOVEDAD"
                    copiar_archivo(ruta_soporte_original, ruta_soporte_destino)
                    resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte, extension_soporte_destino,)
                
                elif nombre_soporte == "CUV":
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = "EJECUTADO SIN NOVEDAD"
                    resultado_copia = descarga_cuv(engine, llave_unica,ruta_soporte_destino,nombre_soporte,extension_soporte_destino)

                elif nombre_soporte =="JSON":
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = "EJECUTADO SIN NOVEDAD"
                    resultado_copia = descarga_json(engine, llave_unica,ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
    
                else:
                    resultado_analisis_contenido = f"RECHAZO: El soporte {nombre_soporte} no está configurado"
                    
            else:
                resultado_analisis_contenido = validaciones (sistema_operativo, ruta_soporte_original,
                                                            ruta_soporte_destino, nombre_soporte, ruta_qpdf,
                                                            servicio,documento_paciente,cliente)
                
                
                if "RECHAZO"  not in resultado_analisis_contenido:                    
                    resultado_conversion_resolucion = conversion_resolucion(ruta_soporte_original, ruta_soporte_destino, llave_unica)

                    if resultado_conversion_resolucion == "EJECUTADO SIN NOVEDAD":                        
                        resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
                
                
            actualizar_resultados(engine, nombre_archivo_destino, resultado_analisis_contenido, resultado_conversion_resolucion, resultado_copia)
            

        except Exception as e:
            print(f"❌ Error en el procesamiento del archivo: {nombre_archivo}")
            print(f"🔹 Llave única: {llave_unica}")
            print(f"🔹 Ruta original: {ruta_soporte_original}")
            print(f"🔹 Ruta destino: {ruta_soporte_destino}")
            print(f"🔹 Detalles del error: {str(e)}")
            print("🔍 **Stack Trace**:")
            traceback.print_exc()  # Muestra la traza completa del error para depuración
            time.sleep(10)

            

    