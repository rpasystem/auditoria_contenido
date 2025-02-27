import time
import re
import traceback

from analisis_contenido_soporte.analisis_contenido_soporte_func import *
from analisis_contenido_soporte.conversion_resolucion import *

def analisis_contenido_soporte(engine,ruta_carpeta_destino,ruta_qpdf,ruta_copia_armado_cuenta_documento_fecha):    
    listar_soportes_en_carpeta_local = listar_archivos(ruta_carpeta_destino)
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

    total_archivos = len(registros_con_novedades)  # Obtener la cantidad total de archivos
    procesados = 0  # Contador de archivos procesados

    for fila in registros_con_novedades:
        try:

            # Información del progreso
            procesados += 1
            print(f"📂 Procesando archivo {procesados} de {total_archivos}... ({total_archivos - procesados} restantes)")
            
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
            fecha_modificacion = fila[6]
            unidad_renal = fila[7]            
            servicio = fila[8].upper() if fila[8] else ""
            cliente = fila[9].upper() if fila[9] else ""
            documento_paciente = fila[10]
                        
            nombre_archivo = "-".join(str(fila[i]) if fila[i] is not None else "N/A" for i in range(9, 14)) + extension_soporte_original
            
            
            ruta_soporte_destino = os.path.join(ruta_carpeta_destino, nombre_archivo_destino)
            if "FE178045-CUV.TXT" in ruta_soporte_destino:
                pass

            if servicio is None:
                continue

            print(f"Se inicia el analisis del archivo {nombre_archivo_destino}")

            resultado_analisis_contenido = None
            resultado_conversion_resolucion = None
            resultado_copia = None           

            
            if origen_soporte == "ADMON":
                ruta_origen_cuv_factura_xml_json = os.path.join(ruta_copia_armado_cuenta_documento_fecha,"Factura de Venta",llave_unica)
                
                if nombre_soporte == "FACTURA" or nombre_soporte == "ANEXO" :                                        
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = conversion_resolucion(ruta_soporte_original, ruta_soporte_destino, llave_unica)
                    resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte, extension_soporte_destino)
                
                elif nombre_soporte == "ATT":                    
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = "EJECUTADO SIN NOVEDAD"
                    copiar_archivo(ruta_soporte_original, ruta_soporte_destino)
                    resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte, extension_soporte_destino,)
                
                elif nombre_soporte == "CUV":                    
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = "EJECUTADO SIN NOVEDAD"
                    resultado_copia,ruta_soporte_destino  = descarga_cuv(engine, llave_unica,ruta_carpeta_destino,nombre_soporte)
                    

                elif nombre_soporte =="RIPS":
                    resultado_analisis_contenido = "EJECUTADO SIN NOVEDAD"
                    resultado_conversion_resolucion = "EJECUTADO SIN NOVEDAD"
                    resultado_copia = descarga_json(engine, llave_unica,ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
    
                else:
                    resultado_analisis_contenido = f"RECHAZO: El soporte {nombre_soporte} no está configurado"
                    ruta_soporte_destino = None
                    
            else:
                resultado_analisis_contenido = validaciones (sistema_operativo, ruta_soporte_original,
                                                            ruta_soporte_destino, nombre_soporte, ruta_qpdf,
                                                            servicio,documento_paciente,cliente)
                
                
                if "RECHAZO"  not in resultado_analisis_contenido:                    
                    resultado_conversion_resolucion = conversion_resolucion(ruta_soporte_original, ruta_soporte_destino, llave_unica)
                    resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
                else:
                    ruta_soporte_destino = None

                    if resultado_conversion_resolucion == "EJECUTADO SIN NOVEDAD":                        
                        resultado_copia = verificar_pdf(ruta_soporte_destino,nombre_soporte,extension_soporte_destino)
                
                
            actualizar_resultados(engine, nombre_archivo_destino, resultado_analisis_contenido, resultado_conversion_resolucion, resultado_copia,ruta_soporte_destino)
            
        except Exception as e:
            print(f"❌ Error en el procesamiento del archivo: {nombre_archivo}")
            print(f"🔹 Llave única: {llave_unica}")
            print(f"🔹 Ruta original: {ruta_soporte_original}")
            print(f"🔹 Ruta destino: {ruta_soporte_destino}")
            print(f"🔹 Detalles del error: {str(e)}")
            print("🔍 **Stack Trace**:")
            traceback.print_exc()  # Muestra la traza completa del error para depuración
            time.sleep(10)

    listar_soportes_en_bd = soportes_en_bd(engine)
    listar_soportes_en_carpeta_local = listar_archivos(ruta_carpeta_destino)
    
    ruta_reporte_comparativo = os.path.join(ruta_carpeta_destino,"..","REPORTE_COMPARATIVO")
    generar_reporte_comparativo(listar_soportes_en_bd, listar_soportes_en_carpeta_local, ruta_reporte_comparativo)

