# En módulo bd_main.py

import os
import sys
import platform
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# 📌 Agregar ruta de herramientas al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'herramientas')))

# 📌 Importaciones internas (después de configurar sys.path)
from func import *  
import func_global

# IMPORTACION MODULOS
from creacion_carpeta_soporte_anio_mes.creacion_carpeta_soporte_anio_mes import *
from insertar_soportesotros_a_control_soporte.insertar_soportesotros_a_control_soporte import *
from insertar_fac_xml_a_control_soporte.insertar_fac_xml_a_control_soporte import *
from analisis_contenido_soporte.analisis_contenido_soporte import *


#Conexion a BD
engine = func_global.crear_conexion_bd('rips')

# 🔹 Ruta base
ruta_base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
ruta_base_superior_dos_niveles = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
ruta_base_superior_tres_niveles = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..','..'))

# RUTAS CARPETAS
ruta_facturacion_caprecom = func_global.obtener_ruta_soportes(ruta_base)
ruta_archivo_plantilla_reporte_auditoria = os.path.join(ruta_base_superior_dos_niveles, "archivos_excel", "plantilla_reporte_auditoria","plantilla_reporte_auditoria.xlsx")
ruta_carpeta_local_reporte_auditoria = os.path.join(ruta_base_superior_dos_niveles, "archivos_excel", "reporte_auditoria")
ruta_carpeta_analisis_reporte_auditoria = os.path.join(ruta_base_superior_dos_niveles, "archivos_excel", "reporte_analisis")
ruta_carpeta_soportes = os.path.join(ruta_base_superior_tres_niveles, "SOPORTES")
ruta_qpdf = os.path.join(ruta_base_superior_dos_niveles, "herramientas", "QPDF", "qpdf.exe")


sistema_operativo = platform.system()    

def main():
    carpeta_soporte = creacion_carpeta_soporte_anio_mes(engine,ruta_carpeta_soportes)
    insertar_soportesotros_a_control_soporte(engine)
    insertar_fac_xml_a_control_soporte(engine)
    analisis_contenido_soporte (engine,carpeta_soporte,ruta_qpdf)
    
    engine.dispose()

    
if __name__ == "__main__":    
    # while True:
        try:
            print ("                                    ")
            print ("######## INICIA PROCESO ###########")
            inicio_main, _ = func_global.registrar_inicio_proceso()
            main()
            print ("¡¡¡¡¡¡¡ FINALIZA PROCESO !!!!!!!!")
            func_global.registrar_tiempo_fin(inicio_main)
        except Exception as e:
            print(f"Ocurrió un error: {e}")  # Registra el error            
        
        tiempo_actual = datetime.now()
        tiempo = timedelta(minutes=5)
        proxima_auditoria = tiempo_actual + tiempo
        print(f"La próxima auditoría se realizará a las {proxima_auditoria.strftime('%H:%M:%S')}")
        time.sleep(tiempo.total_seconds())  # Espera de 15 minutos entre ejecuciones
