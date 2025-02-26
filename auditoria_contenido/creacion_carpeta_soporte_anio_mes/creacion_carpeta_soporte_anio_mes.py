from creacion_carpeta_soporte_anio_mes.creacion_carpeta_soporte_anio_mes_func import *

def creacion_carpeta_soporte_anio_mes(engine,ruta_carpeta_soportes):    
    # Obtener los valores únicos de fecha_soporte desde la tabla
    data = obtener_fechas_unicas(engine)

    # Extraer el año y mes de la fecha
    year = str(data[0]['fecha_soporte'].year)
    month = str(data[0]['fecha_soporte'].month)
    
    # Crear las carpetas correspondientes según los valores obtenidos
    carpeta_destino = crear_carpetas_soportes(data, ruta_carpeta_soportes)
    return carpeta_destino, year, month


