import os
from sqlalchemy import create_engine, text
import func_global

from insertar_soportesotros_a_control_soporte.insertar_soportesotros_a_control_soporte_func import *

def insertar_soportes_ur(engine):
    """
    Obtiene los registros de la tabla listar.listar_ruta_compartida_depurada y, 
    antes de insertarlos en listar.control_soportes, filtra aquellos que ya están registrados
    (basándose en llave_unica). Sólo se insertan los registros nuevos usando la inserción masiva
    mediante una tabla temporal.
    """
    # Obtener los datos de la tabla fuente.
    datos_fuente = obtener_datos_fuente(engine)
    if not datos_fuente:
        print("No se obtuvieron datos de la tabla fuente. Terminando ejecución.")
        return

    # Obtener las llaves ya existentes en la tabla destino.
    llaves_existentes = obtener_llaves_existentes(engine)

    eliminar_soportes_obsoletos(engine,datos_fuente,llaves_existentes)

    datos_a_insertar = [row for row in datos_fuente if (row[0], row[3], row[4]) not in llaves_existentes]

    if not datos_a_insertar:
        print("Todos los registros ya existen en listar.control_soportes. No se insertará nada.")
    else:    
        # Insertar los registros nuevos utilizando la tabla temporal.
        insertar_datos_control(engine, datos_a_insertar)
    actualizar_otros_datos(engine)
        
    
    

    
    

    
    