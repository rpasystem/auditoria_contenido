import os
from sqlalchemy import create_engine, text
import func_global

def obtener_datos_fuente(engine):
    """
    Consulta la tabla listar.listar_ruta_compartida_depurada y retorna los registros con las columnas
    fecha_soporte, ruta_completa, nombre_soporte y llave_unica.
    """
    query = text("""
        SELECT fecha_soporte, ruta_completa, nombre_soporte, llave_unica
        FROM listar.listar_ruta_compartida_depurada
    """)
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            datos = result.fetchall()
        return datos
    except Exception as e:
        print(f"❌ Error al consultar la tabla fuente: {e}")
        func_global.enviar_correo_error(
            "Error en consulta de datos fuente", 
            "Error al consultar la tabla listar.listar_ruta_compartida_depurada", 
            error=str(e)
        )
        return []

def insertar_datos_control(engine, datos):
    """
    Utiliza una tabla temporal para insertar en bloque los datos en listar.control_soportes.
    
    Se crea una tabla temporal (temp_control_soportes) con las columnas:
      fecha_soporte, ruta_completa, nombre_soporte, llave_unica.
    Luego se realiza un INSERT ... SELECT en el que, utilizando la función
    string_to_array de PostgreSQL, se realiza el split de llave_unica para obtener:
      codigo_sede, llave_a, llave_b, cod_soporte, origen_sede y extramural.
    
    Los campos adicionales (resultado_analisis_contenido, convertido_parametros_resolucion,
    resultado_copia) se insertan como NULL.
    """
    try:
        with engine.begin() as connection:
            # 1. Crear la tabla temporal. Esta se eliminará al finalizar la transacción.
            connection.execute(text("""
                CREATE TEMP TABLE temp_control_soportes (
                    fecha_soporte DATE,
                    ruta_completa TEXT,
                    nombre_soporte TEXT,
                    llave_unica TEXT
                ) ON COMMIT DROP;
            """))
            
            # 2. Preparar los datos para la tabla temporal.
            # Se asume que 'datos' es una lista de tuplas con (fecha_soporte, ruta_completa, nombre_soporte, llave_unica)
            lista_temp = []
            for row in datos:
                fecha_soporte, ruta_completa, nombre_soporte, llave_unica = row
                lista_temp.append({
                    "fecha_soporte": fecha_soporte,
                    "ruta_completa": ruta_completa,
                    "nombre_soporte": nombre_soporte,
                    "llave_unica": llave_unica
                })
            
            # 3. Insertar en la tabla temporal en bloque.
            connection.execute(
                text("""
                    INSERT INTO temp_control_soportes (fecha_soporte, ruta_completa, nombre_soporte, llave_unica)
                    VALUES (:fecha_soporte, :ruta_completa, :nombre_soporte, :llave_unica)
                """),
                lista_temp
            )
            
            # 4. Insertar en la tabla final (listar.control_soportes) utilizando un INSERT ... SELECT
            # que realice el split de llave_unica usando string_to_array.
            connection.execute(text("""
                INSERT INTO listar.control_soportes (
                    fecha_soporte, ruta_completa, nombre_soporte, llave_unica, 
                    codigo_sede, llave_a, llave_b, cod_soporte, origen_sede, extramural, 
                    resultado_analisis_contenido, convertido_parametros_resolucion, resultado_copia
                )
                SELECT 
                    fecha_soporte,
                    ruta_completa,
                    nombre_soporte,
                    llave_unica,
                    (string_to_array(llave_unica, '-'))[1] AS codigo_sede,
                    (string_to_array(llave_unica, '-'))[2] AS llave_a,
                    (string_to_array(llave_unica, '-'))[3] AS llave_b,
                    (string_to_array(llave_unica, '-'))[4] AS cod_soporte,
                    (string_to_array(llave_unica, '-'))[5] AS origen_sede,
                    (string_to_array(llave_unica, '-'))[6] AS extramural,
                    NULL AS resultado_analisis_contenido,
                    NULL AS convertido_parametros_resolucion,
                    NULL AS resultado_copia
                FROM temp_control_soportes;
            """))
            
        print("✅ Datos insertados en listar.control_soportes exitosamente mediante tabla temporal.")
    except Exception as e:
        print(f"❌ Error al insertar datos mediante tabla temporal en listar.control_soportes: {e}")
        func_global.enviar_correo_error(
            "Error en inserción con tabla temporal", 
            "Error al insertar datos mediante tabla temporal en listar.control_soportes", 
            error=str(e)
        )



def obtener_llaves_existentes(engine):
    """
    Consulta la tabla listar.control_soportes y retorna un conjunto con los valores de llave_unica ya registrados.
    """
    query = text("SELECT llave_unica FROM listar.control_soportes")
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            llaves_existentes = {row[0] for row in result}
        return llaves_existentes
    except Exception as e:
        print(f"❌ Error al obtener llaves existentes: {e}")
        func_global.enviar_correo_error(
            "Error en consulta de llaves existentes", 
            "Error al consultar las llaves en listar.control_soportes", 
            error=str(e)
        )
        return set()
    

