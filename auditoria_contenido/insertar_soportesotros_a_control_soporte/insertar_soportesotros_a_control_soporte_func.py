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

from sqlalchemy import text

def insertar_datos_control(engine, datos):
    """
    Inserta datos en listar.control_soportes utilizando una tabla temporal.
    """
    try:
        with engine.begin() as connection:
            # 1️⃣ Crear la tabla temporal con las columnas correctas
            connection.execute(text("""
                CREATE TEMP TABLE temp_control_soportes (
                    fecha_soporte DATE,
                    origen_soporte TEXT,
                    ruta_completa TEXT,
                    nombre_soporte TEXT,
                    llave_unica TEXT
                ) ON COMMIT DROP;
            """))
            
            # 2️⃣ Preparar los datos en una lista de diccionarios
            lista_temp = [
                {
                    "fecha_soporte": fecha_soporte,
                    "origen_soporte": "UNIDAD RENAL",  # Origen fijo en este caso
                    "ruta_completa": ruta_completa,
                    "nombre_soporte": nombre_soporte,
                    "llave_unica": llave_unica
                }
                for fecha_soporte, ruta_completa, nombre_soporte, llave_unica in datos
            ]
            
            # 3️⃣ Insertar los datos en la tabla temporal
            connection.execute(
                text("""
                    INSERT INTO temp_control_soportes (fecha_soporte, origen_soporte, ruta_completa, nombre_soporte, llave_unica)
                    VALUES (:fecha_soporte, :origen_soporte, :ruta_completa, :nombre_soporte, :llave_unica)
                """),
                lista_temp
            )
            
            # 4️⃣ Insertar en la tabla final con las columnas correctas
            connection.execute(text("""
                INSERT INTO listar.control_soportes (
                    fecha_soporte, origen_soporte, ruta_completa, nombre_soporte, llave_unica, 
                    unidad_renal, servicio, cliente, documento_paciente, 
                    codigo_sede, llave_a, llave_b, cod_soporte, origen_sede, extramural, 
                    resultado_analisis_contenido, convertido_parametros_resolucion, resultado_copia
                )
                SELECT 
                    fecha_soporte,
                    origen_soporte,
                    ruta_completa,
                    nombre_soporte,
                    llave_unica,
                    NULL AS unidad_renal,  -- No tenemos datos en temp_control_soportes
                    NULL AS servicio,
                    NULL AS cliente,
                    NULL AS documento_paciente,
                    COALESCE((string_to_array(llave_unica, '-'))[1], '') AS codigo_sede,
                    COALESCE((string_to_array(llave_unica, '-'))[2], '') AS llave_a,
                    COALESCE((string_to_array(llave_unica, '-'))[3], '') AS llave_b,
                    COALESCE((string_to_array(llave_unica, '-'))[4], '') AS cod_soporte,
                    COALESCE((string_to_array(llave_unica, '-'))[5], '') AS origen_sede,
                    COALESCE((string_to_array(llave_unica, '-'))[6], '') AS extramural,
                    NULL AS resultado_analisis_contenido,
                    NULL AS convertido_parametros_resolucion,
                    NULL AS resultado_copia
                FROM temp_control_soportes;
            """))
            
        print("✅ Datos insertados correctamente en listar.control_soportes usando una tabla temporal.")
    except Exception as e:
        print(f"❌ Error al insertar datos en listar.control_soportes: {e}")
        func_global.enviar_correo_error(
            "Error en inserción con tabla temporal", 
            "Error al insertar datos en listar.control_soportes", 
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
    


from sqlalchemy import text

def actualizar_otros_datos(engine):
    """
    Actualiza las columnas unidad_renal, servicio, cliente, documento_paciente en listar.control_soportes
    utilizando datos de auditoria_soportes.base_auditoria en PostgreSQL.
    """
    query = text("""
        UPDATE listar.control_soportes cs
        SET unidad_renal = ba.unidad_renal_depurada,
            servicio = ba.tipo_servicio,
            cliente = ba.cliente_depurado,
            documento_paciente = ba.cedula
        FROM auditoria_soportes.base_auditoria ba
        WHERE ba.id_cargo_depurado = CONCAT(cs.codigo_sede, '-',llave_a, '-', cs.llave_b)
        AND cs.origen_soporte = 'UNIDAD RENAL';
    """)

    try:
        with engine.begin() as connection:
            result = connection.execute(query)
        print("✅ Actualización de otros datos completada en PostgreSQL.")
    except Exception as e:
        print(f"❌ Error al actualizar listar.control_soportes: {e}")
