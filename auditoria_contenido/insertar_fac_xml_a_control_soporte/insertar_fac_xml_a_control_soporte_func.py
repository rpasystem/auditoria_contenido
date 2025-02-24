from sqlalchemy import create_engine, text
import func_global

def obtener_fecha_archivo_facturacion(engine):
    """
    Obtiene el primer valor de la columna fecha_archivo_facturacion de la tabla auditoria_soportes.base_auditoria.
    Se asume que ese valor será utilizado para la inserción en listar.control_soportes_fac_xml.
    """
    query = text("SELECT fecha_archivo_facturacion FROM auditoria_soportes.base_auditoria LIMIT 1")
    try:
        with engine.connect() as connection:
            fecha = connection.execute(query).scalar()
        return fecha
    except Exception as e:
        print(f"❌ Error al obtener fecha_archivo_facturacion: {e}")
        func_global.enviar_correo_error("Error en auditoria", 
                                         "Error al obtener fecha_archivo_facturacion", 
                                         error=str(e))
        return None

def obtener_facturas_base_auditoria(engine):
    """
    Obtiene los valores únicos de la columna no_factura de auditoria_soportes.base_auditoria 
    que comienzan con 'FE'. Estos valores se alojarán en la variable factura_base_auditoria.
    """
    query = text("SELECT DISTINCT no_factura FROM auditoria_soportes.base_auditoria WHERE no_factura LIKE 'FE%'")
    try:
        with engine.connect() as connection:
            facturas = [row[0] for row in connection.execute(query)]
        return facturas
    except Exception as e:
        print(f"❌ Error al obtener facturas base auditoria: {e}")
        func_global.enviar_correo_error("Error en auditoria", 
                                         "Error al obtener facturas base auditoria", 
                                         error=str(e))
        return []

def obtener_documentos_descargados(engine, facturas):
    """
    Busca en soportes.documentos_descargados_api los documentos cuya columna documento esté en la lista de facturas
    y que tengan estado_dian = 'Exitosa'. Retorna una lista de diccionarios con las columnas:
    documento, formato y ruta.
    """
    query = text("""
        SELECT documento, formato, ruta 
        FROM soportes.documentos_descargados_api
        WHERE documento = ANY(:facturas) AND estado_dian = 'Exitosa'
    """)
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"facturas": facturas})
            # Utilizamos row._mapping para obtener un diccionario con los nombres de columna.
            documentos = [dict(row._mapping) for row in result]
        return documentos
    except Exception as e:
        print(f"❌ Error al obtener documentos descargados: {e}")
        func_global.enviar_correo_error("Error en documentos", 
                                         "Error al obtener documentos descargados", 
                                         error=str(e))
        return []


from sqlalchemy import text
import func_global

def obtener_llaves_existentes_fac_xml(engine):
    """
    Consulta la tabla listar.control_soportes_fac_xml y retorna un conjunto de tuplas
    (llave_unica, nombre_soporte) que ya se encuentran insertadas.
    """
    query = text("SELECT llave_unica, nombre_soporte FROM listar.control_soportes")
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            # Usamos _mapping para obtener un diccionario con las claves de las columnas.
            existentes = {(row._mapping["llave_unica"], row._mapping["nombre_soporte"]) for row in result}
        return existentes
    except Exception as e:
        print(f"❌ Error al obtener llaves existentes en control_soportes_fac_xml: {e}")
        func_global.enviar_correo_error("Error en consulta", 
                                         "Error al obtener llaves existentes en control_soportes_fac_xml", 
                                         error=str(e))
        return set()

def insertar_control_soportes_fac_xml(engine, relacion_facturas_con_cuv, relacion_facturas_con_anexo, documentos, fecha_archivo_facturacion, existentes):
    """
    Inserta en listar.control_soportes los documentos obtenidos desde 
    soportes.documentos_descargados_api.
    """

    insert_query = text("""
        INSERT INTO listar.control_soportes
        (fecha_soporte, origen_soporte, ruta_completa, nombre_soporte, llave_unica, cod_soporte, 
         resultado_analisis_contenido, convertido_parametros_resolucion, resultado_copia)
        VALUES
        (:fecha_soporte, :origen_soporte, :ruta_completa, :nombre_soporte, :llave_unica, :cod_soporte, 
         :resultado_analisis_contenido, :convertido_parametros_resolucion, :resultado_copia)
    """)
    
    registros = []

    # 🔹 1️⃣ Procesa los documentos obtenidos
    for doc in documentos:
        documento = doc.get("documento")
        formato = doc.get("formato")
        ruta = doc.get("ruta") or ""  # Manejo de valores nulos

        # Determinar nombre_soporte según el formato.
        if formato.lower() == 'pdf':
            nombre_soporte = "FACTURA"
        elif formato.lower() == 'xml':
            nombre_soporte = "XML"
        else:
            nombre_soporte = formato  # O asignar otro valor predeterminado
        
        # Formar la llave compuesta.
        llave_compuesta = (documento, nombre_soporte)
        if llave_compuesta in existentes:
            continue
        
        # Agregar el registro a la lista de inserción.
        registros.append({
            "fecha_soporte": fecha_archivo_facturacion,
            "origen_soporte": "ADMON",
            "ruta_completa": ruta,
            "nombre_soporte": nombre_soporte,
            "llave_unica": documento,
            "cod_soporte": "1",
            "resultado_analisis_contenido": None,
            "convertido_parametros_resolucion": None,
            "resultado_copia": None
        })
        existentes.add(llave_compuesta)

    # 🔹 2️⃣ Ahora procesamos las facturas que tienen CUV y ANEXO
    for documento in relacion_facturas_con_cuv:
        if (documento, "CUV") not in existentes:
            registros.append({
                "fecha_soporte": fecha_archivo_facturacion,
                "origen_soporte": "ADMON",
                "ruta_completa": "RUTA INTERNA",  # No hay ruta para CUV
                "nombre_soporte": "CUV",
                "llave_unica": documento,
                "cod_soporte": "1",
                "resultado_analisis_contenido": None,
                "convertido_parametros_resolucion": None,
                "resultado_copia": None
            })
            existentes.add((documento, "CUV"))
    
    # 🔹 2️⃣ Ahora procesamos las facturas que tienen CUV y ANEXO
    for documento in relacion_facturas_con_cuv:
        if (documento, "JSON") not in existentes:
            registros.append({
                "fecha_soporte": fecha_archivo_facturacion,
                "origen_soporte": "ADMON",
                "ruta_completa": "RUTA INTERNA",  # No hay ruta para CUV
                "nombre_soporte": "JSON",
                "llave_unica": documento,
                "cod_soporte": "1",
                "resultado_analisis_contenido": None,
                "convertido_parametros_resolucion": None,
                "resultado_copia": None
            })
            existentes.add((documento, "JSON"))

    for documento, ruta_anexo in relacion_facturas_con_anexo.items():
        if (documento, "ANEXO") not in existentes:
            registros.append({
                "fecha_soporte": fecha_archivo_facturacion,
                "origen_soporte": "ADMON",
                "ruta_completa": ruta_anexo,  
                "nombre_soporte": "ANEXO",
                "llave_unica": documento,
                "cod_soporte": "1",
                "resultado_analisis_contenido": None,
                "convertido_parametros_resolucion": None,
                "resultado_copia": None
            })
            existentes.add((documento, "ANEXO"))

    # 🔹 3️⃣ Verificar si hay registros nuevos antes de insertar
    if not registros:
        print("No hay registros nuevos para insertar en listar.control_soportes.")
        return

    # 🔹 4️⃣ Intentar la inserción en la base de datos
    try:
        with engine.begin() as connection:
            connection.execute(insert_query, registros)
        print("✅ Datos insertados en listar.control_soportes exitosamente.")
        existentes.clear()  # Limpieza de memoria después de la inserción
    except Exception as e:
        print(f"❌ Error al insertar en control_soportes: {e}")
        func_global.enviar_correo_error(
            "Error en inserción", 
            "Error al insertar en listar.control_soportes", 
            error=str(e)
        )


def facturas_con_anexo(engine,facturas_base_auditoria):
    """
    Consulta la base de datos para obtener las facturas que tienen 'ANEXO' en sop_admon_pte.
    """
    query = text("""
        SELECT factura, ruta_completa
        FROM listar.listar_ruta_compartida_depurada_anexo        
    """)

    # query = text("""
    #     SELECT factura 
    #     FROM auditoria_soportes.reporte_auditoria
    #     WHERE sop_admon_completos LIKE '%ANEXO%'
    # """)

    with engine.connect() as connection:
        result = connection.execute(query)
        listado_facturas_con_anexo = {factura: ruta_completa for factura, ruta_completa in result.fetchall()}
        

    listado_facturas_con_anexo_que_se_necesitan = {factura: ruta for factura, ruta in listado_facturas_con_anexo.items() if factura in facturas_base_auditoria}
    return listado_facturas_con_anexo_que_se_necesitan

def facturas_con_cuv(engine):
    """
    Consulta la base de datos para obtener las facturas que tienen 'CUV' en sop_admon_pte.
    """
    query = text("""
        SELECT factura 
        FROM auditoria_soportes.reporte_auditoria
        WHERE sop_admon_completos LIKE '%CUV%'
    """)

    with engine.connect() as connection:
        result = connection.execute(query)
        listado_facturas_con_cuv = {factura[0] for factura in result.fetchall()}  # Retorna un conjunto con las facturas
        return listado_facturas_con_cuv
