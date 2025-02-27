from sqlalchemy import create_engine, text
import func_global

from insertar_fac_xml_a_control_soporte.insertar_fac_xml_a_control_soporte_func import *

def insertar_soportes_admon(engine):
    # 1. Obtener la fecha_archivo_facturacion desde auditoria_soportes.base_auditoria.
    fecha_archivo_facturacion = obtener_fecha_archivo_facturacion(engine)
    if not fecha_archivo_facturacion:
        print("No se obtuvo fecha_archivo_facturacion. Terminando ejecución.")
        return
    
    
    # 2. Obtener los no_factura que comienzan con 'FE' de auditoria_soportes.base_auditoria.
    facturas_base_auditoria = obtener_facturas_base_auditoria(engine)
    if not facturas_base_auditoria:
        print("No se obtuvieron facturas base auditoria. Terminando ejecución.")
        return
    
    # 3. Buscar en soportes.documentos_descargados_api los documentos correspondientes a esas facturas,
    #    filtrando por estado_dian = 'Exitosa'.
    documentos = obtener_facturas_descargadas(engine, facturas_base_auditoria)
    if not documentos:
        print("No se encontraron documentos con estado 'Exitosa'. Terminando ejecución.")
        return
    
    relacion_facturas_con_cuv = facturas_con_cuv (engine)
    relacion_facturas_con_anexo = facturas_con_anexo (engine,facturas_base_auditoria)
    
    
    existentes = obtener_llaves_existentes_fac_xml(engine)    

    # 4. Insertar los datos obtenidos en listar.control_soportes_fac_xml.
    insertar_fac_y_xml(engine, documentos, fecha_archivo_facturacion, existentes)
    insertar_facturas_cuv(engine, relacion_facturas_con_cuv, fecha_archivo_facturacion, existentes)
    insertar_facturas_rips(engine, relacion_facturas_con_cuv, fecha_archivo_facturacion, existentes)
    insertar_facturas_anexo(engine, relacion_facturas_con_anexo, fecha_archivo_facturacion, existentes)
    
    # insertar_control_soportes_fac_xml(engine, relacion_facturas_con_cuv,relacion_facturas_con_anexo, documentos, fecha_archivo_facturacion,existentes)
    

