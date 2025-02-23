from sqlalchemy import create_engine, MetaData, Table, Column, String, Date

# Definir la metadata y esquema
metadata = MetaData(schema='listar')

control_soportes_fac_xml = Table(
    'control_soportes_fac_xml', metadata,
    Column('fecha_soporte', Date),
    Column('ruta_completa', String),
    Column('nombre_soporte', String, primary_key=True),  # Parte de la llave compuesta
    Column('llave_unica', String, primary_key=True),      # Parte de la llave compuesta
    Column('cod_soporte', String),
    Column('resultado_analisis_contenido', String),
    Column('convertido_parametros_resolucion', String),
    Column('resultado_copia', String)
)

def crear_tablas_control_soportes_fac_xml(engine):
    metadata.create_all(engine)
    print("✅ La tabla 'listar.control_soportes_fac_xml' ha sido creada exitosamente.")