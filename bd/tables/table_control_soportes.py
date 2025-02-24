from sqlalchemy import create_engine, MetaData, Table, Column, String, Date

# Definir la metadata y esquema
metadata = MetaData(schema='listar')

# 🔹 Definición de la tabla control_soportes
control_soportes = Table(
    'control_soportes', metadata,
    Column('fecha_soporte', Date),  # Si prefieres, puedes usar Date
    Column('origen_soporte', String),
    Column('ruta_completa', String),
    Column('nombre_soporte', String,primary_key=True),
    Column('llave_unica', String, primary_key=True),            
    Column('unidad_renal', String),            
    Column('servicio', String),            
    Column('cliente', String),            
    Column('documento_paciente', String),
    Column('codigo_sede', String),
    Column('llave_a', String),
    Column('llave_b', String),
    Column('cod_soporte', String),
    Column('origen_sede', String),
    Column('extramural', String),
    Column('resultado_analisis_contenido', String),
    Column('convertido_parametros_resolucion', String),
    Column('resultado_copia', String)
)

# 🔹 Función para crear la tabla en la base de datos
def crear_tablas_control_soportes(engine):
    """
    Crea la tabla listar.control_soportes en el esquema listar.
    """
    metadata.create_all(engine)
    print("✅ La tabla 'listar.control_soportes' ha sido creada exitosamente.")
