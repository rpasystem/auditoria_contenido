# en módulo func.py
from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateSchema

def crear_conexion_bd(config):
    # Crear la URL de conexión para SQLAlchemy
    engine_url = f"postgresql://{config.usuario_bd}:{config.password_bd}@{config.hosta}:{config.port}/{config.nombre_bd}"
    engine = create_engine(engine_url)
    return engine

def crear_bd(engine):
    try:
        # Crear la base de datos usando una conexión directa
        with engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE {nombre_bd} WITH ENCODING='UTF8' TEMPLATE=template0;"))

        print(f"Base de datos '{nombre_bd}' creada exitosamente")

    except Exception as e:
        print(f"Error al crear la base de datos: {e}")

def crear_esquema_en_bd(engine):
    try:
        with engine.connect() as connection:
            connection.execute(CreateSchema('soportes'))
            connection.commit()  # Asegura que el esquema se haya creado
        print("Esquema 'soportes' creado exitosamente.")
    except Exception as e:
        print(f"Error al crear el esquema 'soportes': {e}")

def conocer_peso_bd(engine):
    # Ejecutar la consulta para obtener el tamaño de la base de datos
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT pg_size_pretty(pg_database_size('{nombre_bd}'));"))
        size = result.scalar()
        print(f"El tamaño de la base de datos '{nombre_bd}' es: {size}")