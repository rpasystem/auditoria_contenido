# En módulo bd_main.py

import os
import sys
from sqlalchemy import create_engine, text

# 📌 Definir ruta base correctamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'herramientas')))
import config


# 📌 Importaciones internas (después de configurar sys.path)
from func import *  
from tables.table_control_soportes import *
from tables.table_control_soportes_fac_xml import *


# Crear la conexcion a la base de datos
engine = crear_conexion_bd(config)

# crear_tablas_control_soportes(engine)
crear_tablas_control_soportes_fac_xml(engine)

# Finalizar la conexión a la base de datos
engine.dispose()

