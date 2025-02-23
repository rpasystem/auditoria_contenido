# en config.py
import os

from sqlalchemy import create_engine, text
from pathlib import Path

# Obtener la ruta base del script
ruta_base = Path(__file__).resolve().parent
# Construir la ruta del archivo Excel de manera relativa y multiplataforma
ruta_excel_base_auditoria = (ruta_base / ".." / ".." / "archivos_excel" / "base_auditoria" / "BASE AUDITORIA.xlsx").resolve()

# Configuración de conexión a BD
usuario_bd = 'postgres'
password_bd = 'Davita2024*RPA**'
host = 'localhost'
hosta = "10.193.65.201"
port = '5432'
nombre_bd = 'rips'



