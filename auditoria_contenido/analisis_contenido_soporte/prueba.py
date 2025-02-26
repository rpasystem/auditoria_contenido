from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text  # Importar text para consultas en crudo
import os
import subprocess
import fitz
import re
import time
import json
from datetime import datetime
from PyPDF2 import PdfReader

from tqdm import tqdm  # 🔹 Para mostrar progreso
from datetime import datetime
from pytz import timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Detectar el sistema operativo
sistema_operativo = os.name  # 'nt' para Windows, 'posix' para Linux/Mac

ruta_soporte_original = 

resultado_analisis_contenido = validaciones (sistema_operativo, ruta_soporte_original,
                                                            ruta_soporte_destino, nombre_soporte, ruta_qpdf,
                                                            servicio,documento_paciente,cliente)