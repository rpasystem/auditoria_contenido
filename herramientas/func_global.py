import smtplib
import os
import platform
import time
from config import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def enviar_correo_error(asunto, mensaje, error=None):
    """
    Envía un correo con el asunto, mensaje personalizado y un error si se proporciona.
    
    :param asunto: Asunto del correo
    :param mensaje: Cuerpo del mensaje
    :param error: Mensaje de error opcional
    """
    remitente = "davitaclienterpa@gmail.com"
    destinatario = "rpa_system@hotmail.com"
    contraseña = "otqs psec yned tvhd"  # Contraseña de aplicación de Gmail

    # Configurar el mensaje de correo
    msg = MIMEMultipart()
    msg["From"] = remitente
    msg["To"] = destinatario
    msg["Subject"] = asunto

    # Construir el cuerpo del correo
    cuerpo = f"{mensaje}\n\n"
    if error:
        cuerpo += f"🔴 Error detectado:\n{error}\n"

    msg.attach(MIMEText(cuerpo, "plain"))

    try:
        # Establecer conexión con el servidor SMTP de Gmail
        servidor = smtplib.SMTP("smtp.gmail.com", 587)
        servidor.starttls()  # Activar cifrado TLS
        servidor.login(remitente, contraseña)  # Autenticación
        servidor.sendmail(remitente, destinatario, msg.as_string())  # Enviar correo
        servidor.quit()

        print(f"✅ Correo enviado a {destinatario} con el asunto: {asunto}")

    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")


def validar_ruta(ruta):
    """
    Verifica si una ruta existe y es accesible. Si no existe, envía un correo de error.

    :param ruta: Ruta del directorio a validar
    :return: True si la ruta existe, False en caso contrario
    """
    if os.path.exists(ruta) and os.path.isdir(ruta):
        print(f"✅ La ruta '{ruta}' existe y es un directorio válido.")
        return True
    else:
        error_msg = f"❌ La ruta '{ruta}' NO existe o no es accesible."
        print(error_msg)

        # 📩 Enviar un correo de alerta
        enviar_correo_error(
            asunto="⚠️ Error: Ruta no encontrada",
            mensaje=f"Se detectó un problema con la ruta.\n\nDetalles:\n{error_msg}",
            error="Ruta inaccesible o inexistente en el sistema."
        )
        
        return False
    
def crear_conexion_bd(nombre_bd):
    """
    Crea una conexión a la base de datos PostgreSQL con SQLAlchemy.
    Si la conexión falla, envía un correo y reintenta cada 1 minuto hasta que sea exitosa.
    """

    while True:  # 🔄 Intentar en bucle hasta que se pueda conectar
        try:
            # Construir la URL de conexión para SQLAlchemy
            engine_url = f"postgresql://{usuario_bd}:{password_bd}@{hosta}:{port}/{nombre_bd}"
            
            # Crear el motor de conexión
            engine = create_engine(engine_url)

            # Probar la conexión con una consulta simple
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))

            print(f"✅ Conexión a la base de datos: {nombre_bd} exitosa.")
            return engine  # Retorna el motor de conexión si es exitoso

        except Exception as e:
            error_msg = str(e)
            print(f"⚠️ Error al conectar con la base de datos: {error_msg}")

            # Enviar un correo de alerta
            enviar_correo_error(
                asunto="🚨 Error: No se pudo conectar a la base de datos",
                mensaje="Se detectó un fallo al intentar conectarse a la base de datos. Verifica la conexión y vuelve a intentarlo.",
                error=error_msg
            )

            # Esperar 1 minuto antes de volver a intentar
            print("⏳ Esperando 1 minuto antes de volver a intentar la conexión...")
            time.sleep(60)  # 🔹 Espera 60 segundos antes de reintentar



def obtener_ruta_soportes(ruta_base):
    """
    Detecta el sistema operativo y define la ruta adecuada para '2 - SOPORTES DIGITALIZADOS'.

    :param ruta_base: Ruta base del proyecto
    :param validar_func: Función de validación de ruta (ejemplo: func_global.validar_ruta)
    :return: Ruta de la carpeta '2 - SOPORTES DIGITALIZADOS'
    """
    sistema = platform.system()

    if sistema == "Windows":
        print("🖥️ Sistema Windows detectado")
        ruta_raiz = os.path.abspath(os.path.join(ruta_base, '..', '..', '..'))  # Subir tres niveles
    else:
        print("🐧 Sistema Linux detectado")
        ruta_raiz = "/mnt/FACTURACION CAPRECOM2"  # Asegurar que esta ruta existe en Linux

    # Definir la ruta de '2 - SOPORTES DIGITALIZADOS'
    ruta_soportes = os.path.join(ruta_raiz, '2 - SOPORTES DIGITALIZADOS')

    print(f"📂 Ruta base: {ruta_base}")
    print(f"📂 Ruta raíz: {ruta_raiz}")
    print(f"📂 Ruta soportes: {ruta_soportes}")
    
    # Validar la ruta antes de devolverla
    validar_ruta(ruta_soportes)

    return ruta_soportes


import time

def registrar_inicio_proceso():
    """
    Registra la hora de inicio del proceso.

    Retorna:
    - inicio: Timestamp de inicio (time.time())
    - hora_inicio: Hora exacta en formato YYYY-MM-DD HH:MM:SS

    También imprime el inicio en consola.
    """
    inicio = time.time()
    hora_inicio = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(inicio))
    print(f"⏳ Proceso iniciado a las: {hora_inicio}")
    return inicio, hora_inicio

def registrar_tiempo_fin(inicio):
    """
    Calcula el tiempo de ejecución desde el timestamp `inicio` hasta el momento actual.

    Retorna:
    - hora_fin: Hora exacta en formato YYYY-MM-DD HH:MM:SS
    - duracion_segundos: Duración total en segundos
    - duracion_minutos: Duración total en minutos

    También imprime los valores en consola.
    """
    fin = time.time()
    hora_fin = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fin))

    # Calcular duración en segundos y minutos
    duracion_segundos = round(fin - inicio, 2)
    duracion_minutos = round(duracion_segundos / 60, 2)

    # Mostrar información en consola
    print(f"✅ Proceso finalizado a las: {hora_fin}")
    print(f"⏱️ Duración total: {duracion_segundos} segundos ({duracion_minutos} minutos)")

    return hora_fin, duracion_segundos, duracion_minutos



