import pyodbc
import configparser
import sys
from datetime import datetime
import time
import os
import zipfile

def esperar_archivo_bak(ruta, nombre_archivo,tiempo_max_espera=600):
    ruta_archivo = os.path.join(ruta, nombre_archivo+".bak")
    ruta_comprimido = os.path.join(ruta, nombre_archivo+".zip")
    print(f"Ruta del archivo: {ruta_archivo}")
    
    tiempo_inicial = time.time()
    time.sleep(5)

    while time.time() - tiempo_inicial < tiempo_max_espera:
        if os.path.exists(ruta_archivo):
            print(f"¡Archivo {nombre_archivo} encontrado!")
            try:
                comprimir_archivo(ruta_archivo, ruta_comprimido)
                time.sleep(20)
                return True
            except Exception as e:
                print(f"Error al comprimir el archivo: {e}")
                time.sleep(10)
                return False
        else:
            time.sleep(5)

    print(f"Error: No se encontró el archivo {nombre_archivo} después de esperar.")
    time.sleep(10)
    return False

def comprimir_archivo(archivo_a_comprimir, nombre_archivo_zip):
    print(f"Comprimiendo archivo {archivo_a_comprimir} \nen {nombre_archivo_zip}")
    with zipfile.ZipFile(nombre_archivo_zip, 'w', zipfile.ZIP_DEFLATED) as archivo_zip:
        archivo_zip.write(archivo_a_comprimir, arcname=os.path.basename(archivo_a_comprimir))
    
def realizar_copia_de_seguridad():
    # Cargar la configuración
    config = configparser.ConfigParser()
    config.read('config.ini')

    server = config['config']['server']
    database = config['config']['database']
    username = config['config']['user']
    password = config['config']['password']
    pv_nombre = config['config']['nombre_pv']
    rutaarchivo = config['config']['ruta_archivo']
    tiempo_max = config['config']['tiempo_max']

    # Cadena de conexión para la autenticación de SQL Server
    conn_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};USER={username};PASSWORD={password};'

    try:
        # Intentar conectar a la base de datos
        conn = pyodbc.connect(conn_string)
        conn.autocommit = True
        os.system('cls')
        print("Conexión exitosa a la base de datos")

        # Crear un objeto cursor para ejecutar consultas SQL
        cursor = conn.cursor()

        # Obtener la fecha y hora actual para incluir en el nombre del archivo
        fecha_hora_actual = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        # Nombre del archivo de copia de seguridad, incluyendo la ruta del directorio actual
        archivo_backup = f"{pv_nombre}_{fecha_hora_actual}"

        # Comprobar ruta de acceso
        crear_directorio_si_no_existe(rutaarchivo)

        try:
            # Consulta de copia de seguridad
            query_backup = f"BACKUP DATABASE {database} TO DISK = '{rutaarchivo}{archivo_backup}.bak' WITH INIT"

            # Ejecutar la consulta de copia de seguridad
            cursor.execute(query_backup)
            
            if esperar_archivo_bak(rutaarchivo, archivo_backup, float(tiempo_max)):
                print(f"Copia de seguridad realizada con éxito: {archivo_backup}")
            else:
                print(f"Error al realizar la copia de seguridad: No se encontró el archivo .bak después de esperar.")
                time.sleep(10)
        except Exception as ex:
            # Manejar excepciones de pyodbc, por ejemplo, mostrar un mensaje de error
            print(f"Error al realizar la copia de seguridad: {ex}")
            time.sleep(10)

        finally:
            # eliminacion del archivo .bak
            
            bak_file_path = os.path.join(rutaarchivo, f"{archivo_backup}.bak")
            try:
                os.remove(bak_file_path)
                print(f"Archivo {archivo_backup}.bak eliminado exitosamente.")
            except Exception as ex:
                print(f"Error al eliminar el archivo {archivo_backup}.bak: {ex}")
            
            for i in range(5):
                print(f"Esperando {5-i} segundos para cerrar la conexión...")
                time.sleep(1)
            
            # No olvides cerrar el cursor y la conexión cuando hayas terminado
            conn.autocommit = False
            cursor.close()
            conn.close()

    except pyodbc.Error as ex:
        # Manejar excepciones de pyodbc, por ejemplo, mostrar un mensaje de error
        print(f"Error de conexión a la base de datos: {ex}")
        time.sleep(10)
        sys.exit(1)  # Terminar el script con código de error 1

def crear_directorio_si_no_existe(ruta_directorio):
    if not os.path.exists(ruta_directorio):
        os.makedirs(ruta_directorio)


if __name__ == "__main__":
    try:
        realizar_copia_de_seguridad()
    except Exception as e:
        print(f"Error al realizar la copia de seguridad: {e}")
        time.sleep(10)
