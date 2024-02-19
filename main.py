import pyodbc
import configparser
import sys
from datetime import datetime
import time
import os
import zipfile

class BackupDatabase:
    def __init__(self, config_path='config.ini'):
        self.config = self.cargar_configuracion(config_path)

    def cargar_configuracion(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        if 'config' in config:
            return config['config']
        else:
            print("Sección de configuración 'config' no encontrada en el archivo de configuración.")
            sys.exit(1)

    def conectar_bd(self):
        try:
            conn_string = f"DRIVER={{SQL Server}};SERVER={self.config['server']};" \
                          f"DATABASE={self.config['database']};USER={self.config['user']};" \
                          f"PASSWORD={self.config['password']};"
            conn = pyodbc.connect(conn_string, autocommit=True)
            print("Conexión exitosa a la base de datos")
            return conn
        except Exception as ex:
            print(f"Error de conexión a la base de datos: {ex}")
            return None

    @staticmethod
    def cerrar_conexion(conn, cursor):
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            print("Conexión cerrada.")

    @staticmethod
    def esperar_archivo_bak(ruta, nombre_archivo, tiempo_max_espera=600):
        ruta_archivo = os.path.join(ruta, nombre_archivo + ".bak")
        ruta_comprimido = os.path.join(ruta, nombre_archivo + ".zip")
        print(f"Buscando archivo: {ruta_archivo}")

        tiempo_inicial = time.time()
        time.sleep(2)

        while (time.time() - tiempo_inicial) < tiempo_max_espera:
            if os.path.exists(ruta_archivo):
                print(f"¡Archivo {nombre_archivo} encontrado!")
                try:
                    if BackupDatabase.comprimir_archivo(ruta_archivo, ruta_comprimido, tiempo_max_espera):
                        BackupDatabase.eliminar_archivo_bak(ruta_archivo)  # Llamar al método para eliminar el .bak
                        return True
                    else:
                        return False
                except Exception as e:
                    print(f"Error al comprimir el archivo: {e}")
                    return False
            else:
                time.sleep(5)

        print(f"Error: No se encontró el archivo {nombre_archivo} después de esperar.")
        return False

    @staticmethod
    def comprimir_archivo(archivo_a_comprimir, nombre_archivo_zip, tiempo_max_espera):
        inicio = time.time()  # Guarda el tiempo de inicio
        exito = False  # Para controlar si la compresión fue exitosa

        while (time.time() - inicio) < tiempo_max_espera:
            try:
                with zipfile.ZipFile(nombre_archivo_zip, 'w', zipfile.ZIP_DEFLATED) as archivo_zip:
                    archivo_zip.write(archivo_a_comprimir, arcname=os.path.basename(archivo_a_comprimir))
                print(f"Archivo comprimido exitosamente en {nombre_archivo_zip}")
                exito = True  # Marcar como exitoso
                break  # Salir del bucle si la compresión fue exitosa
            except PermissionError as e:
                print(f"Error al intentar comprimir el archivo")
                print(f"Reintentando en 5 segundos...")
                time.sleep(5)  # Espera 5 segundos antes de reintentar
            except Exception as e:
                print(f"Error al comprimir el archivo: {e}")
                break  # Salir del bucle si ocurre un error no manejado

        if exito:
            return True
        else:
            print("No se pudo comprimir el archivo después del tiempo máximo de espera.")
            return False


    @staticmethod
    def crear_directorio_si_no_existe(ruta_directorio):
        if not os.path.exists(ruta_directorio):
            try:
                os.makedirs(ruta_directorio)
                return True
            except Exception as e:
                print(f"Error al crear el directorio {ruta_directorio}: {e}")
                return False
        return True  # El directorio ya existe
    
    @staticmethod
    def eliminar_archivo_bak(ruta_archivo):
        try:
            os.remove(ruta_archivo)
            print(f"Archivo {ruta_archivo} eliminado con éxito.")
        except Exception as e:
            print(f"No se pudo eliminar el archivo {ruta_archivo}: {e}")

    def realizar_copia_de_seguridad(self):
        if not self.crear_directorio_si_no_existe(self.config['ruta_archivo']):
            print("Error al preparar el directorio de copias de seguridad.")
            return

        conn = self.conectar_bd()
        if conn is None:
            return  # Error de conexión ya manejado en conectar_bd

        cursor = conn.cursor()
        fecha_hora_actual = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        archivo_backup = f"{self.config['nombre_pv']}_{fecha_hora_actual}"

        try:
            query_backup = f"BACKUP DATABASE {self.config['database']} TO DISK = " \
                           f"'{self.config['ruta_archivo']}{archivo_backup}.bak' WITH INIT"
            cursor.execute(query_backup)
            print("Consulta de copia de seguridad ejecutada, esperando por el archivo...")

            if not self.esperar_archivo_bak(self.config['ruta_archivo'], archivo_backup, float(self.config['tiempo_max'])):
                print("Error al realizar la copia de seguridad: No se encontró el archivo .bak después de esperar.")
                return
            print(f"Copia de seguridad realizada con éxito: {archivo_backup}")
        except Exception as ex:
            print(f"Error al realizar la copia de seguridad: {ex}")
        finally:
            self.cerrar_conexion(conn, cursor)


if __name__ == "__main__":
    backup_db = BackupDatabase()
    backup_db.realizar_copia_de_seguridad()
