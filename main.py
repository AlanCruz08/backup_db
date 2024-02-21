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

    def conectar_bd(self, db_name):
        try:
            conn_string = f"DRIVER={{SQL Server}};SERVER={self.config['server']}; DATABASE={db_name};"
            conn = pyodbc.connect(conn_string, autocommit=True)
            print(f"Conexión exitosa a la base de datos {db_name}")
            return conn
        except Exception as ex:
            print(f"Error de conexión a la base de datos {db_name}: {ex}")
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
                        BackupDatabase.eliminar_archivo_bak(ruta_archivo)
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
        inicio = time.time()
        exito = False

        while (time.time() - inicio) < tiempo_max_espera:
            try:
                print(f"Comprimiendo archivo...")
                with zipfile.ZipFile(nombre_archivo_zip, 'w', zipfile.ZIP_DEFLATED) as archivo_zip:
                    archivo_zip.write(archivo_a_comprimir, arcname=os.path.basename(archivo_a_comprimir))
                print(f"Archivo comprimido exitosamente en {nombre_archivo_zip}")
                exito = True
                break
            except PermissionError as e:
                print("Error al intentar comprimir el archivo")
                print("Reintentando en 5 segundos...")
                time.sleep(5)
            except Exception as e:
                print(f"Error al comprimir el archivo: {e}")
                break

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
        return True

    @staticmethod
    def eliminar_archivo_bak(ruta_archivo):
        try:
            os.remove(ruta_archivo)
            print(f"Archivo {ruta_archivo} eliminado con éxito.")
        except Exception as e:
            print(f"No se pudo eliminar el archivo {ruta_archivo}: {e}")

    def realizar_backup_bd(self, db_name):
        if not self.crear_directorio_si_no_existe(self.config['ruta_archivo']):
            print("Error al preparar el directorio de copias de seguridad.")
            return

        conn = self.conectar_bd(db_name)
        if conn is None:
            return

        cursor = conn.cursor()
        fecha_hora_actual = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        archivo_backup = f"{self.config['nombre_pv']}_{db_name}_{fecha_hora_actual}"

        try:
            query_backup = f"BACKUP DATABASE {db_name} TO DISK = '{self.config['ruta_archivo']}{archivo_backup}.bak' WITH INIT"
            print("Consulta de copia de seguridad ejecutandose..")
            cursor.execute(query_backup)

            if not self.esperar_archivo_bak(self.config['ruta_archivo'], archivo_backup, float(self.config['tiempo_max'])):
                print("Error al realizar la copia de seguridad: No se encontró el archivo .bak después de esperar.")
                return
            print(f"Copia de seguridad realizada con éxito: {archivo_backup}")

        except Exception as ex:
            print(f"Error al realizar la copia de seguridad: {ex}")
        finally:
            self.cerrar_conexion(conn, cursor)

    def realizar_copia_de_seguridad(self):
        os.system('cls')
        print("Backup V0.5")

        # Backup de la primera base de datos
        self.realizar_backup_bd(self.config['database'])
        
        # Backup de la segunda base de datos
        if 'database2' in self.config:
            self.realizar_backup_bd(self.config['database2'])
        else:
            print("La configuración para 'database2' no se encontró.")

if __name__ == "__main__":
    backup_db = BackupDatabase()
    backup_db.realizar_copia_de_seguridad()
