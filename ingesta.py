import mysql.connector
import csv
import boto3

# Parámetros de conexión a MySQL
host = '34.227.210.250'
port = 8005  # Puerto personalizado
database = 'tienda'
user = 'root'
password = 'utec'
tabla = 'fabricantes'

# Conectarse a MySQL
try:
    conexion = mysql.connector.connect(
        host=host,
        port=port,  # Especificar el puerto 8006
        database=database,
        user=user,
        password=password
    )

    if conexion.is_connected():
        print(f'Conectado a la base de datos {database}')
        cursor = conexion.cursor()
        cursor.execute(f'SELECT * FROM {tabla}')
        registros = cursor.fetchall()
        columnas = [i[0] for i in cursor.description]
        fichero_csv = 'data.csv'
        
        with open(fichero_csv, 'w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            escritor_csv.writerow(columnas)
            escritor_csv.writerows(registros)
        print(f'Registros guardados en {fichero_csv}')

except mysql.connector.Error as error:
    print(f'Error al conectar a MySQL: {error}')
finally:
    if 'conexion' in locals() and conexion.is_connected():
        cursor.close()
        conexion.close()
        print('Conexión MySQL cerrada')

nombreBucket = "ingest2outputzamir"
s3 = boto3.client('s3')

try:
    response = s3.upload_file(fichero_csv, nombreBucket, fichero_csv)
    print(f'Archivo {fichero_csv} subido al bucket {nombreBucket}')
except Exception as e:
    print(f'Error al subir el archivo a S3: {e}')
