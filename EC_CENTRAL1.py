import socket
import sqlite3
import json
import threading
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time

producer_client=KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
Tiempo =time.time()
consumer_taxi = KafkaConsumer(
    'Estado',  # Nombre del topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # Leer solo nuevos mensajes
    enable_auto_commit=True,      # Habilitar el auto-commit de offsets
    group_id='taxi-group',        # Identificador del grupo de consumidores
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserializar mensajes
    )
#Usar base de datos para verificar el ID
def verificar_id(id):
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM Taxi WHERE id = {id}")
        if cursor.fetchone() is None:
            print(f"ID {id} no existente.")
            return False
        else:
            print(f"ID {id} ya existente.")
            cursor.execute(f"Update Taxi set estado = 'OK' where id = {id}")
            conn.commit()
            imprimir_base_datos()
            return True
        
    except sqlite3.Error as e:
        print(f"Error al intentar verificar el ID: {e}")
        return False
    finally:
        conn.close()
    
    
def borra_id(id):
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        cursor.execute(f"Update Taxi set estado = 'KO' where id = {id}")
        print(f"ID {id} inactivo.")
        conn.commit()
        imprimir_base_datos()
    except sqlite3.Error as e:
        print(f"Error al intentar borrar el ID: {e}")
    finally:
        conn.close()

def imprimir_base_datos():
    try:
        conn = sqlite3.connect('Taxi.db')
        cursor = conn.cursor()
        
        # Obtener todos los registros de la tabla Taxi
        cursor.execute("SELECT * FROM Taxi")
        registros = cursor.fetchall()
        
        print(f"{'ID':<2} {'POSX':<2} {'POSY':<2} {'ESTADO':<7} {'DESTINO':<1}")
        print("-" * 30)
        
        # Imprimir cada registro
        for registro in registros:
            id_taxi, posx, posy, estado, destino = registro
            destino = destino if destino is not None else ""
            print(f"{id_taxi:<4} {posx:<5} {posy:<8} {estado:<7} {destino:<1}")
    except sqlite3.Error as e:
        print(f"Error al intentar leer la base de datos: {e}")
    finally:
        conn.close()


def start_server(base_port=3001):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = base_port

    try:
        server_socket.bind(('', port))
        server_socket.listen(10)  # Escuchar hasta 5 conexiones en cola
        print(f"Servidor escuchando en el puerto {port}...")
        threading.Thread(target=estado_taxi, args=(), daemon=True).start()
        while True:
            print("Esperando conexión...")
            client_socket, client_address = server_socket.accept()  # Aceptar conexión de cliente
            print(f"Cliente conectado desde: {client_address}")
            #threading.Thread(target=servicio_cliente, args=(), daemon=True).start()
            
           
            data = client_socket.recv(1024).decode().strip()
            print(f"Mensaje: {data}")
            if data.startswith("STOP"):
                id = id = int(''.join([caracter for caracter in data if caracter.isdigit()]))
                borra_id(id)
                continue
            if verificar_id(data):
                print("ID valido.")
                client_socket.sendall(b"ID valido.")
            else:
                client_socket.sendall(b"Error: ID no existente.")  
            # Verificar si el ID existe en el fichero
            
            client_socket.close()  # Cerrar la conexión con el cliente

    except Exception as e:
        print(f"Error en el servidor: {e}")
        server_socket.close()
    except KeyboardInterrupt:
        print("Servidor detenido manualmente.")
        server_socket.close()
        exit(1)
    finally:
        server_socket.close()


def estado_taxi():
    conn = sqlite3.connect('Taxi.db')
    cursor=conn.cursor()
    print("Esperando mensajes en el topic 'Estado'...")
    for message in consumer_taxi:
        print(f"Mensaje recibido: {message.value}")
        if message.value["Estado"] == "KO":
            cursor.execute(f"Update Taxi set estado = 'KO' where id = {message.value['id']}")
            conn.commit()
            imprimir_base_datos()
        elif message.value["Estado"] == "OK":
            cursor.execute(f"Update Taxi set estado = 'OK' where id = {message.value['id']}")
            conn.commit()
            imprimir_base_datos()
            
                

    
        
#def servicio_cliente():
#    conn = sqlite3.connect('Taxi.db')
#    cursor = conn.cursor()
#    for message in consumer_client:
#        cursor.execute(f"Update Taxi set posx = {message.value['posx']}, posy = {message.value['posy']}, destino = '{message.value['destino']}' where id = {message.value['id']}")


if __name__ == "__main__":
    start_server()
