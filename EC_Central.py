import socket
import threading
import time
import argparse
from kafka import KafkaProducer

# Constantes
HOST = 'localhost'
PORT = 12345
KAFKA_TOPIC = 'SD'  # Tema para transmitir eventos en Kafka
KAFKA_SERVER = 'localhost:9092'  # Dirección del servidor Kafka
MAP_SIZE = 20  # Mapa 20x20
taxis = {}  # Diccionario para almacenar los taxis y sus estados
mapa = [[' ' for _ in range(MAP_SIZE)] for _ in range(MAP_SIZE)]  # Mapa 2D

# Bloqueo para la concurrencia
lock = threading.Lock()

# Inicializar el productor de Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

# Función para leer el fichero del mapa
def leer_mapa(filename):
    global mapa
    with open(filename, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            id_localizacion, x, y = parts[0], int(parts[1]), int(parts[2])
            mapa[x][y] = id_localizacion  # Añadir localización al mapa
    print("Mapa inicial cargado:")
    mostrar_mapa()

# Función para mostrar el mapa en la consola
def mostrar_mapa():
    for fila in mapa:
        print(' '.join(fila))
    print()

# Función para manejar la conexión con los taxis
def handle_taxi(conn, addr):
    global taxis
    try:
        # Recibe el ID del taxi
        taxi_id = conn.recv(1024).decode('utf-8')
        print(f"Taxi {taxi_id} conectado desde {addr}")
        
        with lock:
            taxis[taxi_id] = {'estado': 'LIBRE', 'posicion': [1, 1]}  # Posición inicial

        # Enviar el mapa inicial al taxi
        conn.send(str(mapa).encode('utf-8'))

        while True:
            # Recibe las actualizaciones del taxi
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break
            print(f"Taxi {taxi_id}: {data}")

            # Actualiza la posición del taxi
            if data.startswith('POS'):
                _, x, y = data.split('#')
                with lock:
                    taxis[taxi_id]['posicion'] = [int(x), int(y)]
                    actualizar_mapa()
                    # Enviar evento a Kafka
                    enviar_evento_kafka(f"Taxi {taxi_id} se movió a {x},{y}")

            # Envía el mapa actualizado al taxi
            conn.send(str(mapa).encode('utf-8'))

    except Exception as e:
        print(f"Error con el taxi {taxi_id}: {e}")
    finally:
        conn.close()
        with lock:
            del taxis[taxi_id]
        print(f"Taxi {taxi_id} desconectado.")

# Función para enviar eventos a Kafka
def enviar_evento_kafka(evento):
    producer.send(KAFKA_TOPIC, value=evento.encode('utf-8'))
    producer.flush()

# Función para manejar las solicitudes de clientes
def handle_customer(conn, addr):
    try:
        # Recibe la solicitud del cliente
        destino = conn.recv(1024).decode('utf-8')
        print(f"Cliente {addr} solicitó un servicio al destino {destino}")
        
        # Asignar un taxi disponible
        taxi_id = asignar_taxi()
        if taxi_id:
            print(f"Taxi {taxi_id} asignado al cliente en {addr}")
            conn.send(f"OK#Taxi {taxi_id} asignado".encode('utf-8'))
        else:
            conn.send("KO#No hay taxis disponibles".encode('utf-8'))
    
    except Exception as e:
        print(f"Error con el cliente: {e}")
    finally:
        conn.close()

# Función para asignar un taxi disponible
def asignar_taxi():
    with lock:
        for taxi_id, info in taxis.items():
            if info['estado'] == 'LIBRE':
                taxis[taxi_id]['estado'] = 'OCUPADO'
                return taxi_id
    return None

# Función para actualizar el mapa
def actualizar_mapa():
    global mapa
    mapa = [[' ' for _ in range(MAP_SIZE)] for _ in range(MAP_SIZE)]  # Limpiar mapa
    for taxi_id, info in taxis.items():
        x, y = info['posicion']
        mapa[x][y] = 'T' + taxi_id  # Marcar taxi en el mapa
    print("Mapa actualizado:")
    mostrar_mapa()

# Función principal para aceptar conexiones de taxis y clientes
def main():
    # Usar argparse para recibir los parámetros de línea de comandos
    parser = argparse.ArgumentParser(description="Central de control EasyCab")
    parser.add_argument("fichero_mapa", help="Fichero con la configuración del mapa")
    args = parser.parse_args()

    # Leer el fichero del mapa pasado por parámetro
    leer_mapa(args.fichero_mapa)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Central de control escuchando en {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            data = conn.recv(1024).decode('utf-8')
            if data == "TAXI":
                # Conexión con un taxi
                threading.Thread(target=handle_taxi, args=(conn, addr)).start()
            elif data == "CUSTOMER":
                # Conexión con un cliente
                threading.Thread(target=handle_customer, args=(conn, addr)).start()

if __name__ == "__main__":
    main()
