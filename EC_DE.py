import sys
import socket
import json
import time
import threading
from kafka.errors import kafka_errors
from kafka import KafkaConsumer
from kafka import KafkaProducer

producer=KafkaProducer(
    
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


consumer = KafkaConsumer(
    'Mapa',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='Taxis'
)

if len(sys.argv) != 6:
    print("Usage: python3 EC_DE.py <IP EC_S> <PORT EC_S> <IP CENTRAL> <CENTRAL PORT> <ID TAXI>")
    sys.exit(1)

# Servidor empieza en 8080 y si está ocupado, intenta con el siguiente puerto
def start_server(expected_client_ip, expected_client_port, base_port=8080):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = base_port

    while True:  # Este bucle es para seguir escuchando nuevos clientes
        try:
            server_socket.bind(('', port))
            server_socket.listen(1)
            print(f"Servidor escuchando en el puerto {port}...")
            
            while True:  # Bucle para aceptar múltiples clientes
                print("Esperando conexión...")
                client_socket, client_address = server_socket.accept()  # Aceptar la conexión del cliente
                client_ip, client_port = client_address
                
                print(f"Cliente conectado desde IP: {client_ip}, Puerto: {client_port}")

                # Manejar la comunicación con el cliente
                handle_client(client_socket)
                
        except OSError as e:
            print(f"Error al intentar iniciar el servidor en el puerto {port}: {e}")
            port += 1  # Intentar con el siguiente puerto
        except KeyboardInterrupt:
            print("Servidor detenido manualmente.")
            server_socket.close()
            exit(1)



    


def handle_client(client_socket):
    try:
        client_socket.sendall(b"Conexion exitosa!")
        start_client(ip_central, puerto_central, id_taxi, client_socket,0)
        threading.Thread(target=process_kafka_messages, args=(), daemon=True).start()
        ko=False
        while True:  # Bucle para manejar los mensajes del cliente
            data = client_socket.recv(1024)
            if not data:
                print("Cliente desconectado.")
                break
            message = data.decode()
            print(f"Mensaje recibido: {message}")
            if message == "KO" and ko==False:
                print("Enviado a central")
                producer.send('Estado',value={"Estado":"KO","id":id_taxi})
                ko=True
            elif message == "OK" and ko==True:
                print("Enviado a central OK")
                producer.send('Estado',value={"Estado":"OK","id":id_taxi})
                ko=False
    except KeyboardInterrupt:
        print("Servidor detenido")
        client_socket.sendall(b"Servidor detenido")
        client_socket.close()
        start_client(ip_central, puerto_central, id_taxi, client_socket,1)
        exit(1)
    finally:
        client_socket.close()  # Cerrar la conexión del cliente

def process_kafka_messages():
    try:
        for message in consumer:
            print(f"Mensaje recibido: {message.value}")
    except kafka_errors:
        print("Error al intentar leer mensajes de Kafka.")
        
        
        
        
        
def start_client(ip_central, puerto_central, id_taxi, client_socket_client,error):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((ip_central, puerto_central))
        if error == 1:
            client_socket.sendall(b"STOP"+str(id_taxi).encode())
            exit(1)
        client_socket.sendall(str(id_taxi).encode())
        
        if client_socket.recv(1024).decode() == "ID valido.":
            print("ID válido. Conexión establecida con el servidor central.")
            client_socket_client.sendall(b"ID valido.")
        else:
            print("Error: ID no válido no exite en la BD.")
            client_socket_client.sendall(b"Servidor detenido")
            client_socket_client.close()
            client_socket.close()
            exit(1)
    except ConnectionRefusedError:
        print("Error: No se pudo conectar al servidor central.")
        sys.exit(1)
    except KeyboardInterrupt:
            print(f"Servidor detenido manualmente.")
            client_socket.sendall(b"STOP"+str(id_taxi).encode())
            exit(1)
    




if __name__ == "__main__":
    expected_client_ip = sys.argv[1]  # Puedes definir esto según tu configuración
    expected_client_port = int(sys.argv[2]) # Puerto esperado del cliente
    puerto_central = int(sys.argv[4])
    ip_central = sys.argv[3]
    id_taxi = sys.argv[5]
    
    start_server(expected_client_ip, expected_client_port)
