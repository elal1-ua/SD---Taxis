import sys
import time
import json
from kafka import KafkaProducer

class ECCustomer:
    def __init__(self, broker_ip, broker_port, customer_id, file_path):
        # Conexión al broker de Kafka
        self.producer = KafkaProducer( #creación de un productor de Kafka
            bootstrap_servers=[f"{broker_ip}:{broker_port}"], #se especifica la dirección IP y el puerto del broker
            value_serializer=lambda v: json.dumps(v).encode('utf-8') #se especifica cómo se serializarán los mensajes que se envíen a Kafka
        )
        self.customer_id = customer_id
        self.file_path = file_path #ruta del archivo con los destinos de servicio
    
    def read_services(self):
        # Leer las solicitudes de servicio del archivo
        with open(self.file_path, 'r') as file: #se abre el archivo en modo de lectura (r - read). con with el archivo se cierra solo cuando acabe de usarse
            services = file.readlines() #cada linea es un elemento de la lista services
        # Eliminar saltos de línea y devolver la lista de destinos
        return [service.strip() for service in services]

    def send_service_request(self, pickup, destination): #método para enviar una solicitud de servicio a través de Kafka
        # Formato del mensaje que se enviará a la central
        message = { #diccionario con la información de la solicitud
            'customer_id': self.customer_id,
            'pickup_location': pickup,
            'destination': destination,
            'timestamp': time.time()
        }
        print(f"Solicitando servicio de taxi desde {pickup} hasta {destination}")
        # Enviar el mensaje al topic de Kafka
        self.producer.send('taxi_requests', value=message) 

    def run(self): #se le llama cuando queremos que el cliente comience a enviar solicitudes de servicio
        # Leer los destinos de servicio
        services = self.read_services() #se leen los destinos de servicio del archivo
        pickup_location = self.get_pickup_location() #se obtiene la ubicación actual del cliente
        
        for service in services:
            # Enviar la solicitud de taxi
            self.send_service_request(pickup_location, service)
            # Esperar 4 segundos antes de enviar la siguiente solicitud (lo pone en el enunciado)
            time.sleep(4)

    def get_pickup_location(self): #método para obtener la ubicación actual del cliente
        #falta implementar como se obtiene la ubicación actual del cliente
        return self.get_pickup_location

if __name__ == '__main__': #se ejecuta cuando se llama al script desde la línea de comandos
    # Verificar que se recibieron los parámetros correctos
    if len(sys.argv) != 5:
        print("Uso: python EC_Customer.py <broker_ip> <broker_port> <customer_id> <file_path>")
        sys.exit(1)

    # Leer los argumentos de la línea de comandos
    broker_ip = sys.argv[1]
    broker_port = sys.argv[2]
    customer_id = sys.argv[3]
    file_path = sys.argv[4]

    # Crear la aplicación cliente y ejecutar
    customer = ECCustomer(broker_ip, broker_port, customer_id, file_path)
    customer.run()
