import sys
import time
import json
from kafka import KafkaProducer

class ECCustomer:
    def __init__(self, broker_ip, broker_port, customer_id, pickup_x, pickup_y, file_path):
        # Conexión al broker de Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=[f"{broker_ip}:{broker_port}"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.customer_id = customer_id
        self.pickup_location = (pickup_x, pickup_y)  # Almacena las coordenadas X e Y
        self.file_path = file_path
    
    def read_services(self):
        # Leer las solicitudes de servicio del archivo
        with open(self.file_path, 'r') as file:
            services = file.readlines()
        # Eliminar saltos de línea y devolver la lista de destinos
        return [service.strip() for service in services]

    def send_service_request(self, pickup, destination):
        # Formato del mensaje que se enviará a la central
        message = {
            'customer_id': self.customer_id,
            'pickup_location': pickup,
            'destination': destination,
            'timestamp': time.time()
        }
        print(f"Solicitando servicio de taxi desde {pickup} hasta {destination}")
        # Enviar el mensaje al topic de Kafka
        self.producer.send('taxi_requests', value=message)

    def run(self):
        # Leer los destinos de servicio
        services = self.read_services()
        
        for service in services:
            # Enviar la solicitud de taxi
            self.send_service_request(self.pickup_location, service)
            # Esperar 4 segundos antes de enviar la siguiente solicitud
            time.sleep(4)

if __name__ == '__main__':
    # Verificar que se recibieron los parámetros correctos
    if len(sys.argv) != 7:
        print("Uso: python EC_Customer.py <broker_ip> <broker_port> <customer_id> <pickup_x> <pickup_y> <file_path>")
        sys.exit(1)

    # Leer los argumentos de la línea de comandos
    broker_ip = sys.argv[1]
    broker_port = sys.argv[2]
    customer_id = sys.argv[3]
    pickup_x = int(sys.argv[4])  # Convertir a entero
    pickup_y = int(sys.argv[5])  # Convertir a entero
    file_path = sys.argv[6]

    # Crear la aplicación cliente y ejecutar
    customer = ECCustomer(broker_ip, broker_port, customer_id, pickup_x, pickup_y, file_path)
    customer.run()
