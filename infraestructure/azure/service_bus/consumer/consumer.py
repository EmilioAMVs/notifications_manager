import json
import time
import sys
sys.path.append('C:\\Users\\emili\\OneDrive\\Desktop\\notifications_manager')
from infraestructure.azure.service_bus.connections.connection import get_service_bus_client, get_mongo_client, load_config


def receive_messages(queue_name, timeout=30):
    print(f"Configuring receiver for queue: {queue_name}\n")
    client = get_service_bus_client()
    receiver = client.get_queue_receiver(queue_name=queue_name, prefetch_count=10)
    
    with receiver:
        print(f"Starting to receive messages from queue: {queue_name}\n")
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                print(f"Timeout reached for queue '{queue_name}'\n")
                break
            
            messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)  # Espera hasta 5 segundos
            
            if not messages:
                print(f"No more messages in queue '{queue_name}'\n")
                break
            
            for msg in messages:
                # Procesa el mensaje
                if isinstance(msg.body, bytes):
                    message = msg.body.decode('utf-8')  # Decodifica el mensaje a una cadena si es bytes
                else:
                    # Si es un generador, convierte cada elemento a cadena y une
                    message = ''.join(part.decode('utf-8') if isinstance(part, bytes) else part for part in msg.body)

                print(f"Received message from queue '{queue_name}': {message}\n")

                # Guarda el mensaje en MongoDB
                mongo_client = get_mongo_client()
                db_name = load_config()['mongodb']['db_name']
                db = mongo_client[db_name]
                collection = db[queue_name]

                print(f"Inserting message into MongoDB collection '{queue_name}' in database '{db_name}'\n")
                collection.insert_one({"message": message})

                # Completa el mensaje
                receiver.complete_message(msg)
                print(f"Message from queue '{queue_name}' completed.")
                

if __name__ == "__main__":
    config = load_config()
    queues = config['queues']

    print("Starting message receiver\n")
    for queue in queues:
        print(f"Processing queue: {queue['name']}\n")
        try:
            receive_messages(queue['name'])
        except Exception as e:
            print(f"An error occurred while processing queue '{queue['name']}': {e}")
    print("Finished processing all queues")
