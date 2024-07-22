import json
import os
from pymongo import MongoClient
from azure.servicebus import ServiceBusClient

# Carga la configuración desde el archivo JSON
def load_config():
    config_path = 'C:\\Users\\emili\\OneDrive\\Desktop\\notifications_manager\\config\\config.json'
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r') as file:
        return json.load(file)

# Configura la conexión a MongoDB
def get_mongo_client():
    config = load_config()
    mongo_uri = f"mongodb://{config['mongodb']['uri']}"
    return MongoClient(mongo_uri)

# Configura la conexión a Azure Service Bus
def get_service_bus_client():
    config = load_config()
    connection_str = config['azure_service_bus']['connection_string']
    return ServiceBusClient.from_connection_string(connection_str)
