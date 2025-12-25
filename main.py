import time
import json
from awscrt import mqtt
from awsiot import mqtt_connection_builder

# --- Configurations ---
ENDPOINT = "a1szma62g3s0di-ats.iot.eu-north-1.amazonaws.com"
CLIENT_ID = "MyPythonIoTDevice"
PATH_TO_CERT = "./certs/certificate.pem.crt"
PATH_TO_KEY = "./certs/private.pem.key"
PATH_TO_ROOT = "./certs/AmazonRootCA1.pem"
TOPIC = "test/topic"

# Callback when a message is received
def on_message_received(topic, payload):
    print(f"Received message from topic '{topic}': {payload}")

# 1. Build the connection
print(f"Connecting to {ENDPOINT}...")
mqtt_connection = mqtt_connection_builder.mtls_from_path(
    endpoint=ENDPOINT,
    cert_filepath=PATH_TO_CERT,
    pri_key_filepath=PATH_TO_KEY,
    ca_filepath=PATH_TO_ROOT,
    client_id=CLIENT_ID,
    clean_session=True,
    keep_alive_secs=30
)

# 2. Connecting to AWS IoT
connect_future = mqtt_connection.connect()
connect_future.result() # Wait until connected
print("Connected!")

# 3. Subscribe to the topic
print(f"Subscribing to topic '{TOPIC}'...")
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=TOPIC,
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received
)
subscribe_result = subscribe_future.result()
print(f"Subscribed with {str(subscribe_result['qos'])}")

# 4. Publish a message
message_json = json.dumps({"message": "Hello from Python SDK!"})
print(f"Publishing message to topic '{TOPIC}'...")
mqtt_connection.publish(
    topic=TOPIC,
    payload=message_json,
    qos=mqtt.QoS.AT_LEAST_ONCE
)


# Listen for messages
print("Waiting for messages... (Press Ctrl+C to exit)")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected.")