import paho.mqtt.client as mqtt
import time
import threading
import sys


class Publisher:
    def __init__(self, instance_id, broker_address="localhost", broker_port=1883):
        self.instance_id = instance_id
        # Fix for Paho MQTT 2.0+ by specifying callback API version
        self.client = mqtt.Client(client_id=f"publisher-{instance_id}",
                                  callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.broker_address = broker_address
        self.broker_port = broker_port

        # Default settings
        self.qos = 0
        self.delay = 100
        self.messagesize = 0
        self.instancecount = 1
        self.running = False

    def on_connect(self, client, userdata, flags, rc):
        print(f"Publisher {self.instance_id} connected with result code {rc}")
        # Subscribe to request topics
        client.subscribe("request/qos")
        client.subscribe("request/delay")
        client.subscribe("request/messagesize")
        client.subscribe("request/instancecount")
        client.subscribe("request/go")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        if topic == "request/qos":
            self.qos = int(payload)
        elif topic == "request/delay":
            self.delay = int(payload)
        elif topic == "request/messagesize":
            self.messagesize = int(payload)
        elif topic == "request/instancecount":
            self.instancecount = int(payload)
        elif topic == "request/go":
            # Check if this instance should be active
            if self.instance_id <= self.instancecount:
                self.start_publishing()
            else:
                self.running = False

    def start_publishing(self):
        self.running = True
        # Start publishing in a separate thread
        threading.Thread(target=self.publish_messages).start()

    def publish_messages(self):
        counter = 0
        end_time = time.time() + 30  # Run for 30 seconds

        topic = f"counter/{self.instance_id}/{self.qos}/{self.delay}/{self.messagesize}"

        while time.time() < end_time and self.running:
            # Create message with format counter:timestamp:xxx...xxx
            timestamp = int(time.time() * 1000)  # millisecond precision
            x_string = "x" * self.messagesize
            message = f"{counter}:{timestamp}:{x_string}"

            # Publish message
            self.client.publish(topic, message, qos=self.qos)
            counter += 1

            # Wait for delay if specified
            if self.delay > 0:
                time.sleep(self.delay / 1000)  # Convert to seconds

        self.running = False
        print(f"Publisher {self.instance_id} finished publishing {counter} messages")

    def connect(self):
        self.client.connect(self.broker_address, self.broker_port, 60)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()


# Create and start 10 publisher instances
if __name__ == "__main__":
    # Get broker address from command line if provided
    broker_address = "localhost"
    if len(sys.argv) > 1:
        broker_address = sys.argv[1]

    publishers = []
    for i in range(1, 11):
        pub = Publisher(i, broker_address=broker_address)
        pub.connect()
        publishers.append(pub)
        print(f"Started publisher {i}")

    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Disconnect all publishers on keyboard interrupt
        print("Shutting down publishers...")
        for pub in publishers:
            pub.disconnect()