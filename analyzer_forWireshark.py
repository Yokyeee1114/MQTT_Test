import paho.mqtt.client as mqtt
import time
import json
import statistics
import csv
import os
import sys
from datetime import datetime


class Analyzer:
    def __init__(self, broker_address="localhost", broker_port=1883):
        # Fix for Paho MQTT 2.0+ by specifying callback API version
        self.client = mqtt.Client(client_id="analyzer",
                                  callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.broker_address = broker_address
        self.broker_port = broker_port

        # Current test parameters
        self.current_test = {
            "qos": 0,
            "delay": 0,
            "messagesize": 0,
            "instancecount": 1,
            "sub_qos": 0
        }

        # Data collection
        self.messages_received = {}  # {instance_id: {counter: timestamp}}
        self.messages_expected = {}  # {instance_id: expected_count}
        self.test_start_time = 0
        self.test_end_time = 0
        self.sys_metrics = {}

        # Results storage
        self.results = []

        # Create output directory
        os.makedirs("results", exist_ok=True)

    def on_connect(self, client, userdata, flags, rc):
        print(f"Analyzer connected with result code {rc}")
        # Subscribe to $SYS topics
        client.subscribe("$SYS/#")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        # Handle $SYS messages
        if topic.startswith("$SYS/"):
            try:
                # Try to convert to number if possible
                payload_num = float(payload)
                self.sys_metrics[topic] = payload_num
            except ValueError:
                self.sys_metrics[topic] = payload
            return

        # Handle counter messages
        if topic.startswith("counter/"):
            try:
                parts = topic.split("/")
                instance_id = int(parts[1])

                # Parse message
                message_parts = payload.split(":", 2)
                counter = int(message_parts[0])
                timestamp = int(message_parts[1])

                # Store message data
                if instance_id not in self.messages_received:
                    self.messages_received[instance_id] = {}

                self.messages_received[instance_id][counter] = timestamp
            except Exception as e:
                print(f"Error processing message: {e}")

    def subscribe_to_counter_topics(self):
        # Unsubscribe from previous counter topics if any
        self.client.unsubscribe("counter/#")

        # Subscribe to new counter topics based on current test
        topic = f"counter/+/{self.current_test['qos']}/{self.current_test['delay']}/{self.current_test['messagesize']}"
        self.client.subscribe(topic, qos=self.current_test["sub_qos"])
        print(f"Subscribed to {topic} with QoS {self.current_test['sub_qos']}")

    def run_test(self, qos, delay, messagesize, instancecount, sub_qos):
        # Update current test parameters
        self.current_test = {
            "qos": qos,
            "delay": delay,
            "messagesize": messagesize,
            "instancecount": instancecount,
            "sub_qos": sub_qos
        }

        # Clear previous test data
        self.messages_received = {}
        self.messages_expected = {}
        self.sys_metrics = {}

        # Subscribe to appropriate topics
        self.subscribe_to_counter_topics()

        # Send test parameters to publishers
        self.client.publish("request/qos", qos)
        self.client.publish("request/delay", delay)
        self.client.publish("request/messagesize", messagesize)
        self.client.publish("request/instancecount", instancecount)

        # Wait for a moment to ensure all publishers have received the configuration
        time.sleep(1)

        # Start the test
        print(f"Starting test: QoS={qos}, Delay={delay}, Size={messagesize}, Count={instancecount}, Sub_QoS={sub_qos}")
        self.test_start_time = time.time()
        self.client.publish("request/go", "1")

        # Wait for test to complete (30 seconds + buffer)
        test_duration = 30 + 2  # 2 seconds buffer
        time.sleep(test_duration)
        self.test_end_time = time.time()

        # Calculate and store results
        self.calculate_results()

        # Wait before next test
        time.sleep(2)

    def calculate_results(self):
        # Calculate overall message rate
        total_messages = sum(len(messages) for messages in self.messages_received.values())
        test_duration = self.test_end_time - self.test_start_time
        message_rate = total_messages / test_duration if test_duration > 0 else 0

        # Initialize per-publisher metrics
        per_publisher_metrics = {
            "message_loss": [],
            "out_of_order": [],
            "duplicates": [],
            "mean_gap": [],
            "std_dev_gap": []
        }

        for instance_id, messages in self.messages_received.items():
            # Sort messages by counter
            sorted_counters = sorted(messages.keys())

            if not sorted_counters:
                continue

            # Calculate expected messages based on delay
            if self.current_test["delay"] == 0:
                # For 0ms delay, just use the highest counter as expected
                max_counter = max(sorted_counters)
                expected_messages = max_counter + 1
            else:
                # For fixed delay, calculate theoretical count
                expected_messages = int(30 * (1000 / self.current_test["delay"]))

            # Message loss
            actual_messages = len(messages)
            message_loss = 100 * (1 - actual_messages / expected_messages) if expected_messages > 0 else 0
            per_publisher_metrics["message_loss"].append(message_loss)

            # Out of order messages
            out_of_order_count = 0
            prev_counter = None
            for counter in sorted(messages.keys()):
                if prev_counter is not None and counter < prev_counter:
                    out_of_order_count += 1
                prev_counter = counter
            out_of_order_percentage = 100 * out_of_order_count / actual_messages if actual_messages > 0 else 0
            per_publisher_metrics["out_of_order"].append(out_of_order_percentage)

            # Duplicate messages
            unique_counters = set(sorted_counters)
            duplicate_count = len(sorted_counters) - len(unique_counters)
            duplicate_percentage = 100 * duplicate_count / actual_messages if actual_messages > 0 else 0
            per_publisher_metrics["duplicates"].append(duplicate_percentage)

            # Mean inter-message gap and standard deviation
            gaps = []
            for i in range(1, len(sorted_counters)):
                if sorted_counters[i] == sorted_counters[i - 1] + 1:  # Only consecutive messages
                    current_timestamp = messages[sorted_counters[i]]
                    prev_timestamp = messages[sorted_counters[i - 1]]
                    gap = current_timestamp - prev_timestamp
                    gaps.append(gap)

            if gaps:
                mean_gap = statistics.mean(gaps)
                std_dev_gap = statistics.stdev(gaps) if len(gaps) > 1 else 0
            else:
                mean_gap = 0
                std_dev_gap = 0

            per_publisher_metrics["mean_gap"].append(mean_gap)
            per_publisher_metrics["std_dev_gap"].append(std_dev_gap)

        # Calculate averages across publishers
        avg_message_loss = statistics.mean(per_publisher_metrics["message_loss"]) if per_publisher_metrics[
            "message_loss"] else 0
        avg_out_of_order = statistics.mean(per_publisher_metrics["out_of_order"]) if per_publisher_metrics[
            "out_of_order"] else 0
        avg_duplicates = statistics.mean(per_publisher_metrics["duplicates"]) if per_publisher_metrics[
            "duplicates"] else 0
        avg_mean_gap = statistics.mean(per_publisher_metrics["mean_gap"]) if per_publisher_metrics["mean_gap"] else 0
        avg_std_dev_gap = statistics.mean(per_publisher_metrics["std_dev_gap"]) if per_publisher_metrics[
            "std_dev_gap"] else 0

        # Store results
        result = {
            "qos": self.current_test["qos"],
            "delay": self.current_test["delay"],
            "messagesize": self.current_test["messagesize"],
            "instancecount": self.current_test["instancecount"],
            "sub_qos": self.current_test["sub_qos"],
            "message_rate": message_rate,
            "avg_message_loss": avg_message_loss,
            "avg_out_of_order": avg_out_of_order,
            "avg_duplicates": avg_duplicates,
            "avg_mean_gap": avg_mean_gap,
            "avg_std_dev_gap": avg_std_dev_gap,
            "sys_metrics": self.sys_metrics.copy()
        }

        self.results.append(result)
        print(f"Test completed: QoS={result['qos']}, Delay={result['delay']}, "
              f"Size={result['messagesize']}, Count={result['instancecount']}, Sub_QoS={result['sub_qos']}")
        print(f"Message rate: {result['message_rate']:.2f} msg/s, Loss: {result['avg_message_loss']:.2f}%, "
              f"Out-of-order: {result['avg_out_of_order']:.2f}%, Duplicates: {result['avg_duplicates']:.2f}%")

    def run_for_wireshark_capture(self):
        """
        Run simplified tests just to capture QoS handshakes with Wireshark.
        This method runs one test for each QoS level with minimal configuration
        to make it easy to capture the handshakes in Wireshark.
        """
        print("Running tests for Wireshark QoS handshake capture...")

        # Wait for user to start Wireshark capture
        input("Make sure Wireshark is running and capturing with filter 'mqtt'. Press Enter to continue...")

        # Run one test for each QoS level with simple configuration
        for qos in [0, 1, 2]:
            print(f"\n===== Testing QoS {qos} handshake =====")
            print(f"Start watching Wireshark for QoS {qos} packets now!")

            # Use simple configuration:
            # - 1 publisher
            # - 0ms delay for single packet capture
            # - 0 message size for simplicity
            # - Same QoS for subscriber
            self.run_test(qos=qos,
                          delay=0,
                          messagesize=0,
                          instancecount=1,
                          sub_qos=qos)

            input(f"QoS {qos} test complete. Verify capture in Wireshark. Press Enter for next QoS level...")

        print("\nAll QoS handshake tests completed. You can stop Wireshark capture now.")

    def run_reduced_tests(self):
        """Run a reduced set of tests for quicker verification"""
        delays = [0]  # Just test with 0ms delay
        qos_levels = [0, 1]  # Test QoS 0 and 1 only
        message_sizes = [0]  # Test with empty messages only
        instance_counts = [1, 5]  # Test with 1 and 5 publishers
        sub_qos_levels = [0]  # Test with subscriber QoS 0 only

        total_tests = len(delays) * len(qos_levels) * len(message_sizes) * len(instance_counts) * len(sub_qos_levels)
        current_test = 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for sub_qos in sub_qos_levels:
            for qos in qos_levels:
                for delay in delays:
                    for messagesize in message_sizes:
                        for instancecount in instance_counts:
                            print(f"\nRunning test {current_test}/{total_tests}")
                            self.run_test(qos, delay, messagesize, instancecount, sub_qos)
                            current_test += 1

        # Save results
        self.save_results(timestamp)

    def run_all_tests(self):
        delays = [0, 100]
        qos_levels = [0, 1, 2]
        message_sizes = [0, 1000, 4000]
        instance_counts = [1, 5, 10]
        sub_qos_levels = [0, 1, 2]

        # original dataset is too long to test, the following is the test set
        # delays = [0]
        # qos_levels = [0, 1]
        # message_sizes = [0]
        # instance_counts = [1, 5]
        # sub_qos_levels = [0]

        total_tests = len(delays) * len(qos_levels) * len(message_sizes) * len(instance_counts) * len(sub_qos_levels)
        current_test = 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for sub_qos in sub_qos_levels:
            for qos in qos_levels:
                for delay in delays:
                    for messagesize in message_sizes:
                        for instancecount in instance_counts:
                            print(f"\nRunning test {current_test}/{total_tests}")
                            self.run_test(qos, delay, messagesize, instancecount, sub_qos)
                            current_test += 1

                            # Save intermediate results every 10 tests
                            if current_test % 10 == 0:
                                self.save_results(timestamp)

        # Save final results
        self.save_results(timestamp)

    def save_results(self, timestamp="final"):
        # Save results to CSV
        filename = f"results/mqtt_test_results_{timestamp}.csv"
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ["qos", "delay", "messagesize", "instancecount", "sub_qos",
                          "message_rate", "avg_message_loss", "avg_out_of_order",
                          "avg_duplicates", "avg_mean_gap", "avg_std_dev_gap"]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for result in self.results:
                # Remove sys_metrics from CSV output
                result_copy = {k: v for k, v in result.items() if k != "sys_metrics"}
                writer.writerow(result_copy)

        print(f"Results saved to {filename}")

        # Save sys metrics separately
        sys_filename = f"results/mqtt_sys_metrics_{timestamp}.json"
        with open(sys_filename, 'w') as jsonfile:
            sys_data = {i: result["sys_metrics"] for i, result in enumerate(self.results)}
            json.dump(sys_data, jsonfile, indent=2)

        print(f"System metrics saved to {sys_filename}")

    def connect(self):
        self.client.connect(self.broker_address, self.broker_port, 60)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()


# Run the analyzer
if __name__ == "__main__":
    # Get broker address from command line if provided
    broker_address = "localhost"
    if len(sys.argv) > 1:
        broker_address = sys.argv[1]

    analyzer = Analyzer(broker_address=broker_address)
    analyzer.connect()

    try:
        # Ask user what they want to do
        print("Select mode:")
        print("1. Run full test suite")
        print("2. Run reduced test set")
        print("3. Capture Wireshark handshakes only")

        choice = input("Enter your choice (1-3): ")

        if choice == "1":
            analyzer.run_all_tests()
        elif choice == "2":
            analyzer.run_reduced_tests()
        elif choice == "3":
            analyzer.run_for_wireshark_capture()
        else:
            print("Invalid choice. Exiting.")

    except KeyboardInterrupt:
        print("Tests interrupted by user")
    finally:
        analyzer.disconnect()