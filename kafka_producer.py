import time
from kafka import KafkaProducer
import json
import os

bootstrap_servers = 'your_bootstrap_server'
kafka_topic = 'raspberrypilogs'
file_path = 'hdfs_json_path'

last_offset = 0

def read_and_send_to_kafka():
    global last_offset

    file_length = os.path.getsize(file_path)

    if last_offset >= file_length:
        print("Reached end of json, waiting for new data from hdfs")
        time.sleep(5)
        return

    with open(file_path, 'r') as hdfs_file:
        hdfs_file.seek(last_offset)

        producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        remaining_content = hdfs_file.read()

        for line in remaining_content.split('\n'):
            if not line:
                continue

            try:

                data = json.loads(line)
                for i in data:
                    print(i)
                    producer.send(kafka_topic, value=i)

            except json.JSONDecodeError as e:
                print(f"Hata: JSONDecodeError - {e}")

        producer.close()
        last_offset = hdfs_file.tell()


if __name__ == "__main__":
    while True:
        read_and_send_to_kafka()
        time.sleep(5)