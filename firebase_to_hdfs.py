import json
import firebase_admin
from firebase_admin import credentials, firestore
from hdfs import InsecureClient

cred = credentials.Certificate(
    "YOUR_JSON_CERTIFICATE_FILE_PATH"
)


if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

db = firestore.client()
collection_path = "logs"


def export_new_data():
    hdfs_client = InsecureClient('http://localhost:xxxx', user='username')

     try:
        with hdfs_client.read('hdfs_json_path', encoding='utf-8') as reader:
            all_data = json.load(reader)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        all_data = []

    sensor_ref = db.collection(collection_path)
    docs = sensor_ref.stream()

    new_data = []
    for doc in docs:
        data = doc.to_dict()
        timestamp = data.get("timestamp")

        if not any(existing_data.get("timestamp") == timestamp for existing_data in all_data):
            new_data.append(data)

                       doc.reference.update({"fetched": "True"})
            data["fetched"] = "True"

    all_data.extend(new_data)

    json_data = json.dumps(all_data)
    with hdfs_client.write('hdfs_json_path', encoding='utf-8', overwrite=True) as writer:
        writer.write(json_data)


    hdfs_client.download('hdfs_json_path', 'local_json_path',
                         overwrite=True)



export_new_data()