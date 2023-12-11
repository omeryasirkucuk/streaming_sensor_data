from gpiozero import MotionSensor, LED
import time
from datetime import datetime
pir = MotionSensor(27)
led_pin = 17
led = LED(led_pin)


import firebase_admin
from firebase_admin import credentials, firestore


cred = credentials.Certificate(YOUR_JSON_CERTIFICATE_FILE_PATH)
firebase_admin.initialize_app(cred)

db = firestore.client()

collection_path = "logs"


try:
    time.sleep(2)
    while True:
        timestamp_name = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if pir.motion_detected:
            led.on()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data_to_send = {
                "room": "oyk_sahsi",
                "led_off_on": "True",
                "detection": "True",
                "timestamp": timestamp
            }
        else:
            led.off()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data_to_send = {
                "room": "oyk_sahsi",
                "led_off_on": "False",
                "detection": "False",
                "timestamp": timestamp
            }
        document_name = f"log_{timestamp_name}"
        db.collection(collection_path).document(document_name).set(data_to_send)
        time.sleep(3)

except Exception as e:
    print(f"Hata: {e}")
    print("İşlem iptal")