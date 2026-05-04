from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

from model import VitalEvent, event_serializer
from model import generate_vital_event

from kafka import KafkaProducer
from typing import Iterator
import random
import pandas as pd

subject_id_csv = pd.read_csv("/home/hadoop/project-de-zoomcamp/data/patients.csv")

def event_stream() -> Iterator[VitalEvent]:
    subject_ids = subject_id_csv['patient'].tolist()
    device_ids = [f"{10000 + i}" for i in range(1, 26)]
    subject_device_map = dict(zip(subject_ids, random.choices(device_ids, k=len(subject_ids))))

    while True:
        subject_id = random.choice(subject_ids)
        device_id = subject_device_map[subject_id]
        yield generate_vital_event(subject_id, device_id)
        

if __name__ == "__main__":
    topic = "healthcare_vitals"
    server = "localhost:9092"
    count = 0
    
    producer = KafkaProducer(
        bootstrap_servers=[server], 
        value_serializer=event_serializer
    )
    
    print(f"Producing events to topic '{topic}' on server '{server}'...")
    
    try: 
        for event in event_stream():
            producer.send(topic, value=event)
            print(f"{event}")
            time.sleep(0.1)
            count += 1
    
    except KeyboardInterrupt:
        print("Interrupted by user")
        print(f"Total events produced: {count}")
    
    finally:
        producer.flush()