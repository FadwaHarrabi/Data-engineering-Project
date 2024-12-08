import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator  # to fetch data
import json
import requests

default_args = {
    'owner': 'fadwa',
    'start_date': datetime(2024, 12, 8, 12, 44)
}  # used to attach the DAG

def get_data():
    response = requests.get("https://randomuser.me/api/")  # get data from API
    res = response.json()
    res = res["results"][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()  # Generates a UUID
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    from kafka import KafkaProducer
    import time

    def custom_serializer(obj):
        if isinstance(obj, uuid.UUID):  # Convert UUID to a string
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    res = get_data()
    formatted_res = format_data(res)
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('user_created', json.dumps(formatted_res, default=custom_serializer).encode('utf-8'))

# Uncomment this block to enable the DAG functionality if running in Airflow
# with DAG('user_automation',  # task ID
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id="stream_data_from_api",
#         python_callable=stream_data
#     )

# For standalone testing
# stream_data()
