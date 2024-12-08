import uuid
from datetime import datetime, timedelta  # Import timedelta explicitly
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

# Define default arguments for the DAG
default_args = {
    'owner': 'fadwa',
    'start_date': datetime(2024, 12, 8, 12, 44),
    'retries': 1,  # Optional: number of retries
    'retry_delay': timedelta(minutes=5)  # Fixed timedelta usage
}

# Fetch data from the API
def get_data():
    response = requests.get("https://randomuser.me/api/")
    response.raise_for_status()  # Ensure we handle HTTP errors
    res = response.json()
    return res["results"][0]

# Format the API response into a structured format
def format_data(res):
    location = res['location']
    data = {
        'id': str(uuid.uuid4()),  # Generates and serializes a UUID
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium'],
    }
    return data

# Stream data to Kafka
def stream_data():
    from kafka import KafkaProducer
    import time
    import logging

    # Set up Kafka producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    
    

   
    start_time = time.time()

    while True:
        if time.time() > start_time + 60:  # Stop streaming after 1 minute
            break
        try:
            # Fetch and format data
            res = get_data()
            formatted_res = format_data(res)

            # Serialize data and send it to Kafka
            def custom_serializer(obj):
                if isinstance(obj, uuid.UUID):
                    return str(obj)
                raise TypeError(f"Type {type(obj)} not serializable")

            producer.send(
                'user_created',
                json.dumps(formatted_res, default=custom_serializer).encode('utf-8')
            )
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

# Uncomment this block to enable the DAG functionality if running in Airflow
with DAG('user_automation_pipeline',  # Task ID
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data
    )

# # For standalone testing
# if __name__ == "__main__":
# stream_data()
