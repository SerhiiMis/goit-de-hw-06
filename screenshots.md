Step 1: Data Generation
In this step, I implemented a Python script (sensor_data_producer.py) to generate sensor data and send it to the Kafka topic building_sensors_serhii_mishovych. The script runs in an infinite loop and sends data with fields: sensor_id, temperature, humidity, and timestamp.

I ran two instances of this script in separate terminals to simulate multiple data producers running at the same time. Below is the screenshot demonstrating both terminals running the data generation script.
