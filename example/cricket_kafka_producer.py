import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests
from bs4 import BeautifulSoup
import re
import logging


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka broker configuration
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
kafka_topic = 'cricket_live_commentary'  # Replace with your desired topic name
commentary_url = "https://www.espncricinfo.com/series/icc-men-s-t20-world-cup-2024-1411166/india-vs-south-africa-final-1415755/ball-by-ball-commentary" # URL provided

#=== start

def fetch_commentary(url):
    """
    Fetches the ball-by-ball commentary from the given URL.  Handles potential errors.

    Args:
        url (str): The URL of the Cricinfo commentary page.

    Returns:
        list: A list of commentary events, or None on error.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Referer': 'https://www.espncricinfo.com/',  # Add the Referer header
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        commentary_divs = soup.find_all('div', class_='ds-flex ds-flex-col ds-py-2')
        commentary_data = []
        for div in commentary_divs:
            try:
                commentary_text = div.find('p', class_='ds-text-sm ds-font-regular ds-text-left ds-leading-3').text
                match = re.match(r"(\d+\.\d+)\s*Ov\s*,\s*(\d+)\s*ball\s*-\s*(.*)", commentary_text)
                if match:
                    # ... (rest of your parsing logic)
                    pass
                else:
                    logging.warning(f"Skipping unparseable commentary: {commentary_text}")
            except Exception as e:
                logging.error(f"Error parsing commentary item: {e}, text: {div.text}")
        return commentary_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching commentary: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

#=== end

def create_kafka_producer(broker):
    """
    Creates a Kafka producer instance.  Handles potential connection errors.

    Args:
        broker (str): The Kafka broker address.

    Returns:
        KafkaProducer: A Kafka producer instance, or None on error.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer
    except NoBrokersAvailable:
        logging.error(f"Could not connect to Kafka broker: {broker}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

def send_commentary_to_kafka(producer, topic, commentary_data):
    """
    Sends the commentary data to the Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send to.
        commentary_data (list): The list of commentary events.
    """
    if not producer:
        logging.error("Kafka producer is not initialized. Cannot send data.")
        return

    if not commentary_data:
        logging.info("No new commentary to send.")
        return

    for event in commentary_data:
        try:
            producer.send(topic, value=event)
            logging.info(f"Sent: {event['Over']}-{event['Ball']}") # reduced logging
            time.sleep(2)  # Simulate real-time updates, reduced to 2 seconds
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}, data: {event}")
            # Consider adding a retry mechanism here if network issues are common
    producer.flush() # Ensure all pending messages are delivered

def main():
    """
    Main function to fetch commentary and send it to Kafka.
    """
    producer = create_kafka_producer(kafka_broker)
    if not producer:
        logging.error("Failed to create Kafka producer. Exiting.")
        return  # Exit if producer creation fails

    while True:
        commentary = fetch_commentary(commentary_url)
        if commentary is not None: # Only proceed if fetch_commentary did not return None
            send_commentary_to_kafka(producer, kafka_topic, commentary)
        time.sleep(60)  # Fetch every 60 seconds, reduced from 300 to avoid overwhelming the server

if __name__ == "__main__":
    main()
