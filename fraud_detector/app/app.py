import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server, Summary, Counter, Histogram, Gauge

sys.path.append(os.path.abspath('./src'))
from preprocessing import run_preproc
from scorer import make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

PROCESSING_TIME = Summary('transaction_processing_seconds', 'Время обработки транзакции')
TRANSACTION_COUNT = Counter('transactions_total', 'Общее количество обработанных транзакций',
                            ['us_state', 'merch'])

FRAUD_SCORE = Histogram('fraud_score', 'Распределение скоров мошенничества',
                        buckets=[i / 50.0 for i in range(51)])
FRAUD_RATIO = Gauge('fraud_ratio', 'Соотношение мошеннических транзакций к общему числу')


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)

        self.total_transactions = 0
        self.fraud_transactions = 0

        start_http_server(8000)
        logger.info("Prometheus метрики доступны на порту 8000")

    @PROCESSING_TIME.time()
    def process_message(self, msg):
        try:
            data = json.loads(msg.value().decode('utf-8'))

            transaction_id = data['transaction_id']
            input_dict = data['data']

            us_state = str(input_dict.get('us_state', '') or 'unknown')
            merch = str(input_dict.get('merch', '') or 'unknown')
            cat_id = str(input_dict.get('cat_id', '') or 'unknown')

            input_df = pd.DataFrame([input_dict])

            processed_df = run_preproc(input_df)
            submission, y_proba = make_pred(processed_df, "kafka_stream")

            TRANSACTION_COUNT.labels(us_state=us_state, merch=merch).inc()
            FRAUD_SCORE.observe(y_proba[0])

            self.total_transactions += 1
            if y_proba[0] > 0.5:
                self.fraud_transactions += 1

            if self.total_transactions > 0:
                FRAUD_RATIO.set(self.fraud_transactions / self.total_transactions)

            submission['transaction_id'] = transaction_id
            submission['us_state'] = us_state
            submission['merch'] = merch
            submission['cat_id'] = cat_id

            self.producer.produce(
                SCORING_TOPIC,
                value=submission.to_json(orient='records'),
            )
            self.producer.flush()
            return True
        except Exception as e:
            logger.exception("Error processing message: %s", e)
            return False

    def process_messages(self):
        logger.info("Fraud detector waiting for messages on topic '%s' ...", TRANSACTIONS_TOPIC)
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            self.process_message(msg)


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
