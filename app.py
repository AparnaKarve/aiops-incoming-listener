import os
import logging
import sys
from json import loads
from uuid import uuid4

from kafka import KafkaConsumer
import requests

from monitoring import prometheus_metrics

# Setup logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)


def hit_next_in_pipepine(payload: dict) -> None:
    """Pass the data to next service in line."""
    host = os.environ.get('NEXT_MICROSERVICE_HOST')

    # FIXME: Convert to aiohttp or some other async requests alternative
    # Do not wait for response now, so we can keep listening
    prometheus_metrics.incoming_listener_post_requests_total.inc()
    try:
        requests.post(f'http://{host}', json=payload, timeout=10)
    except requests.exceptions.ReadTimeout:
        prometheus_metrics.incoming_listener_post_requests_readtimeout.inc()
        pass
    except requests.exceptions.ConnectionError as e:
        prometheus_metrics.incoming_listener_post_requests_connectionerror.inc()
        logger.warning('Call to next service failed: %s', str(e))

    prometheus_metrics.incoming_listener_post_requests_successful.inc()


def listen() -> dict:
    """Kafka messages listener.

    Connects to Kafka server, consumes a topic and yields every message it
    receives.
    """
    server = os.environ.get('KAFKA_SERVER')
    topic = os.environ.get('KAFKA_TOPIC')
    group_id = os.environ.get('KAFKA_CLIENT_GROUP')
    client_id = uuid4()

    logger.info('Connecting to Kafka server...')
    logger.info('Client configuration:')
    logger.info('\tserver:    %s', server)
    logger.info('\ttopic:     %s', topic)
    logger.info('\tgroup_id:  %s', group_id)
    logger.info('\tclient_id: %s', client_id)

    consumer = KafkaConsumer(
        topic,
        client_id=client_id,
        group_id=group_id,
        bootstrap_servers=server
    )

    logger.info('Consumer subscribed and active!')

    for msg in consumer:
        prometheus_metrics.incoming_total.inc()
        logger.debug('Received message: %s', str(msg))
        yield loads(msg.value)


if __name__ == '__main__':
    # Check environment variables passed to container
    # pylama:ignore=C0103
    env = {'KAFKA_SERVER', 'KAFKA_TOPIC', 'NEXT_MICROSERVICE_HOST'}

    if not env.issubset(os.environ):
        logger.error(
            'Environment not set properly, missing %s',
            env - set(os.environ)
        )
        sys.exit(1)

    # Run the consumer
    for received in listen():
        if 'url' not in received:
            prometheus_metrics.incoming_ignored.inc()
            logger.warning(
                'Message is missing data location URL. Message ignored: %s',
                received
            )
            continue

        message = {
            'url': received.get('url'),
            'origin': 'kafka',
            'metadata': {
                'rh_account': received.get('rh_account', None)
            }
        }
        prometheus_metrics.incoming_processed.inc()

        hit_next_in_pipepine(message)
