import os
import logging
import sys
import threading
from json import loads
from uuid import uuid4

from flask import Flask
from flask.logging import default_handler
from kafka import KafkaConsumer
import requests

from monitoring import prometheus_metrics

application = Flask(__name__)  # noqa


@application.route('/metrics')
def metrics():
    """Metrics route."""
    return prometheus_metrics.generate_latest()

@application.route('/hi')
def hi():
    """Metrics route."""
    return 'Hi'

# # Setup logging
# logging.basicConfig(level=logging.WARNING)
# logger = logging.getLogger('consumer')
# logger.setLevel(logging.DEBUG)

# Set up logging
ROOT_LOGGER = logging.getLogger()
ROOT_LOGGER.setLevel(application.logger.level)
ROOT_LOGGER.addHandler(default_handler)


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
        ROOT_LOGGER.warning('Call to next service failed: %s', str(e))

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

    ROOT_LOGGER.info('Connecting to Kafka server...')
    ROOT_LOGGER.info('Client configuration:')
    ROOT_LOGGER.info('\tserver:    %s', server)
    ROOT_LOGGER.info('\ttopic:     %s', topic)
    ROOT_LOGGER.info('\tgroup_id:  %s', group_id)
    ROOT_LOGGER.info('\tclient_id: %s', client_id)

    consumer = KafkaConsumer(
        topic,
        client_id=client_id,
        group_id=group_id,
        bootstrap_servers=server
    )

    ROOT_LOGGER.info('Consumer subscribed and active!')

    for msg in consumer:
        prometheus_metrics.incoming_total.inc()
        ROOT_LOGGER.debug('Received message: %s', str(msg))
        yield loads(msg.value)

def run_consumer():
    # Check environment variables passed to container
    # pylama:ignore=C0103
    env = {'KAFKA_SERVER', 'KAFKA_TOPIC', 'NEXT_MICROSERVICE_HOST'}

    if not env.issubset(os.environ):
        ROOT_LOGGER.error(
            'Environment not set properly, missing %s',
            env - set(os.environ)
        )
        sys.exit(1)

    # Run the consumer
    for received in listen():
        if 'url' not in received:
            prometheus_metrics.incoming_ignored.inc()
            ROOT_LOGGER.warning(
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


if __name__ == '__main__':
    t = threading.Thread(target=run_consumer)
    t.start()
    application.run()
