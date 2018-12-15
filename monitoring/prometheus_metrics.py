from prometheus_client import Counter, generate_latest # noqa

# Prometheus Metrics
incoming_total = Counter('incoming_total', 'The total number of incoming messages')
incoming_processed = Counter('incoming_processed', 'The total number of incoming messages that were processed successfully')
incoming_ignored = Counter('incoming_ignored', 'The total number of incoming messages that were ignored due to missing required data')
incoming_listener_post_requests_total = Counter('incoming_listener_post_requests_total', 'The total number of post request attempts')
incoming_listener_post_requests_successful = Counter('incoming_listener_post_requests_successful', 'The total number of successful post requests')
incoming_listener_post_requests_readtimeout = Counter('incoming_listener_post_requests_readtimeout', 'The total number of post requests with read timeout errors')
incoming_listener_post_requests_connectionerror = Counter('incoming_listener_post_request_connectionerror', 'The total number of post requests with connection errors')
