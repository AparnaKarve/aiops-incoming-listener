import os

# pylama:ignore=C0103
bind = '127.0.0.1:8000'

workers = int(os.environ.get('GUNICORN_PROCESSES', '3'))
# worker_class = os.environ.get('GUNICORN_WORKER', 'gevent')

forwarded_allow_ips = '*'
secure_scheme_headers = {'X-Forwarded-Proto': 'https'}
