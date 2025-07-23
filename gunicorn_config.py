import os

workers = int(os.environ.get('GUNICORN_PROCESSES', '2'))

threads = int(os.environ.get('GUNICORN_THREADS', '4'))

bind = os.environ.get('GUNICORN_BIND',
                      '{0}:{1}'.format(os.getenv('FLASK_HOST', '127.0.0.1'), os.getenv('FLASK_PORT', '8338')))


forwarded_allow_ips = '*'

secure_scheme_headers = { 'X-Forwarded-Proto': 'https' }
