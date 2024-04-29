web: daphne -p $PORT --bind 0.0.0.0 server.asgi:application --websocket_timeout=-1 --websocket_connect_timeout=20
release: ./manage.py migrate --no-input && ./manage.py loaddata deepdive/fixtures/socialapps_prod.json
