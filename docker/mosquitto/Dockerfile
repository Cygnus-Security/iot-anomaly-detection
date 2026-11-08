FROM eclipse-mosquitto:2.1.2-alpine

USER root
COPY mosquitto.conf /mosquitto/config/mosquitto.conf
COPY lab-entrypoint.sh /usr/local/bin/lab-entrypoint.sh
RUN chmod 0755 /usr/local/bin/lab-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/lab-entrypoint.sh"]
