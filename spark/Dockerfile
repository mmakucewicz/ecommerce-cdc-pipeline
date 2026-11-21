FROM spark:4.1.2-scala2.13-java17-python3-ubuntu

USER root

# Writable directories for the non-root "spark" user:
#  - conf-runtime: spark-defaults.conf rendered at container start (with secrets from env)
#  - ivy-cache:    downloaded Maven packages (Iceberg, AWS bundle, JDBC driver, Kafka connector)
RUN mkdir -p /opt/spark/conf-runtime /opt/spark/ivy-cache /opt/spark/conf-templates \
    && chown -R spark:spark /opt/spark/conf-runtime /opt/spark/ivy-cache

COPY --chown=spark:spark conf/spark-defaults.conf.template /opt/spark/conf-templates/spark-defaults.conf.template
COPY --chmod=755 start.sh /opt/spark/start.sh

USER spark

ENV SPARK_CONF_DIR=/opt/spark/conf-runtime

ENTRYPOINT ["/opt/spark/start.sh"]
