
FROM bitnami/spark:latest

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar

ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]

CMD ["spark-shell"]


