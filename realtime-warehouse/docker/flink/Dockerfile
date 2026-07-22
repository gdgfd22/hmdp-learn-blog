FROM flink:1.20.1-scala_2.12-java11

ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.4.0/flink-sql-connector-mysql-cdc-3.4.0.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/org/apache/doris/flink-doris-connector-1.20/25.1.0/flink-doris-connector-1.20-25.1.0.jar /opt/flink/lib/
ADD https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar /opt/flink/lib/

RUN chmod 0644 /opt/flink/lib/*.jar
