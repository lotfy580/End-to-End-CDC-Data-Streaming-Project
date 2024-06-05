
# An End-to-End Change Data Capture (CDC) Real-Time Streaming System
<img src="https://github.com/lotfy580/End-to-End-CDC-Data-Streaming-Project/blob/main/kafka_spark_streaming_arch.png"></img>

I am excited to introduce my latest project, which has led me to explore wonderful tools and techniques. The project aims to establish a system that detects changes in customer data stored in a PostgreSQL database and processes this data in real time to serve a web application. The architecture used to achieve this is as follows:

<div> Debezium: Utilized with a PostgreSQL connection to detect row-level changes from the database log file.</div>

<div> Kafka Cluster: Established and connected to Debezium to capture data in real time and save it in a Kafka topic, making it ready for consumer subscriptions.</div>

<div> Apache Spark: The consumer is an Apache Spark application. With Spark's capabilities in fast processing and structured streaming, the data can be processed and transformed in real time.</div>

<div> Apache Cassandra: A Cassandra cluster was established to store the data in real time, excelling in this type of work due to its high read and write speed.</div>

Finally, the data is ready for our web application to consume. For this project, I used a simple web app to visualize the data in real time using Dash and Plotly packages, but the system can serve many other purposes as well.
