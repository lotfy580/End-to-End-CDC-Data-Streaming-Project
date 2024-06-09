from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark import SparkConf
import os


class KafkaSparkStream:
    
    def __init__(self):
        self.spark_configs= [
            ('spark.master', 'local[*]'),
            ('spark.jars.packages', 
             'com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'),
            ('spark.cassandra.connection.host', 'cassandra_db'),
            ('spark.cassandra.connection.port', '9042')
            
            ]
        
        
        
    def start_spark_session(self):
        try:
            conf=SparkConf()
            conf.setAll(self.spark_configs)
            spark=(SparkSession.builder.appName('kafka_spark_stream').config(conf=conf).getOrCreate())
            
            print(">>>>>>>>>>>>>>[Session Started]<<<<<<<<<<<<<<<<<")
            return spark
        except Exception as e:
            print(f">>>>>>>>>>>>>> ERROR [Starting Session]:{e}<<<<<<<<<<<<<<<<<")
            return None
    
    
    
    def read_from_kafka(self, spark):
        try:
            kafka_df=(
                spark.readStream
                .format("kafka")
                .option('kafka.bootstrap.servers', 'kafka:9092')
                .option('subscribe', 'sales.public.sales')
                .option('endingOffsets', 'latest')
                .load()
                )
            
            print(">>>>>>>>>>>>>>[Data Loaded]<<<<<<<<<<<<<<<<<")
            return kafka_df
        except Exception as e:
            print(f">>>>>>>>>>>>>> ERROR [reading from Kafka]:{e}<<<<<<<<<<<<<<<<<")
            return None
    
    
    
    def transform_data(self, kafka_df):
        data_schema=StructType(
            fields=[
                StructField('id', IntegerType(), False),
                StructField('first_name', StringType(), False),
                StructField('last_name', StringType(), False),
                StructField('user_name', StringType(), False),
                StructField('password', StringType(), False),
                StructField('addrss', StringType(), False),
                StructField('country', StringType(), False),
                StructField('email', StringType(), False),
                StructField('product', StringType(), False),
                StructField('price', IntegerType(), False),
                StructField('tax', DecimalType(3, 2), False),
                StructField('quantity', IntegerType(), False),
                StructField('delivary_fee', IntegerType(), False)
            ]
        )
        
        payload_schema=StructType(
            fields=[
               StructField("before", StringType(), False),
               StructField("after", data_schema, False),
               StructField("source", StringType(), False),
               StructField('opt', StringType(), False),
               StructField("ts_ms", StringType(), False),
               StructField('transaction', StringType(), False) 
            ]
        )
        
        schema=StructType(
            fields=[
                StructField("schema", StringType(), False),
                StructField("payload", payload_schema, False)  
            ]
        )
        
        try:
            df_tranformed=(
                kafka_df.selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema=schema).alias("data"))
                .select("data.payload.after.*")
                .withColumn('total_price', (col("price") + col('price')*col("tax"))*col("quantity")+col("delivary_fee"))
                .withColumn('create_date', current_timestamp())
            )
        
            print(">>>>>>>>>>>>>>[Data Transformed]<<<<<<<<<<<<<<<<<")
            return df_tranformed
        except Exception as e:
            print(f">>>>>>>>>>>>>> ERROR [Data Transformation]:{e}<<<<<<<<<<<<<<<<<")
            return None
        
    
    
    
    def insert_into_cassandra(self, df_transformed):
        try:
            df_transformed.writeStream\
            .trigger(processingTime="5 seconds") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="sales", keyspace="banana_ks", checkpointLocation='/home/checkpoint') \
            .outputMode("append")\
            .start()\
            .awaitTermination()
        
            print(">>>>>>>>>>>>>>[Data Inserted]*********************")
            
        except Exception as e:
            print(f">>>>>>>>>>>>>> ERROR [Cassandra Insertion]:{e}<<<<<<<<<<<<<<<<<")
        
            
    def start(self):
            spark=self.start_spark_session()
            if spark is not None:
                kafka_df=self.read_from_kafka(spark)
            if kafka_df is not None:
                df_transformed=self.transform_data(kafka_df)
            if df_transformed is not None:    
                self.insert_into_cassandra(df_transformed)

if __name__=="__main__":
    main=KafkaSparkStream()
    main.start()