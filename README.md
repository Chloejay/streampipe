# streampipe
real time data pipeline with kafka and spark.  

```
$ pyenv global 3.6.0
```

set up kafka, zookeeper and get it run </br>
#### test 
```
#!/streampipe/kafka/

$ brew services start zookeeper

$ kafka-server-start /usr/local/etc/kafka/server.properties

$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sparktest

$ kafka-console-consumer --bootstrap-server localhost:9092 --topic sparktest --from-beginning
```

then run spark streaming for data processing  

```
#config spark home in .bash_profile then run 

 $ /usr/local/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --master local[2] \
 --jars external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
 --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3  \
 kafkatest/kafka_spark_df.py localhost:2181 sparktest
 ```

```
#linking the artifact (optionally) 

$ /usr/local/spark-2.4.4-bin-hadoop2.7/bin/spark-submit  --master local[2] \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 kafkatest/kafka_spark_df.py
```

 optionally use Kafka Connect (out of message broker) 
 ```
#connect with JDBC conncetor 
$ bin/schema-registry-start etc/schema-registry/schema-registry.properties 
$ bin/connect-standalone etc/schema-registry/\
connect-avro-standalone.properties etc/kafka-connect-jdbc/\
sink-quickstart-sqlite.properties
$ bin/kafka-avro-console-producer --broker-list \
localhost:9092 --topic kafkatesting --property value.schema
```
