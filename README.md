# Kafka Consumer
---
### A consumer for reading string messages on kafka

**Prerequisites**
* Apache Maven command line - https://maven.apache.org/download.cgi
* JDK >= 1.8.0_101
* Confluent Cloud username and password provided by Cloud team

**Instructions to run:**
1. git clone on your local machine
2. go to the root dir of the git clone and mvn package
3. java -jar target/consumer-0.0.1-SNAPSHOT.jar --kafka.username=xxxxxx --kafka.password=xxxxx --writable.dir=/tmp

For step 3 you'll need to provide credentials to Confluent Cloud that were provided to you.  
The api.key is the username and api.secret is the password.
You'll also need to make sure you pass it a valid writable directory absolute path for writable.dir, in the above example /tmp is used.  
If that location is writable then that can be used.

This app takes the avro schema for transactions and listens for messages sent to the transactions
topic, and receives and deserializes those messages back into transaction pojos.