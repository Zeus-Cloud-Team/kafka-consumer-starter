package com.rbc.cloud.hackathon.kafka.consumer.config;


import com.rbc.cloud.hackathon.data.Transactions;
import com.rbc.cloud.hackathon.kafka.consumer.util.JavaVersion;
import com.rbc.cloud.hackathon.kafka.consumer.util.Util;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@EnableKafka
@PropertySource(value = "classpath:application.properties")
public class KafkaConsumerConfig {
    private Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Resource
    private Environment env;

    @Autowired
    private ResourceLoader resourceLoader;

    @Bean(name="ZeusListenerFactory")
    ConcurrentKafkaListenerContainerFactory<String, Transactions> zeusConcurrentKafkaListenerFactory() {

        validationChecks();

        final Map<String, Object> properties = new HashMap<>();

        properties.put("bootstrap.servers",env.getProperty("kafka.bootstrap.servers"));
        properties.put("schema.registry.url",env.getProperty("schema.registry.url"));
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        properties.put("enable.auto.commit",env.getProperty("enable.auto.commit"));
        properties.put("session.timeout.ms",env.getProperty("session.timeout.ms"));
        properties.put("auto.offset.reset",env.getProperty("auto.offset.reset"));
        properties.put("fetch.max.wait.ms",env.getProperty("fetch.max.wait.ms"));
        properties.put("max.partition.fetch.bytes",env.getProperty("max.partition.fetch.bytes"));
        properties.put("max.poll.records",env.getProperty("max.poll.records"));

        properties.put("group.id",env.getProperty("group.id"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        try {
            properties.put("client.id", InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            logger.error("Could not set client.id - {}",e.getMessage());
        }


        properties.put("sasl.jaas.config", env.getProperty("sasl.jaas.config") );
        properties.put("sasl.mechanism", env.getProperty("sasl.mechanism") );
        properties.put("security.protocol", env.getProperty("security.protocol") );

        String writableDir=env.getProperty("writable.dir");
        String jaasFile=null;
        try {
            jaasFile= Util.writeJaasFile(new File(writableDir), env.getProperty("kafka.username"), env.getProperty("kafka.password"));
        }
        catch (Exception e) {
            String message="Error trying to write Jaas file - {}"+e.getMessage();
            logger.error(message);
            e.printStackTrace();
            throw new RuntimeException(message);
        }

        try {
            System.setProperty("java.security.auth.login.config",resourceLoader.getResource("file:/"+jaasFile).getURI().toString() );
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        final ConcurrentKafkaListenerContainerFactory<String, Transactions> factory=new ConcurrentKafkaListenerContainerFactory<>();
        factory.setBatchListener(Boolean.valueOf(env.getProperty("batch.listener")));
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties));
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
        factory.getContainerProperties().setAckOnError(false);

        return factory;
    }

    private Boolean exists(String propertyName) {
        try {
            String val = env.getProperty(propertyName);
            return val != null && !val.equals("");
        } catch(Exception e) {
            return false;
        }
    }

    private void validationChecks() {
        final Properties props = new Properties();

        if (exists("kafka.username") && exists("kafka.password")) {
            logger.info("Found credentials");
        } else {
            String message="either kafka.username or kafka.password is not set, please set both and rerun";
            logger.error(message);
            throw new RuntimeException(message);
        }

        String javaVersion=System.getProperty("java.version");
        logger.info("your Java version {}, minimum required is 1.8.0_101", javaVersion);

        JavaVersion myJavaVersion=new JavaVersion(javaVersion);
        JavaVersion minimumJavaVersion=new JavaVersion("1.8.0_101");

        if (myJavaVersion.greaterThan(minimumJavaVersion)) {
            logger.info("Java version is fine!");
        } else {
            String message="Incompatible Java version! Please upgrade java to be at least 1.8.0_101, you are at "+javaVersion;
            logger.error(message);
            throw new RuntimeException(message);
        }

        String writableDir=null;
        if (exists("writable.dir") && new File(env.getProperty("writable.dir")).canWrite()) {

        }
        else  {
            String message="Please provide a dir path in application property writable.dir that can be written to";
            logger.error(message);
            throw new RuntimeException(message);
        }



    }

}
