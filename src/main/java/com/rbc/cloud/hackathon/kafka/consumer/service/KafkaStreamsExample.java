package com.rbc.cloud.hackathon.kafka.consumer.service;

import com.rbc.cloud.hackathon.data.Transactions;
import com.rbc.cloud.hackathon.data.TransactionsSum;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;


/**
 * Created by chris on 12/15/17.
 */
public class KafkaStreamsExample {
    public static void main (String[] args){
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transactions> transactionsKStream = builder.stream("cmatta-transaction-topic-1");

        KTable<Windowed<String>, TransactionsSum> transactionsSumKTable = transactionsKStream
            .selectKey((k,v) -> v.getCustId().toString())
            .groupByKey()
            .windowedBy(TimeWindows.of(30 * 60 * 1000L))
            .aggregate(
                TransactionsSum::new,
                new Aggregator<String, Transactions, TransactionsSum>() {
                    @Override
                    public TransactionsSum apply(String key, Transactions value, TransactionsSum aggregate) {
                        aggregate.setCustId(key);
                        aggregate.setTransactionSum(
                            Integer.valueOf(value.getTransactionAmount().toString()) +
                                aggregate.getTransactionSum());
                        return aggregate;
                    }
                }, Materialized.as("TransactionSums"));

    }
}
