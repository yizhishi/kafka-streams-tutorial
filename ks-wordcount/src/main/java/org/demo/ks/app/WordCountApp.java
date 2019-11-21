package org.demo.ks.app;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCountApp {

  private static final String KAFKA = "localhost:9092";

  public static void run() {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application1");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // stream & source topic
    KStream<String, String> stream = builder.stream("TextLinesTopic");

    // table
    KTable<String, Long> table = stream

        .peek((key, word) -> System.out.println("key: " + key + "\t\tvalue: " + word)) // KStream

        .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+"))) // KStream

        .groupBy((key, word) -> word) // KGroupedStream

        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")); // KTable


    // target topic
    table.toStream() // KStream

        .peek((key, word) -> System.out.println("key: " + key + "\t\tvalue: " + word)) // KStream

        .to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    streams.start();

  }

}
