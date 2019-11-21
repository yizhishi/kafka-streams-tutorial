package org.demo.ks.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.demo.ks.config.ProducerProperties;

public class WordCountProducer {

  private static final String WORDCOUNT_SOURCE_TOPIC = "TextLinesTopic";
  private static final String WORDCOUNT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";

  public static void main(String[] args) throws Exception {
    RecordMetadata m = produce("hello world");
    System.out.println(m.topic() + "\t" + m.partition());
  }

  private static RecordMetadata produce(String v) throws Exception {
    return produce(v, WORDCOUNT_SOURCE_TOPIC);
  }

  private static RecordMetadata produce(String v, String topic) throws Exception {

    try (Producer<String, String> p = new KafkaProducer<String, String>(
        ProducerProperties.getProperties(WORDCOUNT_BOOTSTRAP_SERVERS_CONFIG))) {

      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, v, v);

      Future<RecordMetadata> f = p.send(record);

      return f.get();

    } catch (ExecutionException e) {
      throw e;
    }

  }

}
