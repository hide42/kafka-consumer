package de.limita.kafka.consumer.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageProcessor<K,V>{
    void processMessages(Iterable<ConsumerRecord<K, V>> values) throws Exception;
}
