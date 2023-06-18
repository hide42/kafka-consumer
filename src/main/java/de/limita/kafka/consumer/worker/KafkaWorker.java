package de.limita.kafka.consumer.worker;

import de.limita.kafka.consumer.retry.RetryStrategyBatch;
import de.limita.kafka.consumer.processor.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public abstract class KafkaWorker<K,V> {
    final RetryStrategyBatch retryStrategy;
    final MessageProcessor<K,V> processor;

    protected KafkaWorker(RetryStrategyBatch retryStrategy, MessageProcessor<K,V> processor) {
        this.retryStrategy = retryStrategy;
        this.processor = processor;
    }


    abstract public void processingMessages(ConsumerRecords<K, V> consumerRecords) throws InterruptedException;

    abstract public void shutdown();
}
