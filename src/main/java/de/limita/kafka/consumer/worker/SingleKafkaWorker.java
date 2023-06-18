package de.limita.kafka.consumer.worker;

import de.limita.kafka.consumer.processor.MessageProcessor;
import de.limita.kafka.consumer.retry.RetryStrategyBatch;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class SingleKafkaWorker<K,V> extends KafkaWorker<K,V> {

    public SingleKafkaWorker(RetryStrategyBatch retryStrategy, MessageProcessor<K,V> processor) {
        super(retryStrategy, processor);
    }

    @Override
    public void processingMessages(ConsumerRecords<K,V> consumerRecords){
        try {
            boolean continueRetries = true;
            int retryCount = 1;
            do{
                try{
                    processor.processMessages(consumerRecords);
                    continueRetries=false;
                }catch (Exception e){
                    continueRetries = retryStrategy.isNeedToRetry(e,retryCount);
                    retryCount++;
                }
            }while (continueRetries);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        //pass
    }
}
