package de.limita.kafka.consumer;

import de.limita.kafka.consumer.processor.MessageProcessor;
import de.limita.kafka.consumer.retry.CountRetryStrategyBatch;
import de.limita.kafka.consumer.retry.RetryStrategyBatch;
import de.limita.kafka.consumer.worker.ParallelKafkaWorker;
import de.limita.kafka.model.UserAction;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class UserActionKafkaConsumer extends KafkaCustomConsumer<String, UserAction>{

    public UserActionKafkaConsumer(Consumer<String, UserAction> consumer,
                                   MessageProcessor<String, UserAction> processor,
                                   WorkerProperties workerProperties,
                                   @Value("${topic}") String topic,
                                   @Value("${poll.interval}") long pollIntervalMillis) {
        super(consumer,
                new ParallelKafkaWorker<>(RetryStrategyBatch.WITHOUT_RETRY,processor,workerProperties,"worker-"+topic),
                topic,
                pollIntervalMillis);
    }

}
