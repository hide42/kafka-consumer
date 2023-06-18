package de.limita.kafka.consumer;

import de.limita.kafka.consumer.worker.KafkaWorker;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import java.time.Duration;
import java.util.List;
public class KafkaCustomConsumer<K,V> {
    private static final Logger log = LoggerFactory.getLogger(KafkaCustomConsumer.class);
    private final Consumer<K,V> consumer;
    private final String topic;
    private final KafkaWorker<K,V> kafkaWorker;
    private final long pollIntervalMillis;

    public KafkaCustomConsumer(Consumer<K,V> consumer,
                               KafkaWorker<K,V> kafkaWorker,
                               String topic,
                               long pollIntervalMillis) {
        this.consumer = consumer;
        this.topic = topic;
        this.kafkaWorker = kafkaWorker;
        this.pollIntervalMillis = pollIntervalMillis;
    }

    private final Thread consumerThread = new Thread(this::runConsumer, this.getClass().getSimpleName());
    private volatile boolean continuePolling = true;

    @EventListener
    public void startEventCycle(ContextRefreshedEvent event) {
        consumerThread.start();
    }

    @PreDestroy
    public void stopEventCycle() {
        continuePolling = false;
        consumer.wakeup();
        kafkaWorker.shutdown();
        consumer.close();
    }

    private void runConsumer() {
        try {
            consumer.subscribe(List.of(topic));
            while (continuePolling) {
                ConsumerRecords<K,V> consumerRecords = consumer.poll(Duration.ofMillis(pollIntervalMillis));
                int count = consumerRecords.count();
                if (count > 0)
                    log.info("Processing messages of count {}", count);
                else continue;
                kafkaWorker.processingMessages(consumerRecords);
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("Stopping consumer", e);
        } catch (InterruptedException e) {
            kafkaWorker.shutdown();
            throw new RuntimeException(e);
        } finally {
            consumer.unsubscribe();
        }
    }
}
