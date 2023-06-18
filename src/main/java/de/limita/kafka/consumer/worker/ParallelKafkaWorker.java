package de.limita.kafka.consumer.worker;

import de.limita.kafka.consumer.retry.RetryStrategyBatch;
import de.limita.kafka.consumer.WorkerProperties;
import de.limita.kafka.consumer.processor.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ParallelKafkaWorker<K,V> extends KafkaWorker<K,V> {
    private final ExecutorService kafkaWorkerExecutor;

    private final AtomicLong threadId = new AtomicLong(0);

    private int countPartitions;
    public ParallelKafkaWorker(RetryStrategyBatch retryStrategy,
                               MessageProcessor<K,V> processor,
                               WorkerProperties workerProperties,
                               String threadName) {
        super(retryStrategy, processor);
        countPartitions = workerProperties.getThreadCount();
        kafkaWorkerExecutor = Executors.newFixedThreadPool(countPartitions,
                runnable -> new Thread(runnable, threadName+"-"+ threadId.incrementAndGet()));
    }

    @Override
    public void processingMessages(ConsumerRecords<K,V> consumerRecords) throws InterruptedException {
        Map<Integer, List<ConsumerRecord<K,V>>> actionsByPartition =
                StreamSupport.stream(consumerRecords.spliterator(), false)
                        .filter(record -> Objects.nonNull(record.value()))
                        .collect(Collectors.groupingBy(record -> record.key().hashCode() % countPartitions));
        CountDownLatch actionsProcessCount = new CountDownLatch(actionsByPartition.keySet().size());

        for(Integer partition: actionsByPartition.keySet()){
            kafkaWorkerExecutor.submit(()->{
                boolean continueRetries = true;
                int retryCount = 1;
                do{
                    try{
                        processor.processMessages(actionsByPartition.get(partition));
                        continueRetries=false;
                        actionsProcessCount.countDown();
                    }catch (Exception e){
                        continueRetries = retryStrategy.isNeedToRetry(e,retryCount);
                        retryCount++;
                    }
                }while (continueRetries);
            });
        }
        actionsProcessCount.await(1, TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        kafkaWorkerExecutor.shutdown();
        try {
            if (!kafkaWorkerExecutor.awaitTermination(800L, TimeUnit.MILLISECONDS)) {
                kafkaWorkerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            kafkaWorkerExecutor.shutdownNow();
        }
    }
}
