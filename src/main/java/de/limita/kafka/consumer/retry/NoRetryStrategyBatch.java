package de.limita.kafka.consumer.retry;

public class NoRetryStrategyBatch  implements RetryStrategyBatch{
    @Override
    public boolean isNeedToRetry(Exception e, int retryCount) {
        return false;
    }
}
