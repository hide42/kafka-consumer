package de.limita.kafka.consumer.retry;

public class CountRetryStrategyBatch implements RetryStrategyBatch {
    private int MAX_RETRIES;

    public CountRetryStrategyBatch(int MAX_RETRIES) {
        this.MAX_RETRIES = MAX_RETRIES;
    }

    @Override
    public boolean isNeedToRetry(Exception e, int retryCount) {
        //any exception => retry
        return retryCount<MAX_RETRIES;
    }
}
