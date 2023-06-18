package de.limita.kafka.consumer.retry;

public interface RetryStrategyBatch {
    public static final RetryStrategyBatch WITHOUT_RETRY = new NoRetryStrategyBatch();
    boolean isNeedToRetry(Exception e, int retryCount);
}