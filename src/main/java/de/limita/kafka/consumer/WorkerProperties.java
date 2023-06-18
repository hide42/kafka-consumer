package de.limita.kafka.consumer;

import org.springframework.stereotype.Component;

@Component
public class WorkerProperties {
    private int threadCount = Runtime.getRuntime().availableProcessors();
    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
