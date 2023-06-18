package de.limita.kafka.consumer.processor;

import de.limita.kafka.model.UserAction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class UserActionProcess implements MessageProcessor<String,UserAction> {
    private static final Logger log = LoggerFactory.getLogger(UserActionProcess.class);

    AtomicLong counter = new AtomicLong(0L);
    public void processUserAction(UserAction userAction) throws InterruptedException {
        Thread.sleep(200);
        log.info("User action {}",userAction);
        counter.incrementAndGet();
    }

    public long getCounter() {
        return counter.get();
    }

    @Override
    public void processMessages(Iterable<ConsumerRecord<String, UserAction>> values) throws InterruptedException  {
        for (ConsumerRecord<String, UserAction> value : values) {
            processUserAction(value.value());
        }
    }
}
