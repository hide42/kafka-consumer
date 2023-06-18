package de.limita.kafka.producer;

import de.limita.kafka.model.UserAction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
@Service
public class UserActionSendService {
    private static final Logger log = LoggerFactory.getLogger(UserActionSendService.class);

    private final String topic;
    private final Producer<String, UserAction> producer;

    public UserActionSendService(@Value("${topic}") String topic, Producer<String, UserAction> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    public CompletableFuture<RecordMetadata> sendUserAction(UserAction userAction) {//todo completable
        var rec = new ProducerRecord<>(topic,userAction.toString(),userAction);
        CallbackCompletableFuture completableFuture = new CallbackCompletableFuture();
        producer.send(rec,completableFuture);
        log.info("Sending message");
        return completableFuture;
    }
    private static class CallbackCompletableFuture extends CompletableFuture<RecordMetadata> implements Callback{
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            log.info("Completed sending message");
            if(exception==null){
                complete(metadata);
            }else{
                completeExceptionally(exception);
            }
        }
    }
}
