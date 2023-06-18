package de.limita.kafka.controller;

import de.limita.kafka.consumer.UserActionKafkaConsumer;
import de.limita.kafka.model.UserAction;
import de.limita.kafka.producer.UserActionSendService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class UserActionController {
    private static final Logger log = LoggerFactory.getLogger(UserActionKafkaConsumer.class);

    private final UserActionSendService sendService;


    public UserActionController(UserActionSendService sendService) {
        this.sendService = sendService;
    }

    @PostMapping(value = "/userActionList", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void sendUserActions(@RequestBody List<UserAction> actions) throws Exception{
        var futures = actions.stream()
                .map(userAction -> sendService.sendUserAction(userAction))
                .toArray(CompletableFuture[]::new);
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures);
        allOf.get();
    }
}
