package de.limita.kafkaconsumer;

import de.limita.kafka.KafkaConsumerApplication;
import de.limita.kafka.model.UserAction;
import de.limita.kafka.producer.UserActionSendService;
import de.limita.kafka.consumer.processor.UserActionProcess;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@EnableKafka
@SpringBootTest(classes = {KafkaConsumerApplication.class})
@EmbeddedKafka(
		partitions = 1,
		topics = {"topic"},
		controlledShutdown = false)
class KafkaConsumerApplicationTests {
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	@Autowired
	UserActionSendService sendService;
	@Autowired
	UserActionProcess processor;

	@Test
	void contextLoads(){
		UserAction action = new UserAction();
		action.name = "hello";
		sendService.sendUserAction(action);
		while (processor.getCounter()!=1){

		}
	}

}
