package de.limita.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.limita.kafka.model.UserAction;
import de.limita.kafka.producer.UserActionSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class UserActionDeserializer implements Deserializer<UserAction> {
    private static final Logger log = LoggerFactory.getLogger(UserActionDeserializer.class);
    private final ObjectMapper mapper;
    public UserActionDeserializer(){
        this.mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    public UserActionDeserializer(ObjectMapper mapper){
        this.mapper = mapper;
    }

    @Override
    public UserAction deserialize(String topic, byte[] data) {
        if(data.length==0)
            return null;
        try{
            return mapper.readValue(data,UserAction.class);
        } catch (IOException e) {
            String message = new String(data, StandardCharsets.UTF_8);
            log.error("Unable to deserialize message {}",message,e);
            return null;
        }
    }
}
