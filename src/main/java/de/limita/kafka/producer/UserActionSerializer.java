package de.limita.kafka.producer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.limita.kafka.model.UserAction;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserActionSerializer implements Serializer<UserAction> {
    private static final Logger log = LoggerFactory.getLogger(UserActionSerializer.class);
    private final ObjectMapper mapper;
    public UserActionSerializer(){
        this.mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    public UserActionSerializer(ObjectMapper mapper){
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(String topic, UserAction data) {
        try{
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
            return new byte[0];
        }
    }
}
