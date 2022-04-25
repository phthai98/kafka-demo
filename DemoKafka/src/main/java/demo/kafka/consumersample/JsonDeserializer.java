package demo.kafka.consumersample;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> className;
    public static final String KEY_CLASS_NAME_CONFIG = "key.class.name.config";
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name.config";

    @SneakyThrows
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        if (isKey) {
            className = (Class<T>) Class.forName((String) props.get(VALUE_CLASS_NAME_CONFIG));
            return;
        }
        className = (Class<T>) Class.forName((String) props.get(VALUE_CLASS_NAME_CONFIG));
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;

        try {
            return (T) objectMapper.readValue(data, className);
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }
}
