package graphlearning;
import graphlearning.protos.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaEventDeserializer implements DeserializationSchema<Event> {
    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        return Event.parseFrom(message);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }
}
