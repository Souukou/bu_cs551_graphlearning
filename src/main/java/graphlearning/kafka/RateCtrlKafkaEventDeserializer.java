package graphlearning.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import graphlearning.kafka.protos.Event;

import java.io.IOException;

/** RateCtrlKafkaEventDeserializer. * */
public class RateCtrlKafkaEventDeserializer extends KafkaEventDeserializer
        implements ResultTypeQueryable<Event> {
    private final long sleepTime;

    public RateCtrlKafkaEventDeserializer(long maxRatePerSecond) {
        this.sleepTime = 1000 / maxRatePerSecond;
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        Event result = super.deserialize(message);

        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ignored) {
        }

        return result;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return super.getProducedType();
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return super.isEndOfStream(nextElement);
    }
}
