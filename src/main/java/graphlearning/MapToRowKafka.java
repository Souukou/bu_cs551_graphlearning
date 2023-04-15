package graphlearning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import graphlearning.protos.Event;
import org.apache.commons.lang3.NotImplementedException;

public class MapToRowKafka implements MapFunction<Event, Row> {

    private final String db_path;

    public MapToRowKafka(String path) {
        this.db_path = path;
    }
    /*
     * Input: Event
     * - timestamp: Current TimeStamp
     * - source: (integer) source node index
     * - target: (integer) target node index
     * - source_label: (integer) label of source node
     * - target_label: (integer) label of target node
     * - source_data_hex: (string) Byte String encoding of source embedding
     * - target_data_hex: (string) Byte String encoding of target node embedding
     */
    public Row map(Event event) {
        throw new NotImplementedException("This Function is yet to be implemented");

        return new Row();
    }
}
