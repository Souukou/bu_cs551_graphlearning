import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Test for RocksDBSourceFunction
 * */

public class RocksDBStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String edgesDbPath = "dataset-test/edges.db";
        DataStreamSource<Tuple2<Integer, Integer>> edgesStream = env.addSource(new RocksDBSourceFunction(edgesDbPath));

        edgesStream.print();

        env.execute("Edges Stream Test");
    }

}
