import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Test for RocksDBSourceFunction
 * */


public class RocksDBStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String nodesPath = "dataset-test/nodes.db";
        String edgesPath = "dataset-test/edges.db";
        String neighborPath = "dataset-test/neighbor.db";

        DataStreamSource<Tuple5<Integer, Short, Integer, byte[], String>> edgesStream = env.addSource(new RocksDBSourceFunction(
                nodesPath, edgesPath, neighborPath));

        edgesStream.print();

        env.execute("Edges Stream Test");
    }


}
