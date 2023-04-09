package graphlearning;

import org.flinkextended.flink.ml.operator.client.NodeUtils;
import org.flinkextended.flink.ml.operator.util.ReflectionUtils;
import org.flinkextended.flink.ml.pytorch.PyTorchClusterConfig;
import org.flinkextended.flink.ml.pytorch.PyTorchNodeIterationBody;
import org.flinkextended.flink.ml.pytorch.PyTorchUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.apache.flink.types.Row;

/** GraphPyTorchUtils. */
public class GraphPyTorchUtils extends PyTorchUtils {
    public static void train(
            StatementSet statementSet,
            DataStream<Row> inputDataStream,
            PyTorchClusterConfig pyTorchClusterConfig,
            Integer epoch) {
        final StreamTableEnvironmentImpl tEnv =
                ReflectionUtils.getFieldValue(
                        statementSet, StatementSetImpl.class, "tableEnvironment");
        final StreamExecutionEnvironment env = tEnv.execEnv();
        final Configuration flinkConfig = NodeUtils.mergeConfiguration(env, tEnv.getConfig());

        NodeUtils.scheduleAMNode(statementSet, pyTorchClusterConfig);

        final IterationConfig iterationConfig = IterationConfig.newBuilder().build();
        final DataStreamSource<Integer> dummyInitVariable = env.fromElements(0);
        final DataStreamList dataStreamList =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(dummyInitVariable),
                        ReplayableDataStreamList.replay(inputDataStream),
                        iterationConfig,
                        new PyTorchNodeIterationBody(
                                env, pyTorchClusterConfig, epoch, flinkConfig));

        final DataStream<Integer> trainResDataStream = dataStreamList.get(0);
        statementSet.addInsert(
                TableDescriptor.forConnector("blackhole").build(),
                tEnv.fromDataStream(trainResDataStream));
    }
}
