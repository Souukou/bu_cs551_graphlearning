package graphlearning;

import org.flinkextended.flink.ml.pytorch.PyTorchClusterConfig;
import org.flinkextended.flink.ml.util.MLConstants;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.rocksdb.RocksDBException;

import java.util.concurrent.ExecutionException;

/** StreamingGraph. */
public class StreamingGraph {
    private static final String MODEL_PATH = "model-path";
    private static final String EPOCH = "epoch";
    private static final String SAMPLE_COUNT = "sample-count";
    private static final String MODE = "mode";
    private static final String PYSCRIPT = "pyscript";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException, RocksDBException {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        final String mode = params.get(MODE, "train");

        final String modelPath =
                params.get(MODEL_PATH, String.format("/tmp/linear/%s", System.currentTimeMillis()));
        final Integer epoch = Integer.valueOf(params.get(EPOCH, "1"));
        final Integer sampleCount = Integer.valueOf(params.get(SAMPLE_COUNT, "256000"));
        final String pyScript = params.get(PYSCRIPT, "");

        if (pyScript.length() == 0) {
            throw new RuntimeException(String.format("%s value not specified", pyScript));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final StreamStatementSet statementSet = tEnv.createStatementSet();

        final Table sample =
                tEnv.from(
                        TableDescriptor.forConnector("datagen")
                                .schema(
                                        Schema.newBuilder()
                                                .column("source", DataTypes.STRING())
                                                .column("neighbor", DataTypes.STRING())
                                                .column("label", DataTypes.STRING())
                                                .column("vectors", DataTypes.STRING())
                                                .build())
                                .option("number-of-rows", String.valueOf(sampleCount))
                                .build());

        DataStream<Row> inputStream = new InputStream().getStream(env);
        if ("train".equals(mode)) {
            train(modelPath, epoch, statementSet, inputStream, pyScript);
        }
    }

    private static void train(
            String modelPath,
            Integer epoch,
            StreamStatementSet statementSet,
            DataStream<Row> inputStream,
            String pyScript)
            throws InterruptedException, ExecutionException {
        final PyTorchClusterConfig config =
                PyTorchClusterConfig.newBuilder()
                        .setWorldSize(2)
                        .setNodeEntry(pyScript, "train")
                        .setProperty(
                                MLConstants.CONFIG_STORAGE_TYPE, MLConstants.STORAGE_LOCAL_FILE)
                        .setProperty("model_save_path", modelPath)
                        .setProperty("input_types", "INT_64,INT_64,STRING,STRING")
                        .build();

        GraphPyTorchUtils.train(statementSet, inputStream, config, epoch);
        statementSet.execute().await();
    }
}
