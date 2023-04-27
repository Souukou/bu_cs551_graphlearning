package graphlearning;

import org.flinkextended.flink.ml.pytorch.PyTorchClusterConfig;
import org.flinkextended.flink.ml.util.MLConstants;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.rocksdb.RocksDBException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** StreamingGraph. */
public class StreamingGraph {
    private static final String MODEL_PATH = "model-path";
    private static final String EPOCH = "epoch";
    private static final String SAMPLE_COUNT = "sample-count";
    private static final String MODE = "mode";
    private static final String PYSCRIPT = "pyscript";
    private static final String PROP_FILE = "properties";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException, RocksDBException {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        final String mode = params.get(MODE, "train");
        final String propFile = params.get(PROP_FILE, "prop.config");
        final String modelPath =
                params.get(MODEL_PATH, String.format("/tmp/linear/%s", System.currentTimeMillis()));
        final Integer epoch = Integer.valueOf(params.get(EPOCH, "1"));
        final Integer sampleCount = Integer.valueOf(params.get(SAMPLE_COUNT, "256000"));
        final String pyScript = params.get(PYSCRIPT, "");
        Configuration conf = new Configuration();
        conf.setString("metrics.latency.interval", "3000");
        conf.setString("metrics.latency.granularity", "operator");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propFile));
        } catch (IOException ex) {
            System.out.println("Error Reading Properties");
        }

        if (pyScript.length() == 0) {
            throw new RuntimeException(String.format("%s value not specified", pyScript));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final StreamStatementSet statementSet = tEnv.createStatementSet();
        InputStream inputObj = new InputStream();
        DataStream<Row> inputStream = inputObj.getStream(env, propFile);
        if ("train".equals(mode)) {
            train(
                    modelPath,
                    epoch,
                    statementSet,
                    inputStream,
                    pyScript,
                    properties.getProperty("dataset.path"));
        }
    }

    private static void train(
            String modelPath,
            Integer epoch,
            StreamStatementSet statementSet,
            DataStream<Row> inputStream,
            String pyScript,
            String datasetPath)
            throws InterruptedException, ExecutionException {
        final PyTorchClusterConfig config =
                PyTorchClusterConfig.newBuilder()
                        .setWorldSize(2)
                        .setNodeEntry(pyScript, "train")
                        .setProperty(
                                MLConstants.CONFIG_STORAGE_TYPE, MLConstants.STORAGE_LOCAL_FILE)
                        .setProperty("model_save_path", modelPath)
                        .setProperty("dataset_path", datasetPath)
                        .setProperty("input_types", "INT_64,STRING")
                        .build();

        GraphPyTorchUtils.train(statementSet, inputStream, config, epoch);
        statementSet.execute();
    }
}
