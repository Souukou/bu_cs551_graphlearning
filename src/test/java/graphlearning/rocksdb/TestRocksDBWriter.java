package graphlearning.rocksdb;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Test RocksDB. */
public class TestRocksDBWriter {
    static {
        RocksDB.loadLibrary();
    }

    static String nodePath = "dataset-ut/node.db";
    static String edgePath = "dataset-ut/edge.db";
    static String neighborPath = "dataset-ut/neighbor.db";

    static void createDirIfNotExists(String dbPath) {
        Path dbDirectory = Paths.get(dbPath).getParent();
        // Create the directory if it doesn't exist
        if (!Files.exists(dbDirectory)) {
            try {
                Files.createDirectories(dbDirectory);
            } catch (IOException e) {
                System.err.println("Error creating directory for RocksDB: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        }
    }

    @AfterEach
    void tearDownRocksDB() {
        // Delete the database created in unit test
        try {
            FileUtils.deleteDirectory(new File(nodePath));
            FileUtils.deleteDirectory(new File(edgePath));
            FileUtils.deleteDirectory(new File(neighborPath));
        } catch (Exception e) {
            System.err.println("Error when delete RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }
    }

    // insert node is disabled
    // @Test
    void testInsertNode1() {
        RocksDBWriter rocksDBWriter = new RocksDBWriter(nodePath, edgePath, neighborPath);

        // insert node
        rocksDBWriter.insertNode(1, "1|0.01,0.02,0.03,0.04,0.05");
        rocksDBWriter.insertNode(2, "2|0.02,0.03,0.04,0.05,0.06");
        rocksDBWriter.insertNode(3, "3|0.03,0.04,0.05,0.06,0.07");
        rocksDBWriter.insertNode(4, "4|0.04,0.05,0.06,0.07,0.08");
        rocksDBWriter.insertNode(5, "5|0.05,0.06,0.07,0.08,0.09");
        rocksDBWriter.insertNode(6, "6|0.06,0.07,0.08,0.09,0.10");
        rocksDBWriter.insertNode(7, "7|0.07,0.08,0.09,0.10,0.11");
        rocksDBWriter.insertNode(8, "8|0.08,0.09,0.10,0.11,0.12");
        rocksDBWriter.insertNode(9, "9|0.09,0.10,0.11,0.12,0.13");
        rocksDBWriter.insertNode(10, "0|0.10,0.11,0.12,0.13,0.14");
        rocksDBWriter.insertNode(11, "1|0.11,0.12,0.13,0.14,0.15");
        rocksDBWriter.insertNode(12, "2|0.12,0.13,0.14,0.15,0.16");
        rocksDBWriter.insertNode(13, "3|0.13,0.14,0.15,0.16,0.17");
        rocksDBWriter.insertNode(14, "4|0.14,0.15,0.16,0.17,0.18");

        // test node
        try (RocksDB db = RocksDB.openReadOnly(nodePath)) {
            Assertions.assertArrayEquals(
                    "6|0.06,0.07,0.08,0.09,0.10".getBytes(), db.get("6".getBytes()));

            Assertions.assertArrayEquals(
                    "3|0.13,0.14,0.15,0.16,0.17".getBytes(), db.get("13".getBytes()));
            Assertions.assertNull(db.get("15".getBytes()));
            Assertions.assertNull(db.get("-1".getBytes()));
            Assertions.assertNull(db.get("16".getBytes()));
        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        // test duplicate write, should ignore
        // rocksDBWriter.insertNode(1, "1|xxxx");
        // try (RocksDB db = RocksDB.openReadOnly(nodePath)) {
        //     Assertions.assertArrayEquals(
        //             "1|0.01,0.02,0.03,0.04,0.05".getBytes(), db.get("1".getBytes()));

        // } catch (RocksDBException e) {
        //     System.err.println("Error working with RocksDB: " + e.getMessage());
        //     e.printStackTrace();
        //     Assertions.assertTrue(false);
        // }

        rocksDBWriter.finalize();
    }

    @Test
    void testInsertEdge() {
        RocksDBWriter rocksDBWriter = new RocksDBWriter(nodePath, edgePath, neighborPath);

        // insert edge
        rocksDBWriter.insertEdge(1, 2);
        rocksDBWriter.insertEdge(1, 3);
        rocksDBWriter.insertEdge(1, 4);
        rocksDBWriter.insertEdge(1, 5);

        rocksDBWriter.insertEdge(2, 3);
        rocksDBWriter.insertEdge(2, 4);

        // test edge
        Options options = new Options();
        ComparatorOptions comparatorOptions = new ComparatorOptions();
        options.setCreateIfMissing(true);
        options.setComparator(new OrderByCountComparator(comparatorOptions));
        try (RocksDB edgeDb = RocksDB.openReadOnly(options, edgePath);
                RocksDB neighborDb = RocksDB.openReadOnly(neighborPath)) {
            Assertions.assertArrayEquals("2".getBytes(), edgeDb.get("1|0".getBytes()));
            Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("1|1".getBytes()));
            Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("1|2".getBytes()));
            Assertions.assertArrayEquals("5".getBytes(), edgeDb.get("1|3".getBytes()));
            Assertions.assertNull(edgeDb.get("1|5".getBytes()));
            Assertions.assertNull(edgeDb.get("1|-1".getBytes()));
            Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("2|0".getBytes()));
            Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("2|1".getBytes()));
            Assertions.assertNull(edgeDb.get("2|2".getBytes()));
            Assertions.assertNull(edgeDb.get("3|0".getBytes()));

            Assertions.assertArrayEquals("4".getBytes(), neighborDb.get("1".getBytes()));
            Assertions.assertArrayEquals("2".getBytes(), neighborDb.get("2".getBytes()));
        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        // test duplicate write, should ignore
        // rocksDBWriter.insertEdge(1, 2);

        // try (RocksDB edgeDb = RocksDB.openReadOnly(edgePath);
        //         RocksDB neighborDb = RocksDB.openReadOnly(neighborPath)) {
        //     Assertions.assertArrayEquals("2".getBytes(), edgeDb.get("1|0".getBytes()));
        //     Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("1|1".getBytes()));
        //     Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("1|2".getBytes()));
        //     Assertions.assertArrayEquals("5".getBytes(), edgeDb.get("1|3".getBytes()));
        //     Assertions.assertNull(edgeDb.get("1|5".getBytes()));
        //     Assertions.assertNull(edgeDb.get("1|-1".getBytes()));
        //     Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("2|0".getBytes()));
        //     Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("2|1".getBytes()));
        //     Assertions.assertNull(edgeDb.get("2|2".getBytes()));
        //     Assertions.assertNull(edgeDb.get("3|0".getBytes()));

        //     Assertions.assertArrayEquals("4".getBytes(), neighborDb.get("1".getBytes()));
        //     Assertions.assertArrayEquals("2".getBytes(), neighborDb.get("2".getBytes()));
        // } catch (RocksDBException e) {
        //     System.err.println("Error working with RocksDB: " + e.getMessage());
        //     e.printStackTrace();
        //     Assertions.assertTrue(false);
        // }

        rocksDBWriter.finalize();
    }

    // @Test
    void testInsertUndirectedEdge() {
        RocksDBWriter rocksDBWriter = new RocksDBWriter(nodePath, edgePath, neighborPath);

        // insert edge
        rocksDBWriter.insertEdge(1, 2);

        rocksDBWriter.insertEdge(2, 1);
        rocksDBWriter.insertEdge(1, 3);
        rocksDBWriter.insertEdge(3, 1);
        rocksDBWriter.insertEdge(1, 4);
        rocksDBWriter.insertEdge(4, 1);
        rocksDBWriter.insertEdge(1, 5);
        rocksDBWriter.insertEdge(5, 1);

        rocksDBWriter.insertEdge(2, 3);
        rocksDBWriter.insertEdge(3, 2);
        rocksDBWriter.insertEdge(4, 2);

        // test edge
        try (RocksDB edgeDb = RocksDB.openReadOnly(edgePath);
                RocksDB neighborDb = RocksDB.openReadOnly(neighborPath)) {
            Assertions.assertArrayEquals("2".getBytes(), edgeDb.get("1|0".getBytes()));
            Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("1|1".getBytes()));
            Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("1|2".getBytes()));
            Assertions.assertArrayEquals("5".getBytes(), edgeDb.get("1|3".getBytes()));
            Assertions.assertNull(edgeDb.get("1|5".getBytes()));
            Assertions.assertNull(edgeDb.get("1|-1".getBytes()));
            Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("2|0".getBytes()));
            Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("2|1".getBytes()));
            Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("2|2".getBytes()));
            Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("3|0".getBytes()));
            Assertions.assertArrayEquals("2".getBytes(), edgeDb.get("3|1".getBytes()));
            Assertions.assertNull(edgeDb.get("3|2".getBytes()));
            Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("4|0".getBytes()));
            Assertions.assertNull(edgeDb.get("4|1".getBytes()));
            Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("5|0".getBytes()));
            Assertions.assertNull(edgeDb.get("5|1".getBytes()));

            Assertions.assertArrayEquals("4".getBytes(), neighborDb.get("1".getBytes()));
            Assertions.assertArrayEquals("3".getBytes(), neighborDb.get("2".getBytes()));
            Assertions.assertArrayEquals("2".getBytes(), neighborDb.get("3".getBytes()));
            Assertions.assertArrayEquals("1".getBytes(), neighborDb.get("4".getBytes()));
            Assertions.assertArrayEquals("1".getBytes(), neighborDb.get("5".getBytes()));
        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        // test duplicate write, should ignore
        // rocksDBWriter.insertEdge(2, 1);

        // try (RocksDB edgeDb = RocksDB.openReadOnly(edgePath);
        //         RocksDB neighborDb = RocksDB.openReadOnly(neighborPath)) {
        //     Assertions.assertArrayEquals("2".getBytes(), edgeDb.get("1|0".getBytes()));
        //     Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("1|1".getBytes()));
        //     Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("1|2".getBytes()));
        //     Assertions.assertArrayEquals("5".getBytes(), edgeDb.get("1|3".getBytes()));
        //     Assertions.assertNull(edgeDb.get("1|5".getBytes()));
        //     Assertions.assertNull(edgeDb.get("1|-1".getBytes()));
        //     Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("2|0".getBytes()));
        //     Assertions.assertArrayEquals("3".getBytes(), edgeDb.get("2|1".getBytes()));
        //     Assertions.assertArrayEquals("4".getBytes(), edgeDb.get("2|2".getBytes()));
        //     Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("3|0".getBytes()));
        //     Assertions.assertArrayEquals("2".getBytes(), edgeDb.get("3|1".getBytes()));
        //     Assertions.assertNull(edgeDb.get("3|2".getBytes()));
        //     Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("4|0".getBytes()));
        //     Assertions.assertNull(edgeDb.get("4|1".getBytes()));
        //     Assertions.assertArrayEquals("1".getBytes(), edgeDb.get("5|0".getBytes()));
        //     Assertions.assertNull(edgeDb.get("5|1".getBytes()));

        //     Assertions.assertArrayEquals("4".getBytes(), neighborDb.get("1".getBytes()));
        //     Assertions.assertArrayEquals("3".getBytes(), neighborDb.get("2".getBytes()));
        //     Assertions.assertArrayEquals("2".getBytes(), neighborDb.get("3".getBytes()));
        //     Assertions.assertArrayEquals("1".getBytes(), neighborDb.get("4".getBytes()));
        //     Assertions.assertArrayEquals("1".getBytes(), neighborDb.get("5".getBytes()));
        // } catch (RocksDBException e) {
        //     System.err.println("Error working with RocksDB: " + e.getMessage());
        //     e.printStackTrace();
        //     Assertions.assertTrue(false);
        // }

        rocksDBWriter.finalize();
    }

    private void checkNode(RocksDB db, int id, int label, String feature) throws RocksDBException {
        byte[] value = db.get(String.valueOf(id).getBytes());
        byte[] labelByte = new byte[4];
        System.arraycopy(value, 0, labelByte, 0, 4);
        int labelRead = ByteBuffer.wrap(labelByte).order(ByteOrder.BIG_ENDIAN).getInt();
        byte[] featureByte = new byte[value.length - 4];
        System.arraycopy(value, 4, featureByte, 0, value.length - 4);
        String featureRead = new String(featureByte);
        Assertions.assertEquals(label, labelRead);
        Assertions.assertEquals(feature, featureRead);
    }

    // insert node is disabled
    // @Test
    void testInserNode2() {
        // test insert with int label and byte feature

        RocksDBWriter rocksDBWriter = new RocksDBWriter(nodePath, edgePath, neighborPath);

        // insert node

        rocksDBWriter.insertNode(1, 1, "0.01,0.02,0.03,0.04,0.05".getBytes());
        rocksDBWriter.insertNode(2, 2, "0.02,0.03,0.04,0.05,0.06".getBytes());
        rocksDBWriter.insertNode(3, 3, "0.03,0.04,0.05,0.06,0.07".getBytes());
        rocksDBWriter.insertNode(4, 4, "0.04,0.05,0.06,0.07,0.08".getBytes());
        rocksDBWriter.insertNode(5, 5, "0.05,0.06,0.07,0.08,0.09".getBytes());
        rocksDBWriter.insertNode(6, 6, "0.06,0.07,0.08,0.09,0.10".getBytes());
        rocksDBWriter.insertNode(7, 7, "0.07,0.08,0.09,0.10,0.11".getBytes());
        rocksDBWriter.insertNode(8, 8, "0.08,0.09,0.10,0.11,0.12".getBytes());
        rocksDBWriter.insertNode(9, 9, "0.09,0.10,0.11,0.12,0.13".getBytes());
        rocksDBWriter.insertNode(10, 0, "0.10,0.11,0.12,0.13,0.14".getBytes());
        rocksDBWriter.insertNode(11, 1, "0.11,0.12,0.13,0.14,0.15".getBytes());
        rocksDBWriter.insertNode(12, 2, "0.12,0.13,0.14,0.15,0.16".getBytes());

        // validate node

        try (RocksDB db = RocksDB.openReadOnly(nodePath)) {

            checkNode(db, 2, 2, "0.02,0.03,0.04,0.05,0.06");
            checkNode(db, 6, 6, "0.06,0.07,0.08,0.09,0.10");
            checkNode(db, 10, 0, "0.10,0.11,0.12,0.13,0.14");
            checkNode(db, 12, 2, "0.12,0.13,0.14,0.15,0.16");

        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        rocksDBWriter.finalize();
    }
}
