package graphlearning.rocksdb;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** This test base is used by other test to set up a simple RocksDB for test. */
public abstract class TestRocksDBSetup1 {
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

    @BeforeAll
    static void setupRocksDB() {
        System.out.println("Setup RocksDB for test");

        createDirIfNotExists(nodePath);
        createDirIfNotExists(edgePath);
        createDirIfNotExists(neighborPath);

        ComparatorOptions comparatorOptions = new ComparatorOptions();
        Options options2 = new Options().setCreateIfMissing(true);
        options2.setComparator(new OrderByCountComparator(comparatorOptions));
        Options options = new Options().setCreateIfMissing(true);
        try (RocksDB nodeDb = RocksDB.open(options, nodePath);
                RocksDB neighborDb = RocksDB.open(options, neighborPath);
                RocksDB edgeDb = RocksDB.open(options2, edgePath)) {
            // construct the nodes.db in the following format:
            // key: nodeID
            // value: label|embedding
            for (int i = 1; i <= 90; i++) {
                String key = String.valueOf(i);
                String value =
                        String.valueOf(i % 10)
                                + "|"
                                + String.format(
                                        "%.2f,%.2f,%.2f,%.2f",
                                        (float) i / 100,
                                        (float) (i + 1) / 100,
                                        (float) (i + 2) / 100,
                                        (float) (i + 3) / 100);

                nodeDb.put(key.getBytes(), value.getBytes());
            }
            // construct the edge.db in the following format:
            // key: srcID|index
            // value: dstID
            // index start from zero
            for (int i = 1; i < 70; i += 2) {
                for (int j = 0; j < 5; ++j) {
                    String key = String.format("%d|%d", i, j);
                    String value = String.valueOf(i + 10 + j * 2);
                    edgeDb.put(key.getBytes(), value.getBytes());
                }
            }
            // consturct neighbor.db in the following format:
            // key: nodeID
            // value: neighborCount
            for (int i = 1; i <= 90; ++i) {
                if (i < 70 && (i + 2) % 2 == 1) {
                    neighborDb.put(String.valueOf(i).getBytes(), String.valueOf(5).getBytes());
                } else {
                    neighborDb.put(String.valueOf(i).getBytes(), String.valueOf(0).getBytes());
                }
            }

        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }
    }

    @AfterAll
    static void tearDownRocksDB() {
        System.out.println("Tear down RocksDB for test");

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

    @Test
    void testRocksDBData() {
        Options options = new Options();
        options.setCreateIfMissing(true);

        try (RocksDB db = RocksDB.openReadOnly(options, neighborPath)) {
            Assertions.assertArrayEquals("5".getBytes(), db.get("51".getBytes()));
            Assertions.assertArrayEquals("0".getBytes(), db.get("52".getBytes()));
            Assertions.assertArrayEquals("5".getBytes(), db.get("1".getBytes()));
            Assertions.assertArrayEquals("0".getBytes(), db.get("2".getBytes()));

        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        // try (RocksDB db = RocksDB.openReadOnly(options, nodePath)) {
        //     // Junit need to use assertArrayEquals to compare byte[]
        //     // assertEquals will compare the reference
        //     Assertions.assertArrayEquals(
        //             "4|0.24,0.25,0.26,0.27".getBytes(), db.get("24".getBytes()));
        //     Assertions.assertArrayEquals(
        //             "1|0.11,0.12,0.13,0.14".getBytes(), db.get("11".getBytes()));
        //     Assertions.assertArrayEquals(
        //             "0|0.90,0.91,0.92,0.93".getBytes(), db.get("90".getBytes()));
        // } catch (RocksDBException e) {
        //     System.err.println("Error working with RocksDB: " + e.getMessage());
        //     e.printStackTrace();
        //     Assertions.assertTrue(false);
        // }

        ComparatorOptions comparatorOptions = new ComparatorOptions();
        Options options2 = new Options();
        options2.setComparator(new OrderByCountComparator(comparatorOptions));

        try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
            Assertions.assertArrayEquals("11".getBytes(), db.get("1|0".getBytes()));
            Assertions.assertArrayEquals("13".getBytes(), db.get("1|1".getBytes()));
            Assertions.assertArrayEquals("15".getBytes(), db.get("1|2".getBytes()));
            Assertions.assertArrayEquals("17".getBytes(), db.get("1|3".getBytes()));
            Assertions.assertArrayEquals("19".getBytes(), db.get("1|4".getBytes()));
            Assertions.assertNull(db.get("1|-1".getBytes()));
            Assertions.assertNull(db.get("1|5".getBytes()));
            Assertions.assertArrayEquals("61".getBytes(), db.get("51|0".getBytes()));
            Assertions.assertArrayEquals("63".getBytes(), db.get("51|1".getBytes()));
            Assertions.assertArrayEquals("65".getBytes(), db.get("51|2".getBytes()));
            Assertions.assertArrayEquals("67".getBytes(), db.get("51|3".getBytes()));
            Assertions.assertArrayEquals("69".getBytes(), db.get("51|4".getBytes()));
            Assertions.assertNull(db.get("51|-1".getBytes()));
            Assertions.assertNull(db.get("51|5".getBytes()));
            Assertions.assertNull(db.get("52|0".getBytes()));
            Assertions.assertArrayEquals("63".getBytes(), db.get("53|0".getBytes()));

        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        options.close();
    }
}
