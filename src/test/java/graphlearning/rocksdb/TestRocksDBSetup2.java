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

/**
 * This test base is used by other test to set up a simple RocksDB for test. Only setup an
 * undirected edge.db
 */
public abstract class TestRocksDBSetup2 {
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

        createDirIfNotExists(edgePath);

        ComparatorOptions comparatorOptions = new ComparatorOptions();
        Options options2 = new Options().setCreateIfMissing(true);
        options2.setComparator(new OrderByCountComparator(comparatorOptions));

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB nodeDb = RocksDB.open(options, nodePath);
                RocksDB edgeDb = RocksDB.open(options2, edgePath);
                RocksDB neighborDb = RocksDB.open(options, neighborPath)) {

            // construct the edge.db in the following format:
            // key: srcID|index
            // value: dstID
            // index start from zero
            for (int i = 1; i < 70; i += 2) {
                for (int j = 0; j < 5; ++j) {
                    String key = String.format("%d|%d", i, j);
                    String value = String.valueOf(i + 10 + j * 2);
                    edgeDb.put(key.getBytes(), value.getBytes());

                    String key2 = String.format("%d|%d", i + 10 + j * 2, j + 5);
                    String value2 = String.valueOf(i);
                    edgeDb.put(key2.getBytes(), value2.getBytes());
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
        ComparatorOptions comparatorOptions = new ComparatorOptions();
        options.setComparator(new OrderByCountComparator(comparatorOptions));

        try (RocksDB db = RocksDB.open(options, edgePath)) {
            Assertions.assertEquals("11", new String(db.get("1|0".getBytes())));
            Assertions.assertEquals("13", new String(db.get("1|1".getBytes())));
            Assertions.assertEquals("15", new String(db.get("1|2".getBytes())));
            Assertions.assertEquals("17", new String(db.get("1|3".getBytes())));
            Assertions.assertEquals("19", new String(db.get("1|4".getBytes())));
            Assertions.assertNull(db.get("1|-1".getBytes()));
            Assertions.assertNull(db.get("1|5".getBytes()));
            Assertions.assertEquals("61", new String(db.get("51|0".getBytes())));
            Assertions.assertEquals("63", new String(db.get("51|1".getBytes())));
            Assertions.assertEquals("65", new String(db.get("51|2".getBytes())));
            Assertions.assertEquals("67", new String(db.get("51|3".getBytes())));
            Assertions.assertEquals("69", new String(db.get("51|4".getBytes())));
            Assertions.assertEquals("41", new String(db.get("51|5".getBytes())));
            Assertions.assertEquals("39", new String(db.get("51|6".getBytes())));
            Assertions.assertEquals("37", new String(db.get("51|7".getBytes())));
            Assertions.assertEquals("35", new String(db.get("51|8".getBytes())));
            Assertions.assertEquals("33", new String(db.get("51|9".getBytes())));

            Assertions.assertNull(db.get("51|-1".getBytes()));
            Assertions.assertNull(db.get("51|10".getBytes()));
            Assertions.assertNull(db.get("52|0".getBytes()));
            Assertions.assertEquals("63", new String(db.get("53|0".getBytes())));

        } catch (RocksDBException e) {
            System.err.println("Error working with RocksDB: " + e.getMessage());
            e.printStackTrace();
            Assertions.assertTrue(false);
        }

        options.close();
    }
}
