package graphlearning;

import graphlearning.rocksdb.RocksDBReader;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/** Test RocksDB. */
public class TestRocksDBOnUndirectedGraph {
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

    // Only build edge.db to test RocksDBReader
    @BeforeAll
    static void setupRocksDB() {
        System.out.println("Setup RocksDB for test");

        createDirIfNotExists(edgePath);

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB nodeDb = RocksDB.open(options, nodePath);
                RocksDB edgeDb = RocksDB.open(options, edgePath);
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

    @Test
    void testGetKNeighborId1() {
        RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
        ArrayList<ArrayList<Integer>> neighbors = rocksDBReader.getKNeighborId(3, 1, 10);

        ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

        expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 13)));
        expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 15)));
        expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 17)));
        expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 19)));
        expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 21)));

        HashSet<ArrayList<Integer>> neighborHashSet = new HashSet<ArrayList<Integer>>(neighbors);
        HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                new HashSet<ArrayList<Integer>>(expectedNeighbors);

        Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
    }

    @Test
    void testGetKNeighborId2() {
        RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
        ArrayList<ArrayList<Integer>> neighbors = rocksDBReader.getKNeighborId(3, 2, 10);

        ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

        for (int j = 13; j <= 21; j += 2) {
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
        }

        for (int i = 13; i <= 21; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }
        HashSet<ArrayList<Integer>> neighborHashSet = new HashSet<ArrayList<Integer>>(neighbors);
        HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                new HashSet<ArrayList<Integer>>(expectedNeighbors);

        Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
    }

    @Test
    void testGetKNeighborId3() {
        RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
        ArrayList<ArrayList<Integer>> neighbors = rocksDBReader.getKNeighborId(3, 3, 10);

        ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

        for (int j = 13; j <= 21; j += 2) {
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
        }

        for (int i = 13; i <= 21; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }

        for (int i = 1; i <= 11; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }
        for (int i = 23; i <= 39; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }

        HashSet<ArrayList<Integer>> neighborHashSet = new HashSet<ArrayList<Integer>>(neighbors);
        HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                new HashSet<ArrayList<Integer>>(expectedNeighbors);

        Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
    }

    @Test
    void testGetKNeighborId4() {
        // test with limit neighbor size
        RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
        ArrayList<ArrayList<Integer>> neighbors = rocksDBReader.getKNeighborId(3, 1, 3);

        ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

        for (int j = 13; j <= 21; j += 2) {
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
        }

        HashSet<ArrayList<Integer>> neighborHashSet = new HashSet<ArrayList<Integer>>(neighbors);
        HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                new HashSet<ArrayList<Integer>>(expectedNeighbors);

        // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
        Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
        Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
    }

    @Test
    void testGetKNeighborId5() {
        RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
        ArrayList<ArrayList<Integer>> neighbors = rocksDBReader.getKNeighborId(3, 2, 3);

        ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

        for (int j = 13; j <= 21; j += 2) {
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
        }

        for (int i = 13; i <= 21; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }
        HashSet<ArrayList<Integer>> neighborHashSet = new HashSet<ArrayList<Integer>>(neighbors);
        HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                new HashSet<ArrayList<Integer>>(expectedNeighbors);

        // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
        Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
        Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
    }

    @Test
    void testGetKNeighborId6() {
        RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
        ArrayList<ArrayList<Integer>> neighbors = rocksDBReader.getKNeighborId(3, 3, 3);

        ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

        for (int j = 13; j <= 21; j += 2) {
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
        }

        for (int i = 13; i <= 21; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }

        for (int i = 1; i <= 11; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }
        for (int i = 23; i <= 39; i += 2) {
            for (int j = i + 10; j < i + 20; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
            for (int j = i - 10; j > i - 20 && j >= 1; j -= 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
            }
        }

        HashSet<ArrayList<Integer>> neighborHashSet = new HashSet<ArrayList<Integer>>(neighbors);
        HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                new HashSet<ArrayList<Integer>>(expectedNeighbors);

        // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
        Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
        Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
    }
}
