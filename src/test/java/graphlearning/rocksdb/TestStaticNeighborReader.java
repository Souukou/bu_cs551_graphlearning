package graphlearning.rocksdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/** Test RocksDB. */
public class TestStaticNeighborReader {
    @Nested
    class DirectedGraph extends TestRocksDBSetup1 {

        @Test
        void testFindNeighbor1() {
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            Options options2 = new Options();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
                ArrayList<Integer> neighbors = NeighborReader.findNeighbor(21, db, -1);
                Assertions.assertEquals(
                        new HashSet<>(Arrays.asList(31, 33, 35, 37, 39)), new HashSet<>(neighbors));
                neighbors = NeighborReader.findNeighbor(21, db, 3);
                Assertions.assertTrue(
                        new HashSet<>(Arrays.asList(31, 33, 35, 37, 39)).containsAll(neighbors));
                Assertions.assertEquals(3, neighbors.size());

                neighbors = NeighborReader.findNeighbor(2, db, -1);
                Assertions.assertEquals(0, neighbors.size());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @Test
        void testFindNeighborReservoir1() {
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            Options options2 = new Options();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
                ArrayList<Integer> neighbors = NeighborReader.findNeighbor(21, db, -1);
                Assertions.assertEquals(
                        new HashSet<>(Arrays.asList(31, 33, 35, 37, 39)), new HashSet<>(neighbors));
                neighbors = NeighborReader.findNeighbor(21, db, 3);
                Assertions.assertTrue(
                        new HashSet<>(Arrays.asList(31, 33, 35, 37, 39)).containsAll(neighbors));
                Assertions.assertEquals(3, neighbors.size());

                neighbors = NeighborReader.findNeighbor(2, db, -1);
                Assertions.assertEquals(0, neighbors.size());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @Test
        void testFindNeighborAndNeighborReservoir1() {
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            Options options2 = new Options();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
                for (int i = 1; i < 50; i++) {
                    ArrayList<Integer> neighbors = NeighborReader.findNeighbor(i, db, 1000);
                    ArrayList<Integer> neighbors2 =
                            NeighborReader.findNeighborReservoir(i, db, 1000);
                    Assertions.assertEquals(
                            new HashSet<Integer>(neighbors), new HashSet<Integer>(neighbors2));
                }

            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

    @Nested
    class UndirectedGraph extends TestRocksDBSetup2 {
        @Test
        void testFindNeighbor1() {
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            Options options2 = new Options();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
                ArrayList<Integer> neighbors = NeighborReader.findNeighbor(21, db, -1);
                Assertions.assertEquals(
                        new HashSet<>(Arrays.asList(11, 9, 7, 5, 3, 31, 33, 35, 37, 39)),
                        new HashSet<>(neighbors));
                neighbors = NeighborReader.findNeighbor(21, db, 3);
                Assertions.assertTrue(
                        new HashSet<>(Arrays.asList(11, 9, 7, 5, 3, 31, 33, 35, 37, 39))
                                .containsAll(neighbors));
                Assertions.assertEquals(3, neighbors.size());

                neighbors = NeighborReader.findNeighbor(2, db, -1);
                Assertions.assertEquals(0, neighbors.size());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @Test
        void testFindNeighborReservoir1() {
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            Options options2 = new Options();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
                ArrayList<Integer> neighbors = NeighborReader.findNeighbor(21, db, -1);
                Assertions.assertEquals(
                        new HashSet<>(Arrays.asList(11, 9, 7, 5, 3, 31, 33, 35, 37, 39)),
                        new HashSet<>(neighbors));
                neighbors = NeighborReader.findNeighbor(21, db, 3);
                Assertions.assertTrue(
                        new HashSet<>(Arrays.asList(11, 9, 7, 5, 3, 31, 33, 35, 37, 39))
                                .containsAll(neighbors));
                Assertions.assertEquals(3, neighbors.size());

                neighbors = NeighborReader.findNeighbor(2, db, -1);
                Assertions.assertEquals(0, neighbors.size());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @Test
        void testFindNeighborAndNeighborReservoir1() {
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            Options options2 = new Options();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            try (RocksDB db = RocksDB.openReadOnly(options2, edgePath)) {
                for (int i = 1; i < 20; i++) {
                    ArrayList<Integer> neighbors = NeighborReader.findNeighbor(i, db, 1000);
                    ArrayList<Integer> neighbors2 =
                            NeighborReader.findNeighborReservoir(i, db, 1000);
                    Assertions.assertEquals(
                            new HashSet<Integer>(neighbors), new HashSet<Integer>(neighbors2));
                }

            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }
}
