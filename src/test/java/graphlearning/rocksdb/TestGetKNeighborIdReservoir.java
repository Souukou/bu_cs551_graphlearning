package graphlearning.rocksdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/** Test RocksDB. */
public class TestGetKNeighborIdReservoir {
    @Nested
    class DirectedGraph extends TestRocksDBSetup1 {

        @Test
        void testGetKNeighborId1() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 1, 10);

            ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 13)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 15)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 17)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 19)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 21)));

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
        }

        @Test
        void testGetKNeighborId2() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 2, 10);

            ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

            for (int j = 13; j < 23; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
            }

            for (int i = 13; i <= 21; i += 2) {
                for (int j = i + 10; j < i + 20; j += 2) {
                    expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
                }
            }

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
        }

        @Test
        void testGetKNeighborId3() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 3, 10);

            ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

            for (int j = 13; j <= 21; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
            }

            for (int i = 13; i <= 21; i += 2) {
                for (int j = i + 10; j < i + 20; j += 2) {
                    expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
                }
            }

            for (int i = 23; i <= 39; i += 2) {
                for (int j = i + 10; j < i + 20; j += 2) {
                    expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
                }
            }

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
        }

        @Test
        void testGetKNeighborId4() {
            // test with limit neighbor size
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 3, 3);

            ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

            for (int j = 13; j <= 21; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
            }

            for (int i = 13; i <= 21; i += 2) {
                for (int j = i + 10; j < i + 20; j += 2) {
                    expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
                }
            }

            for (int i = 23; i <= 39; i += 2) {
                for (int j = i + 10; j < i + 20; j += 2) {
                    expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(i, j)));
                }
            }

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
        }

        @Test
        void testRandomSampleFromWholeDB() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<Integer> sample = rocksDBReader.getRandomSample(10);

            HashSet<Integer> sampleSet = new HashSet<Integer>(sample);

            HashSet<Integer> allPossibleNode = new HashSet<Integer>();
            for (int i = 1; i <= 90; i++) {
                allPossibleNode.add(i);
            }
            // System.out.println(sampleSet);
            Assertions.assertTrue(allPossibleNode.containsAll(sampleSet));
            Assertions.assertEquals(sampleSet.size(), 10);
        }
    }

    @Nested
    class UndirectedGraph extends TestRocksDBSetup2 {
        @Test
        void testGetKNeighborId1() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 1, 10);

            ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 13)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 15)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 17)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 19)));
            expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, 21)));

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
        }

        @Test
        void testGetKNeighborId2() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 2, 10);

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
            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
        }

        @Test
        void testGetKNeighborId3() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 3, 10);

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

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            Assertions.assertEquals(expectedNeighborHashSet, neighborHashSet);
        }

        @Test
        void testGetKNeighborId4() {
            // test with limit neighbor size
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 1, 3);

            ArrayList<ArrayList<Integer>> expectedNeighbors = new ArrayList<ArrayList<Integer>>();

            for (int j = 13; j <= 21; j += 2) {
                expectedNeighbors.add(new ArrayList<Integer>(Arrays.asList(3, j)));
            }

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
        }

        @Test
        void testGetKNeighborId5() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 2, 3);

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
            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
        }

        @Test
        void testGetKNeighborId6() {
            RocksDBReader rocksDBReader = new RocksDBReader(nodePath, edgePath);
            ArrayList<ArrayList<Integer>> neighbors =
                    rocksDBReader.getKNeighborIdReservoir(3, 3, 3);

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

            HashSet<ArrayList<Integer>> neighborHashSet =
                    new HashSet<ArrayList<Integer>>(neighbors);
            HashSet<ArrayList<Integer>> expectedNeighborHashSet =
                    new HashSet<ArrayList<Integer>>(expectedNeighbors);

            // System.out.println(expectedNeighborHashSet.size() + " " + neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.size() > neighborHashSet.size());
            Assertions.assertTrue(expectedNeighborHashSet.containsAll(neighborHashSet));
        }
    }
}
