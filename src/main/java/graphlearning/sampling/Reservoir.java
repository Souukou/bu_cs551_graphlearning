package graphlearning.sampling;

import graphlearning.helper.RandomNumbers;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Reservoir implements Serializable {
    /**
     * Implementation of Reservoir Sampling Algorithm R. See
     * https://en.wikipedia.org/wiki/Reservoir_sampling#Simple:_Algorithm_R
     */
    private Integer timestamp = 0;

    @Getter private Integer size = 10;
    private Random rand = new Random();
    private int[] reservoir = new int[size];

    public void update(Integer newNode) {
        if (timestamp < size) {
            reservoir[timestamp] = newNode;
            timestamp++;
            return;
        }
        Integer j = rand.nextInt(timestamp + 1);
        if (j < size) {
            reservoir[j] = newNode;
            timestamp++;
        }
    }

    public List<Integer> getReservoir() {
        return Arrays.stream(reservoir).boxed().collect(Collectors.toList());
    }

    public List<Integer> sample(int numOfSamples) {
        if (numOfSamples > timestamp) {
            List<Integer> randomNums = new ArrayList<>();
            for (int i = 0; i < timestamp && i < size; i++) {
                randomNums.add(reservoir[i]);
            }
            return randomNums;
        }

        if (numOfSamples > size) {
            List<Integer> randomNums = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                randomNums.add(reservoir[i]);
            }
            return randomNums;
        }

        List<Integer> randomNums = RandomNumbers.randomNumbers(0, size - 1, numOfSamples);
        List<Integer> samples =
                randomNums.stream().map(x -> reservoir[x]).collect(Collectors.toList());

        return samples;
    }
}
