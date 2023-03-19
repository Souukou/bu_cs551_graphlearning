/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import java.util.List;

/**
 * A simple Flink program that processes the Wikipedia edits stream.
 **/
public class WikiEdits {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1); // use 1 processing tasks

        DataStream<WikipediaEditEvent> edits = env
                .addSource(new WikipediaEditsSource());

        // Filter events with byte diff < 0
        DataStream<WikipediaEditEvent> filtered = edits.filter(new MyFilterFunction());

        DataStream<Tuple2<String, Integer>> mappedEdits = filtered.map(new MyMapFunction());

        DataStream<List<Tuple2<String, Integer>>> windowed = mappedEdits
                .countWindowAll(10)
                .aggregate(new MyAggregateFunction());



        // print the results to the console
        windowed.print();

        // Execute the program
        env.execute("Print Wikipedia Edits Stream");
    }

}


