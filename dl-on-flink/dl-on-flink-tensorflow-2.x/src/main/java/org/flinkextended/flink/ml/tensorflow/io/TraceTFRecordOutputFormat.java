/*
 * Copyright 2022 Deep Learning on Flink Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flinkextended.flink.ml.tensorflow.io;

import org.flinkextended.flink.ml.util.ProtoUtil;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.proto.example.Example;

import java.io.IOException;

/** TraceTFRecordOutputFormat. */
public class TraceTFRecordOutputFormat extends RichOutputFormat<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(TraceTFRecordOutputFormat.class);

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {}

    @Override
    public void writeRecord(byte[] record) throws IOException {
        Example example = Example.parseFrom(record);
        LOG.info(ProtoUtil.protoToJson(example).substring(0, 30));
    }

    @Override
    public void close() throws IOException {}
}
