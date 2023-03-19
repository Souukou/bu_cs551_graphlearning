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

package org.flinkextended.flink.ml.cluster.rpc;

import org.flinkextended.flink.ml.cluster.master.meta.AMMeta;
import org.flinkextended.flink.ml.cluster.storage.StorageFactory;
import org.flinkextended.flink.ml.util.MLException;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/** Unit test for {@link AMRegistry}. */
public class AMRegistryTest {

    @Test
    public void testInvalidAMAddress() throws Exception {
        StorageFactory.memoryStorage.setValue(AMMeta.AM_ADDRESS, "foo:1234".getBytes());
        try {
            AMRegistry.getAMClient(new HashMap<>(), Duration.ofSeconds(10).toMillis());
        } catch (MLException e) {
            if (e.getCause() instanceof TimeoutException) {
                // expected
                return;
            }
        }
        Assert.fail("Get AM client with invalid address should timeout");
    }
}
