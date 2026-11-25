/*
 * Copyright 2024 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.qsrv.dtb.store.impl;

import com.epam.deltix.qsrv.dtb.fs.pub.FSFactory;
import com.epam.deltix.qsrv.dtb.store.pub.DataReader;
import com.epam.deltix.qsrv.dtb.store.pub.DataWriter;
import com.epam.deltix.qsrv.dtb.store.pub.EntityFilter;
import com.epam.deltix.qsrv.dtb.store.pub.PersistentDataStore;
import com.epam.deltix.qsrv.dtb.store.pub.TSMessageConsumer;
import com.epam.deltix.qsrv.dtb.store.pub.TSRoot;
import com.epam.deltix.util.JUnitCategories.TickDBFast;
import com.epam.deltix.util.memory.MemoryDataInput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * A time slice deactivated under a reader that is in the middle of a message: the blocks of the slice are cleared
 * only once the read has left the block.
 */
@Category(TickDBFast.class)
public class Test_DeactivateWhileReading {
    private static final long   BASE_TIME = 1356998400000000000L;
    private static final int    MESSAGES = 10;

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test(timeout = 60_000)
    public void blocksAreNotClearedUnderARead() throws Exception {
        final PersistentDataStore cache = PDSFactory.create();
        cache.start();
        try {
            final TSRoot root = cache.createRoot(null, FSFactory.getLocalFS(), folder.newFolder("root").getAbsolutePath());
            root.format();
            final int entity = root.getSymbolRegistry().registerSymbol("DLTX", "data for DLTX");

            try (DataWriter writer = cache.createWriter()) {
                writer.associate(root);
                writer.open(BASE_TIME, null);
                for (int i = 0; i < MESSAGES; i++)
                    writer.insertMessage(entity, BASE_TIME + i, 0, out -> out.writeLong(42));
            }

            final CountDownLatch reading = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicReference<Throwable> readError = new AtomicReference<>();

            final Thread reader = new Thread(() -> {
                try (DataReader r = cache.createReader(false)) {
                    r.associate(root);
                    r.open(Long.MIN_VALUE, true, EntityFilter.ALL);
                    r.readNext(new HoldingConsumer(reading, release));
                } catch (Throwable x) {
                    readError.set(x);
                }
            }, "reader");
            reader.start();
            assertTrue("The reader did not start to read", reading.await(30, TimeUnit.SECONDS));

            final Thread formatter = new Thread(root::format, "formatter");
            formatter.start();
            formatter.join(1_000);

            assertTrue("The blocks were cleared while a read was in the middle of a message", formatter.isAlive());

            release.countDown();
            reader.join(30_000);
            formatter.join(30_000);

            assertFalse("The reader did not finish", reader.isAlive());
            assertFalse("The format did not finish", formatter.isAlive());
            assertNull("The read failed: " + readError.get(), readError.get());
        } finally {
            cache.shutdown();
            cache.waitForShutdown(10_000);
        }
    }

    /** Holds the first message until released, under the lock of the block being read. */
    private static final class HoldingConsumer implements TSMessageConsumer {
        private final CountDownLatch reading;
        private final CountDownLatch release;

        HoldingConsumer(CountDownLatch reading, CountDownLatch release) {
            this.reading = reading;
            this.release = release;
        }

        @Override
        public void process(int entity, long timestampNanos, int type, int bodyLength, MemoryDataInput mdi) {
            reading.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean processRealTime(long timestampNanos) {
            return false;
        }

        @Override
        public boolean isRealTime() {
            return false;
        }

        @Override
        public boolean realTimeAvailable() {
            return false;
        }
    }
}
