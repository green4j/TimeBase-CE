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
package com.epam.deltix.qsrv.dtb.store.dataacc;

import com.epam.deltix.util.JUnitCategories.TickDBFast;
import com.epam.deltix.util.concurrent.QuickExecutor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Category(TickDBFast.class)
public class Test_DeferredNotifications {

    private static QuickExecutor quickExecutor;

    @BeforeClass
    public static void start() {
        quickExecutor = QuickExecutor.createNewInstance("Test_DeferredNotifications", null);
    }

    @AfterClass
    public static void stop() {
        quickExecutor.shutdownInstance();
    }

    private static final class CountingTask extends QuickExecutor.QuickTask {
        final Semaphore runs = new Semaphore(0);
        final Runnable wake = this::submit;

        CountingTask() {
            super(quickExecutor);
        }

        @Override
        public void run() {
            runs.release();
        }

        boolean ranWithin(long millis) throws InterruptedException {
            return runs.tryAcquire(millis, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void submitsImmediatelyOutsideBatch() throws InterruptedException {
        final CountingTask task = new CountingTask();

        DeferredNotifications.submit(task.wake);

        assertTrue("A notifier outside a batch was not submitted", task.ranWithin(5000));
    }

    @Test
    public void defersAndDeduplicatesInsideBatch() throws InterruptedException {
        final DeferredNotifications notifications = DeferredNotifications.current();
        final CountingTask first = new CountingTask();
        final CountingTask second = new CountingTask();

        notifications.begin();
        try {
            DeferredNotifications.submit(first.wake);
            DeferredNotifications.submit(second.wake);
            DeferredNotifications.submit(first.wake);

            assertFalse("A notifier ran before the batch was flushed", first.ranWithin(100));

            notifications.flush();

            assertTrue(first.ranWithin(5000));
            assertTrue(second.ranWithin(5000));
            assertFalse("A notifier submitted twice in a batch ran twice", first.ranWithin(100));

            DeferredNotifications.submit(first.wake);
        } finally {
            notifications.end();
        }

        assertTrue("end() did not flush the batch", first.ranWithin(5000));
    }

    @Test
    public void nestedBatchFlushesOnOutermostEnd() throws InterruptedException {
        final DeferredNotifications notifications = DeferredNotifications.current();
        final CountingTask task = new CountingTask();

        notifications.begin();
        notifications.begin();
        DeferredNotifications.submit(task.wake);
        notifications.end();

        assertFalse("An inner end() flushed the outer batch", task.ranWithin(100));

        notifications.end();

        assertTrue(task.ranWithin(5000));
    }

    @Test
    public void growsBeyondInitialCapacity() throws InterruptedException {
        final DeferredNotifications notifications = DeferredNotifications.current();
        final CountingTask[] tasks = new CountingTask[100];

        notifications.begin();
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new CountingTask();
            DeferredNotifications.submit(tasks[i].wake);
        }
        notifications.end();

        for (CountingTask task : tasks)
            assertTrue("A deferred notifier was lost", task.ranWithin(5000));
    }

    @Test(expected = IllegalStateException.class)
    public void endWithoutBeginFails() {
        DeferredNotifications.current().end();
    }
}
