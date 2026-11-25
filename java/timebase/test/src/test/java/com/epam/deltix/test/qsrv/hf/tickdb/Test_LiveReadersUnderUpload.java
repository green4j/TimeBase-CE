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
package com.epam.deltix.test.qsrv.hf.tickdb;

import com.epam.deltix.qsrv.hf.tickdb.StreamConfigurationHelper;
import com.epam.deltix.qsrv.hf.tickdb.TDBRunner;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.qsrv.hf.tickdb.pub.LoadingError;
import com.epam.deltix.qsrv.hf.tickdb.pub.LoadingErrorListener;
import com.epam.deltix.qsrv.hf.tickdb.pub.LoadingOptions;
import com.epam.deltix.qsrv.hf.tickdb.pub.SelectionOptions;
import com.epam.deltix.qsrv.hf.tickdb.pub.StreamOptions;
import com.epam.deltix.qsrv.hf.tickdb.pub.StreamScope;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickCursor;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickLoader;
import com.epam.deltix.qsrv.test.messages.BarMessage;
import com.epam.deltix.test.qsrv.hf.tickdb.server.ServerRunner;
import com.epam.deltix.util.JUnitCategories;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.*;

/**
 * Live readers of a stream a remote loader writes to: every reader has to get every message, in order
 * per symbol. The loader alternates bursts and pauses, so that readers keep going to sleep and waking up.
 * Remote cursors are woken up through their availability listener, local ones through {@code notifyAll}.
 */
@Category(JUnitCategories.TickDBFast.class)
public class Test_LiveReadersUnderUpload {

    private static final int    SYMBOLS = 1500;
    private static final int    MESSAGES = 150_000;
    private static final int    BURST = 20;
    private static final long   BASE_TIME = 1600000000000L;
    private static final long   TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);

    private static TDBRunner runner;

    @BeforeClass
    public static void start() throws Throwable {
        runner = new ServerRunner(true, true);
        runner.startup();
    }

    @AfterClass
    public static void stop() throws Throwable {
        runner.shutdown();
        runner = null;
    }

    @Test(timeout = 180_000)
    public void everyReaderGetsEveryMessageInOrder() throws Exception {
        final String name = "liveReadersUnderUpload";

        final StreamOptions options = new StreamOptions(StreamScope.DURABLE, name, null, 0);
        options.version = "5.0";
        options.setFixedType(StreamConfigurationHelper.mkUniversalBarMessageDescriptor());

        final DXTickStream remote = runner.getTickDb().createStream(name, options);
        final DXTickStream local = runner.getServerDb().getStream(name);

        final List<Reader> readers = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            readers.add(new Reader("Remote #" + i, remote, false));
        for (int i = 0; i < 2; i++)
            readers.add(new Reader("Local #" + i, local, false));
        readers.add(new Reader("Remote slow", remote, true));
        final Reader late = new Reader("Remote late", remote, false);
        readers.add(late);

        try {
            for (Reader r : readers) {
                if (r != late)
                    r.start();
            }
            for (Reader r : readers) {
                if (r != late)
                    r.awaitOpened();
            }

            write(remote, late);

            final long deadline = System.nanoTime() + TIMEOUT_NANOS;
            for (Reader r : readers) {
                while (r.received.get() < MESSAGES && r.failure.get() == null && System.nanoTime() < deadline)
                    Thread.sleep(50);
            }

            for (Reader r : readers) {
                if (r.failure.get() != null)
                    throw new AssertionError(r.getName() + " failed", r.failure.get());
                assertEquals(r.getName() + " did not get every message", MESSAGES, r.received.get());
            }
        } finally {
            for (Reader r : readers)
                r.stopReading();
        }

        for (Reader r : readers)
            assertFalse(r.getName() + " did not stop", r.isAlive());
    }

    /**
     * Sends a per-symbol sequence number in every message, in bursts with short pauses in between.
     */
    private static void write(DXTickStream stream, Reader late) {
        final AtomicInteger errors = new AtomicInteger();
        try (TickLoader loader = stream.createLoader(new LoadingOptions())) {
            loader.addEventListener(new LoadingErrorListener() {
                @Override
                public void onError(LoadingError e) {
                    errors.incrementAndGet();
                }
            });

            final BarMessage bar = new BarMessage();
            final int[] sequences = new int[SYMBOLS];
            for (int i = 0; i < MESSAGES; i++) {
                final int symbol = (int) ((i * 2654435761L) % SYMBOLS);
                bar.setSymbol("S" + symbol);
                bar.setTimeStampMs(BASE_TIME + i);
                bar.setClose(sequences[symbol]++);
                loader.send(bar);

                if (i == MESSAGES * 3 / 10)
                    late.start();
                if ((i + 1) % BURST == 0)
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(20));
            }
        }
        assertEquals("The loader reported errors", 0, errors.get());
    }

    private static final class Reader extends Thread {
        private final DXTickStream                  stream;
        private final boolean                       slow;
        private final AtomicReference<TickCursor>   cursor = new AtomicReference<>();
        final AtomicLong                            received = new AtomicLong();
        final AtomicReference<Throwable>            failure = new AtomicReference<>();
        private volatile boolean                    stopped;

        Reader(String name, DXTickStream stream, boolean slow) {
            super(name);
            this.stream = stream;
            this.slow = slow;
            setDaemon(true);
        }

        void awaitOpened() throws InterruptedException {
            final long deadline = System.nanoTime() + TIMEOUT_NANOS;
            while (cursor.get() == null && failure.get() == null) {
                if (System.nanoTime() > deadline)
                    fail(getName() + " did not open its cursor within 60 seconds");
                Thread.sleep(10);
            }
        }

        void stopReading() throws InterruptedException {
            stopped = true;
            final TickCursor c = cursor.get();
            if (c != null)
                c.close();
            if (getState() != State.NEW)
                join(TimeUnit.SECONDS.toMillis(10));
        }

        @Override
        public void run() {
            final Map<String, Integer> last = new HashMap<>();
            try (TickCursor c = stream.select(Long.MIN_VALUE, new SelectionOptions(false, true))) {
                cursor.set(c);
                while (!stopped && c.next()) {
                    final BarMessage bar = (BarMessage) c.getMessage();
                    final String symbol = bar.getSymbol().toString();
                    final int sequence = (int) bar.getClose();
                    final Integer previous = last.put(symbol, sequence);
                    final int expected = previous == null ? 0 : previous + 1;
                    if (sequence != expected)
                        throw new AssertionError(symbol + ": expected message #" + expected + ", got #" + sequence);

                    if (received.incrementAndGet() == MESSAGES)
                        return;
                    if (slow && received.get() % 2_000 == 0)
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                }
            } catch (Throwable x) {
                if (!stopped)
                    failure.compareAndSet(null, x);
            }
        }
    }
}
