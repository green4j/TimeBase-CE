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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.*;

@Category(TickDBFast.class)
public class Test_PendingBlocks {

    private static DataBlock block(int entity) {
        final DataBlock db = new DataBlock();
        db.init(null, entity, 0, 0, 0);
        return db;
    }

    @Test
    public void keepsLatestBlockPerEntity() {
        keepsLatestBlock(new PendingBlocks(), 3);
    }

    @Test
    public void clearedBlockIsIgnored() {
        final PendingBlocks pending = new PendingBlocks();
        final DataBlock db = block(7);
        db.clear();

        pending.offer(db);
        assertTrue("A cleared block became pending", pending.isEmpty());
        assertNull("A cleared block was delivered", pending.poll());
    }

    @Test
    public void stalledWriterDoesNotHoldReaderUp() {
        final PendingBlocks pending = new PendingBlocks();
        assertTrue(pending.prepareToSleep());

        // Writer A offers and defers its wake-up, then stalls without sending it.
        assertTrue(pending.offer(block(3)));
        // Writer B offers to the same, already pending entity: it still has to see to a wake-up.
        assertTrue("A second writer was not asked to wake the reader up", pending.offer(block(3)));
        assertTrue(pending.requestWake());
        // Writer A finally flushes: the reader has been woken up already.
        assertFalse(pending.requestWake());
    }

    @Test
    public void failedWakeUpIsSentByTheNextWriter() {
        final PendingBlocks pending = new PendingBlocks();
        assertTrue(wake(pending, block(3)));

        pending.cancelWake();

        assertTrue("A writer was not asked to replace a failed wake-up", pending.offer(block(4)));
        assertTrue(pending.requestWake());
    }

    @Test
    public void slotsGrowForFarEntity() {
        keepsLatestBlock(new PendingBlocks(), 5_000_000);
    }

    private static void keepsLatestBlock(PendingBlocks pending, int entity) {
        final DataBlock first = block(entity);
        final DataBlock second = block(entity);

        assertTrue("The first block did not wake the reader", wake(pending, first));
        assertFalse("The same block woke the reader twice", wake(pending, first));
        assertFalse("A newer block of a pending entity woke the reader again", wake(pending, second));
        assertFalse(pending.isEmpty());

        assertSame(second, pending.poll());
        assertNull(pending.poll());
        assertTrue(pending.isEmpty());
        assertTrue(pending.prepareToSleep());

        assertTrue("A taken entity did not wake the reader again", wake(pending, first));
        assertSame(first, pending.poll());
    }

    @Test
    public void wakesReaderOncePerSleep() {
        final PendingBlocks pending = new PendingBlocks();

        assertTrue(wake(pending, block(1)));
        assertFalse("A reader that has not gone to sleep since the last wake-up was woken again",
                wake(pending, block(2)));
        assertFalse("The reader was let to sleep with blocks pending", pending.prepareToSleep());

        assertNotNull(pending.poll());
        assertNotNull(pending.poll());
        assertNull(pending.poll());
        assertTrue(pending.prepareToSleep());

        assertTrue("A block offered after the reader prepared to sleep did not wake it", wake(pending, block(3)));
        assertFalse(wake(pending, block(4)));
    }

    @Test
    public void readerThatStaysAwakeIsNotWokenUp() {
        final PendingBlocks pending = new PendingBlocks();

        assertTrue(pending.prepareToSleep());
        assertTrue(pending.offer(block(1)));     // the writer defers its wake-up ...
        assertFalse(pending.prepareToSleep());   // ... and the reader sees the block before sleeping

        assertFalse("A writer woke up a reader that stayed awake", pending.requestWake());
        assertFalse("A reader that stayed awake was woken up", wake(pending, block(2)));

        assertNotNull(pending.poll());
        assertNotNull(pending.poll());
        assertTrue(pending.prepareToSleep());
        assertTrue("A block offered after the reader prepared to sleep did not wake it", wake(pending, block(3)));
    }

    @Test
    public void blockOfferedToBusyReaderKeepsItAwake() {
        final PendingBlocks pending = new PendingBlocks();

        assertTrue(wake(pending, block(1)));
        assertNotNull(pending.poll());

        // The reader is still busy with the wake-up it got, so this offer does not wake it again ...
        assertFalse(wake(pending, block(2)));
        // ... and it has to see the block on its last look before sleeping.
        assertFalse("The reader was let to sleep with a block it was not woken up for", pending.prepareToSleep());
        assertNotNull(pending.poll());
    }

    @Test
    public void ringGrowsWithoutLosingEntities() {
        final PendingBlocks pending = new PendingBlocks();
        final DataBlock[] blocks = new DataBlock[200_001];
        for (int i = 0; i < 100_000; i++) {
            blocks[i] = block(i * 7);
            wake(pending, blocks[i]);
        }
        // Half taken, so the ring wraps around before it grows again.
        final Set<DataBlock> polled = new HashSet<>();
        for (int i = 0; i < 50_000; i++)
            assertTrue("A block was polled twice", polled.add(pending.poll()));
        for (int i = 100_000; i < 200_000; i++) {
            blocks[i] = block(i * 7);
            wake(pending, blocks[i]);
        }

        final DataBlock newer = block(199_999 * 7);
        assertFalse(wake(pending, newer));
        blocks[199_999] = newer;

        assertFalse(pending.prepareToSleep());
        blocks[200_000] = block(Integer.MAX_VALUE);
        assertFalse("A reader that stayed awake was woken up", wake(pending, blocks[200_000]));

        DataBlock db;
        while ((db = pending.poll()) != null)
            assertTrue("A block was polled twice", polled.add(db));

        assertEquals(blocks.length, polled.size());
        for (DataBlock b : blocks)
            assertTrue("A pending block was lost: " + b, polled.contains(b));
        assertTrue(pending.isEmpty());
    }

    @Test
    public void clearDropsEverything() {
        final PendingBlocks pending = new PendingBlocks();
        for (int i = 0; i < 100; i++)
            wake(pending, block(i));

        pending.clear();

        assertTrue(pending.isEmpty());
        assertNull(pending.poll());

        assertTrue(pending.prepareToSleep());
        final DataBlock db = block(5);
        assertTrue("An entity did not become pending again after clear()", wake(pending, db));
        assertSame(db, pending.poll());
    }

    @Test
    public void deadBlockIsDroppedAndDoesNotStrandItsEntity() {
        final PendingBlocks pending = new PendingBlocks();
        final DataBlock db = block(5);
        assertTrue(wake(pending, db));

        // The block's file is deactivated while its ticket is queued.
        db.clear();

        assertNull("A block of a deactivated file was delivered as data", pending.poll());
        assertTrue(pending.prepareToSleep());

        final DataBlock again = block(5);
        assertTrue("The entity of a dead block stayed pending forever", wake(pending, again));
        assertSame(again, pending.poll());
    }

    @Test
    public void clearDropsEverySegmentAndMap() {
        final PendingBlocks pending = new PendingBlocks();
        final int[] entities = {5, 1500, 4000, 4001, 9000, 1_000_000};
        for (int entity : entities)
            wake(pending, block(entity));

        pending.clear();

        assertNull("A block offered before clear() was delivered after it", pending.poll());
        assertTrue(pending.prepareToSleep());

        final Set<DataBlock> offered = new HashSet<>();
        for (int entity : entities) {
            final DataBlock db = block(entity);
            wake(pending, db);
            offered.add(db);
        }
        DataBlock db;
        while ((db = pending.poll()) != null)
            assertTrue("A block was polled twice", offered.remove(db));
        assertTrue("A block offered after clear() was lost", offered.isEmpty());
    }

    @Test(timeout = 60_000)
    public void clearWaitsForOfferInProgress() throws Exception {
        final PendingBlocks pending = new PendingBlocks();
        final Thread clearing = new Thread(pending::clear, "Clearing");
        clearing.setDaemon(true);

        // Holding the writers' lock stands for a writer in the middle of an offer.
        synchronized (pending.offerLock) {
            wake(pending, block(5));
            clearing.start();

            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (clearing.getState() != Thread.State.BLOCKED && clearing.isAlive() && System.nanoTime() < deadline)
                Thread.sleep(1);
            assertEquals("clear() did not wait for an offer in progress", Thread.State.BLOCKED, clearing.getState());
        }

        clearing.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(clearing.isAlive());
        assertNull("A block offered before clear() was delivered after it", pending.poll());
    }

    /**
     * A writer offers distinct entities, in random order, one after another while the reader clears once:
     * what survives the clear has to be everything offered from some point on, as if the clear happened
     * at that point.
     */
    @Test(timeout = 180_000)
    public void clearIsAtomicAgainstConcurrentOffers() throws Exception {
        final int entities = 4096;
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        final DataBlock[] blocks = new DataBlock[entities];
        final int[] position = new int[entities];
        for (int e = 0; e < entities; e++)
            blocks[e] = block(e);

        for (int round = 0; round < 300; round++) {
            final PendingBlocks pending = new PendingBlocks();
            Collections.shuffle(Arrays.asList(blocks));
            for (int i = 0; i < entities; i++)
                position[blocks[i].getEntity()] = i;

            final AtomicInteger offered = new AtomicInteger();
            final Thread writer = new Thread(() -> {
                try {
                    for (DataBlock db : blocks) {
                        wake(pending, db);
                        offered.lazySet(offered.get() + 1);
                    }
                } catch (Throwable x) {
                    failure.compareAndSet(null, x);
                }
            }, "Writer");
            writer.setDaemon(true);
            writer.start();

            // Clear in the middle of the stream of offers.
            final int clearAfter = ThreadLocalRandom.current().nextInt(entities / 2);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (offered.get() < clearAfter && writer.isAlive() && System.nanoTime() < deadline)
                Thread.onSpinWait();
            pending.clear();

            writer.join(TimeUnit.SECONDS.toMillis(10));
            checkFailure(failure);
            assertFalse("The writer did not finish in time", writer.isAlive());

            final boolean[] survived = new boolean[entities];
            DataBlock db;
            while ((db = pending.poll()) != null)
                survived[position[db.getEntity()]] = true;

            int first = 0;
            while (first < entities && !survived[first])
                first++;
            for (int i = first; i < entities; i++)
                assertTrue("Offer #" + i + " was dropped by clear() while offer #" + first
                        + ", made before it, was kept", survived[i]);
        }
    }

    @Test(timeout = 180_000)
    public void concurrentWritersNeverLoseUpdatesOrWakeUps() throws Exception {
        runConcurrent(false);
    }

    @Test(timeout = 180_000)
    public void clearingUnderConcurrentWritersLeavesConsistentState() throws Exception {
        runConcurrent(true);
    }

    /**
     * Writers offer random blocks and wake the reader up when {@link PendingBlocks#offer} says so; the reader
     * sleeps, without a timeout, when {@link PendingBlocks#prepareToSleep} lets it. Then every entity gets
     * a final block nobody else offers. Once the writers are done and the reader is asleep, nothing may be
     * pending and the final block of every entity must be the last one the reader took; otherwise
     * a wake-up or an update was lost.
     * <p>
     * With {@code clearing}, the reader clears the blocks every now and then during the random phase:
     * an entity left in an inconsistent state by a racing clear would never deliver its final block.
     */
    private static void runConcurrent(boolean clearing) throws Exception {
        final int entities = 2560;
        final int writers = 4;
        final int rounds = 200_000;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        final PendingBlocks pending = new PendingBlocks();
        final AtomicReferenceArray<DataBlock> lastTaken = new AtomicReferenceArray<>(entities);
        final AtomicBoolean clearPhase = new AtomicBoolean(clearing);
        final AtomicBoolean clearStopped = new AtomicBoolean(!clearing);
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        final DataBlock[][] blocks = new DataBlock[entities][2];
        final DataBlock[] finals = new DataBlock[entities];
        final java.util.Map<Integer, Integer> index = new java.util.HashMap<>();
        for (int e = 0; e < entities; e++) {
            final int id = entityId(e);
            index.put(id, e);
            blocks[e][0] = block(id);
            blocks[e][1] = block(id);
            finals[e] = block(id);
        }

        final Thread reader = new Thread(() -> {
            try {
                long polls = 0;
                while (!stop.get()) {
                    final DataBlock db = pending.poll();
                    if (db != null) {
                        lastTaken.set(index.get(db.getEntity()), db);
                        if (clearPhase.get() && ++polls % 1000 == 0)
                            pending.clear();
                        continue;
                    }

                    if (!clearPhase.get())
                        clearStopped.set(true);

                    if (!pending.prepareToSleep())
                        continue;

                    LockSupport.park();
                }
            } catch (Throwable x) {
                failure.compareAndSet(null, x);
            }
        }, "Reader");
        reader.setDaemon(true);

        final CyclicBarrier randomPhaseDone = new CyclicBarrier(writers, () -> {
            clearPhase.set(false);
            LockSupport.unpark(reader);
        });
        final Thread[] writerThreads = new Thread[writers];
        for (int w = 0; w < writers; w++) {
            final int writer = w;
            writerThreads[w] = new Thread(() -> {
                try {
                    final ThreadLocalRandom random = ThreadLocalRandom.current();
                    for (int i = 0; i < rounds && !stop.get(); i++)
                        offer(pending, blocks[random.nextInt(entities)][random.nextInt(2)], reader);

                    randomPhaseDone.await(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    while (!clearStopped.get()) {
                        if (stop.get())
                            return;
                        Thread.yield();
                    }

                    for (int e = writer; e < entities; e += writers)
                        offer(pending, finals[e], reader);
                } catch (Throwable x) {
                    failure.compareAndSet(null, x);
                }
            }, "Writer #" + w);
            writerThreads[w].setDaemon(true);
        }

        try {
            reader.start();
            for (Thread t : writerThreads)
                t.start();

            for (Thread t : writerThreads) {
                t.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                checkFailure(failure);
                assertFalse(t.getName() + " did not finish in time", t.isAlive());
            }

            awaitAsleep(reader, failure, deadline);
            assertTrue("The reader sleeps while blocks are pending: a wake-up was lost", pending.isEmpty());
            for (int e = 0; e < entities; e++)
                assertSame("Entity " + e + " has a final block the reader never took", finals[e], lastTaken.get(e));
        } finally {
            stop.set(true);
            for (Thread t : writerThreads)
                t.interrupt();
            LockSupport.unpark(reader);
            for (Thread t : writerThreads)
                t.join(TimeUnit.SECONDS.toMillis(10));
            reader.join(TimeUnit.SECONDS.toMillis(10));
        }

        checkFailure(failure);
        assertFalse("The reader did not stop", reader.isAlive());
    }

    /**
     * Spread far enough apart for the slots to grow while writers run.
     */
    private static int entityId(int e) {
        return e * 37;
    }

    private static void offer(PendingBlocks pending, DataBlock db, Thread reader) {
        if (wake(pending, db))
            LockSupport.unpark(reader);
    }

    /**
     * An offer followed right away by the wake-up it may call for, as a writer's flush would send it.
     */
    private static boolean wake(PendingBlocks pending, DataBlock db) {
        return pending.offer(db) && pending.requestWake();
    }

    private static void checkFailure(AtomicReference<Throwable> failure) {
        if (failure.get() != null)
            throw new AssertionError("A test thread failed", failure.get());
    }

    /**
     * Every writer has finished, so every unpark has been issued already: a reader parked now stays parked.
     */
    private static void awaitAsleep(Thread reader, AtomicReference<Throwable> failure, long deadline)
            throws InterruptedException {
        int parkedChecks = 0;
        while (parkedChecks < 5) {
            checkFailure(failure);
            if (System.nanoTime() > deadline)
                fail("The reader did not go to sleep in time");

            parkedChecks = reader.getState() == Thread.State.WAITING ? parkedChecks + 1 : 0;
            Thread.sleep(20);
        }
    }
}
