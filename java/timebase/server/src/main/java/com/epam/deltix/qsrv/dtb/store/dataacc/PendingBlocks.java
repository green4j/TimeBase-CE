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

import net.jcip.annotations.GuardedBy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Blocks with data a live reader has not seen yet, at most one (the latest) per entity, and the
 * reader's sleep/wake-up handshake. Any number of writers may {@link #offer} concurrently; all other
 * methods belong to the single reader.
 * <p>
 * Writers serialize on a lock of their own, which the reader takes only to {@link #clear}. The latest
 * block of an entity sits in a slot, and the entity is appended to a ring on its slot's null -> block
 * transition only, so the ring never holds an entity twice. Slots and the ring grow on demand; a writer
 * takes the reader's lock only to grow the ring.
 */
final class PendingBlocks {
    // Allocation granularity of the slots; not a limit.
    private static final int                    SEGMENT_SHIFT = 10;
    private static final int                    SEGMENT_SIZE = 1 << SEGMENT_SHIFT;
    private static final int                    SEGMENT_MASK = SEGMENT_SIZE - 1;

    private static final int                    NO_ENTITY = -1;

    final Object                                offerLock = new Object();

    // Replaced by a larger copy under offerLock; the segments themselves are shared by the copies.
    private volatile AtomicReferenceArray<AtomicReferenceArray<DataBlock>> segments = new AtomicReferenceArray<>(1);

    // Entities in the order their slots were filled: written by writers under offerLock, read by the reader
    // under this. Indices are absolute, the array is replaced by a larger copy under both locks.
    private volatile int[]                      ring = new int[SEGMENT_SIZE];
    private final AtomicLong                    tail = new AtomicLong();
    private final AtomicLong                    head = new AtomicLong();

    // Set when a wake-up is actually sent or the reader stays awake, cleared by the reader right before it
    // goes to sleep, so writers do not wake up a reader that is busy anyway.
    private final AtomicBoolean                 wakeRequested = new AtomicBoolean();

    /**
     * @return true when the caller has to see to a wake-up of the reader ({@link #requestWake}): none has been
     *         sent since the reader last called {@link #prepareToSleep}, and the reader did not stay awake on
     *         that call. Every writer that offers a block before
     *         that is bound to send one, so a writer that defers its wake-up and stalls does not hold the reader up.
     */
    boolean                                     offer(DataBlock db) {
        synchronized (offerLock) {
            add(db);
        }
        return !wakeRequested.get();
    }

    /**
     * @return true when the caller is the one to send the wake-up
     */
    boolean                                     requestWake() {
        return !wakeRequested.get() && wakeRequested.compareAndSet(false, true);
    }

    /**
     * Called when sending a wake-up granted by {@link #requestWake} failed, so that the next writer sends one.
     */
    void                                        cancelWake() {
        wakeRequested.set(false);
    }

    /**
     * Called by the reader right before it goes to sleep.
     *
     * @return true when the reader may sleep: a writer that offers a block after this call
     *         gets true from {@link #offer} and {@link #requestWake}; false when blocks are pending already,
     *         and the reader, which then stays awake, is not woken up until its next call
     */
    boolean                                     prepareToSleep() {
        wakeRequested.set(false);
        if (isEmpty())
            return true;

        wakeRequested.set(true);
        return false;
    }

    /**
     * Never reports an entity a writer has appended to the ring as absent. It may report one whose slot
     * is filled but which is not appended yet; that writer's {@link #offer} then returns true.
     */
    synchronized boolean                        isEmpty() {
        return head.get() == tail.get();
    }

    synchronized DataBlock                      poll() {
        int entity;
        while ((entity = pop()) != NO_ENTITY) {
            final DataBlock latest = take(entity);
            if (isOf(latest, entity))
                return latest;
        }
        return null;
    }

    /**
     * Drops everything offered before the call; offers after it stay pending. Keeps the grown storage.
     */
    void                                        clear() {
        synchronized (offerLock) {
            synchronized (this) {
                int entity;
                while ((entity = pop()) != NO_ENTITY)
                    take(entity);
            }
        }
    }

    /**
     * Makes the block the latest of its entity; the entity becomes pending unless it is already.
     */
    @GuardedBy("offerLock")
    private void                                add(DataBlock db) {
        final int entity = db.getEntity();
        // A cleared block ({@link DataBlock#clear()}): its file has been deactivated, nothing to read.
        if (entity < 0)
            return;

        // Everything that allocates happens before the slot is filled, so the append below cannot fail.
        final AtomicReferenceArray<DataBlock> blocks = segmentFor(entity >>> SEGMENT_SHIFT);
        ensureRingSpace();

        final int slot = entity & SEGMENT_MASK;
        for (;;) {
            final DataBlock cur = blocks.get(slot);
            if (cur == db)
                return;
            if (cur == null) {
                if (blocks.compareAndSet(slot, null, db)) {
                    append(entity);
                    return;
                }
            } else if (blocks.compareAndSet(slot, cur, db)) {
                return;
            }
        }
    }

    @GuardedBy("offerLock")
    private AtomicReferenceArray<DataBlock>     segmentFor(int index) {
        AtomicReferenceArray<AtomicReferenceArray<DataBlock>> dir = segments;
        if (index >= dir.length()) {
            final AtomicReferenceArray<AtomicReferenceArray<DataBlock>> grown =
                    new AtomicReferenceArray<>(Math.max(index + 1, dir.length() * 2));
            for (int i = 0; i < dir.length(); i++)
                grown.set(i, dir.get(i));
            segments = dir = grown;
        }

        AtomicReferenceArray<DataBlock> segment = dir.get(index);
        if (segment == null)
            dir.set(index, segment = new AtomicReferenceArray<>(SEGMENT_SIZE));
        return segment;
    }

    @GuardedBy("offerLock")
    private void                                ensureRingSpace() {
        if (tail.get() - head.get() < ring.length)
            return;

        synchronized (this) {
            final int[] old = ring;
            final long h = head.get();
            final long t = tail.get();
            if (t - h < old.length)
                return;

            final int[] grown = new int[old.length * 2];
            for (long i = h; i < t; i++)
                grown[(int) i & (grown.length - 1)] = old[(int) i & (old.length - 1)];
            ring = grown;
        }
    }

    @GuardedBy("offerLock")
    private void                                append(int entity) {
        final int[] r = ring;
        final long t = tail.get();
        r[(int) t & (r.length - 1)] = entity;
        // A full volatile write: it must not pass the writer's subsequent read of wakeRequested,
        // as the reader clears wakeRequested and then reads the tail.
        tail.set(t + 1);
    }

    @GuardedBy("this")
    private int                                 pop() {
        final long h = head.get();
        if (h == tail.get())
            return NO_ENTITY;

        final int[] r = ring;
        final int entity = r[(int) h & (r.length - 1)];
        // Writers read the head only to size the ring: a stale one can only make them grow it early
        head.lazySet(h + 1);
        return entity;
    }

    private DataBlock                           take(int entity) {
        return segments.get(entity >>> SEGMENT_SHIFT).getAndSet(entity & SEGMENT_MASK, null);
    }

    /**
     * Drops a block cleared after it was offered. Best effort only: the entity is not volatile.
     */
    private static boolean                      isOf(DataBlock db, int entity) {
        return db != null && db.getEntity() == entity;
    }
}
