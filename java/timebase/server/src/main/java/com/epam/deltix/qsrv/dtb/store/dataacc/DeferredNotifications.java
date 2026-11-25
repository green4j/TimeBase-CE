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

import java.util.Arrays;

/**
 * Per-thread batch of reader wake-ups. Between {@link #begin()} and the matching {@link #end()}, a reader's
 * wake-up runs once, on the next {@link #flush()}, instead of once per inserted message.
 * Outside a batch wake-ups run immediately.
 */
public final class DeferredNotifications {
    private static final ThreadLocal<DeferredNotifications> CURRENT = ThreadLocal.withInitial(DeferredNotifications::new);

    private Runnable[]                  wakeUps = new Runnable[16];
    private int                         size;
    private int                         depth;

    private DeferredNotifications() {
    }

    public static DeferredNotifications current() {
        return CURRENT.get();
    }

    public void                         begin() {
        depth++;
    }

    public void                         end() {
        if (depth == 0)
            throw new IllegalStateException("end() without begin()");

        if (--depth == 0)
            flush();
    }

    public boolean                      hasPending() {
        return size != 0;
    }

    public void                         flush() {
        final Runnable[] w = wakeUps;
        final int n = size;
        size = 0;

        RuntimeException error = null;
        for (int i = 0; i < n; i++) {
            final Runnable wakeUp = w[i];
            w[i] = null;
            try {
                wakeUp.run();
            } catch (RuntimeException x) {
                if (error == null)
                    error = x;
                else
                    error.addSuppressed(x);
            }
        }

        if (error != null)
            throw error;
    }

    static void                         submit(Runnable wakeUp) {
        final DeferredNotifications d = CURRENT.get();
        if (d.depth == 0) {
            wakeUp.run();
            return;
        }

        final Runnable[] w = d.wakeUps;
        for (int i = 0, n = d.size; i < n; i++) {
            if (w[i] == wakeUp)
                return;
        }

        if (d.size == w.length)
            d.wakeUps = Arrays.copyOf(w, w.length * 2);
        d.wakeUps[d.size++] = wakeUp;
    }
}
