/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.memory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import org.junit.Test;


public class NativeAllocatorRegion2Test
{

    @Test
    public void testRegionCAS() throws InterruptedException
    {
        int maxThreads = 512;
        // imitate different concurrent write values, allocating lots of NativeCells
        for( int concurrentWrites = 4 ; concurrentWrites <= maxThreads ; concurrentWrites *= 2 )
        {
            ExecutorService executor = Executors.newFixedThreadPool(concurrentWrites);
            final Region region = new Region(0, Integer.MAX_VALUE);
            long start = System.currentTimeMillis();

            IntStream.range(0, maxThreads)
                    .forEach(consumer -> executor.submit(
                            () -> { for (long i = 0 ; i >= 0 ; i = region.allocate(10)); }));

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

            System.out.println("[" + concurrentWrites + "] " + region + " took: " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    //@Test
    public void testRegionThreadLocal() throws InterruptedException
    {
        int maxThreads = 512;
        // imitate different concurrent write values, allocating lots of NativeCells
        for( int c = 4 ; c <= maxThreads ; c *= 2 )
        {
            final int concurrentWrites = c;
            ExecutorService executor = Executors.newFixedThreadPool(concurrentWrites);

            final ThreadLocal<AtomicReference<Region>> currentRegion = new ThreadLocal<AtomicReference<Region>>() {
                @Override
                protected AtomicReference<Region> initialValue()
                {
                    return new AtomicReference();
                }

            };

            long start = System.currentTimeMillis();

            IntStream.range(0, maxThreads)
                    .forEach(consumer -> executor.submit(
                            () ->
                            {
                                currentRegion.get().compareAndSet(null, new Region(0, Integer.MAX_VALUE / concurrentWrites));
                                for (long i = 0 ; i >= 0 ; i = currentRegion.get().get().allocate(10));
                            }));

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

            System.out.println("[TL][" + concurrentWrites + "] " + currentRegion.get() + " took: " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    private static class Region
    {
        private final long peer;
        private final int capacity;

        private final AtomicInteger nextFreeOffset = new AtomicInteger(0);
        private final AtomicInteger allocCount = new AtomicInteger();


        private Region(long peer, int capacity)
        {
            this.peer = peer;
            this.capacity = capacity;
        }

        long allocate(int size)
        {
            int newOffset = nextFreeOffset.addAndGet(size);

            if (newOffset > capacity) // capacity == remaining
                return -1;

            // we got the alloc
            allocCount.getAndIncrement();
            return peer + (newOffset - size);
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                    " allocs=" + allocCount.get()
                    + "waste=" + (capacity - nextFreeOffset.get());
        }
    }
}
