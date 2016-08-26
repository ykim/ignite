/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Base class for data transfer object with cache query metrics.
 */
public abstract class VisorCacheQueryBaseMetrics implements Serializable, LessNamingBean {
    /** Minimum execution time of query. */
    private long minTime;

    /** Maximum execution time of query. */
    private long maxTime;

    /** Average execution time of query. */
    private double avgTime;

    /** Number of executions. */
    private int execs;

    /** Number of executions failed. */
    private int fails;

    /**
     * Initialize metrics.
     *
     * @param minTime Minimum execution time of query.
     * @param maxTime Maximum execution time of query.
     * @param avgTime Average execution time of query.
     * @param execs Number of executions.
     * @param fails Number of executions failed.
     */
    protected void init(long minTime, long maxTime, double avgTime, int execs, int fails) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.avgTime = avgTime;
        this.execs = execs;
        this.fails = fails;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long minimumTime() {
        return minTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long maximumTime() {
        return maxTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double averageTime() {
        return avgTime;
    }

    /**
     * @return Number of executions.
     */
    public int executions() {
        return execs;
    }

    /**
     * @return Total number of times a query execution failed.
     */
    public int fails() {
        return fails;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheQueryBaseMetrics.class, this);
    }
}
