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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Adapter for {@link QueryDetailsMetrics}.
 */
public class GridCacheQueryDetailsMetricsAdapter implements QueryDetailsMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type to track metrics. */
    private GridCacheQueryType qryType;

    /** Textual query representation. */
    private String qry;

    /** Cache name. */
    private String cache;

    /** Number of executions. */
    private int execs;

    /** Number of completions executions. */
    private int completions;

    /** Number of failures. */
    private int failures;

    /** Minimum time of execution. */
    private long minTime = -1;

    /** Maximum time of execution. */
    private long maxTime;

    /** Sum of execution time of completions time. */
    private long totalTime;

    /** Sum of execution time of completions time. */
    private long lastStartTime;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueryDetailsMetricsAdapter() {
        // No-op.
    }

    /**
     * Constructor with metrics.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     * @param cache Cache name where query was executed.
     * @param startTime Duration of queue execution.
     * @param duration Duration of queue execution.
     * @param failed {@code True} query executed unsuccessfully {@code false} otherwise.
     * @param completed {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public GridCacheQueryDetailsMetricsAdapter(GridCacheQueryType qryType, String qry, String cache, long startTime,
        long duration, boolean failed, boolean completed) {
        this.qryType = qryType;
        this.qry = qry;
        this.cache = cache;

        if (failed) {
            execs = 1;
            failures = 1;
        }
        else if (completed) {
            execs = 1;
            completions = 1;
            totalTime = duration;
            minTime = duration;
            maxTime = duration;
        }

        lastStartTime = startTime;
    }

    /**
     * Copy constructor.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     * @param cache Cache name where query was executed.
     */
    public GridCacheQueryDetailsMetricsAdapter(GridCacheQueryType qryType, String qry, String cache,
        int execs, int completions, int failures, long minTime, long maxTime, long totalTime, long lastStartTime) {
        this.qryType = qryType;
        this.qry = qry;
        this.cache = cache;
        this.execs = execs;
        this.completions = completions;
        this.failures = failures;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.totalTime = totalTime;
        this.lastStartTime = lastStartTime;
    }

    /**
     * Aggregate metrics.
     *
     * @param m Other metrics to take into account.
     */
    public GridCacheQueryDetailsMetricsAdapter aggregate(QueryDetailsMetrics m) {
        return new GridCacheQueryDetailsMetricsAdapter(
            qryType,
            qry,
            m.getCache(),
            execs + m.getExecutions(),
            completions + m.getCompletions(),
            failures + m.getFailures(),
            minTime < 0 || minTime > m.getMinimumTime() ? m.getMinimumTime() : minTime,
            maxTime < m.getMaximumTime() ? m.getMaximumTime() : maxTime,
            totalTime + m.getTotalTime(),
            lastStartTime < m.getLastStartTime() ? m.getLastStartTime() : lastStartTime
        );
    }

    /**
     * @return Metrics group key.
     */
    public GridCacheQueryDetailsMetricsKey key() {
        return new GridCacheQueryDetailsMetricsKey(qryType, qry);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryType getQueryType() {
        return qryType;
    }

    /** {@inheritDoc} */
    @Override public String getQuery() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public String getCache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public int getExecutions() {
        return execs;
    }

    /** {@inheritDoc} */
    @Override public int getCompletions() {
        return completions;
    }

    /** {@inheritDoc} */
    @Override public int getFailures() {
        return failures;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumTime() {
        return minTime < 0 ? 0 : minTime;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumTime() {
        return maxTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageTime() {
        double val = completions;

        return val > 0 ? totalTime / val : 0;
    }

    /** {@inheritDoc} */
    @Override public long getTotalTime() {
        return totalTime;
    }

    /** {@inheritDoc} */
    @Override public long getLastStartTime() {
        return lastStartTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, qryType);
        U.writeString(out, qry);
        U.writeString(out, cache);
        out.writeInt(execs);
        out.writeInt(completions);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeLong(totalTime);
        out.writeLong(lastStartTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qryType = GridCacheQueryType.fromOrdinal(in.readByte());
        qry = U.readString(in);
        cache = U.readString(in);
        execs = in.readInt();
        completions = in.readInt();
        minTime = in.readLong();
        maxTime = in.readLong();
        totalTime = in.readLong();
        lastStartTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryDetailsMetricsAdapter.class, this);
    }
}
