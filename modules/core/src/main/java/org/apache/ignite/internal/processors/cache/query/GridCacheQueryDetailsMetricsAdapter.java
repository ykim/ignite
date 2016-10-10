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
     * Calculate hash code for query metrics.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     * @return Hash code.
     */
    public static Integer queryHashCode(GridCacheQueryType qryType, String qry) {
        return  31 * qryType.hashCode() + qry.hashCode();
    }

    /**
     * Calculate hash code for query metrics.
     *
     * @param m Metrics.
     * @return Hash code.
     */
    public static Integer queryHashCode(QueryDetailsMetrics m) {
        return  queryHashCode(m.queryType(), m.query());
    }

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueryDetailsMetricsAdapter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     */
    public GridCacheQueryDetailsMetricsAdapter(GridCacheQueryType qryType, String qry) {
        this.qryType = qryType;
        this.qry = qry;
    }

    /**
     * Update metrics on query execution.
     *
     * @param startTime Duration of queue execution.
     * @param duration Duration of queue execution.
     * @param failed {@code True} query executed unsuccessfully {@code false} otherwise.
     * @param completed {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void update(long startTime, long duration, boolean failed, boolean completed) {
        lastStartTime = startTime;

        if (failed) {
            execs += 1;
            failures += 1;
        }
        else if (completed) {
            execs += 1;
            completions += 1;

            totalTime += duration;

            if (minTime < 0 || minTime > duration)
                minTime = duration;

            if (maxTime < duration)
                maxTime = duration;
        }
    }

    public void update(QueryDetailsMetrics m) {
        if (lastStartTime < m.lastStartTime())
            lastStartTime = m.lastStartTime();

        execs += m.executions();
        failures += m.failures();
        completions += m.completions();

        totalTime += m.totalTime();

        if (minTime < 0 || minTime < m.minimumTime())
            minTime = m.maximumTime();

        if (maxTime < m.maximumTime())
            maxTime = m.maximumTime();
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryType queryType() {
        return qryType;
    }

    /** {@inheritDoc} */
    @Override public String query() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return execs;
    }

    /** {@inheritDoc} */
    @Override public int completions() {
        return completions;
    }

    /** {@inheritDoc} */
    @Override public int failures() {
        return failures;
    }

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        return minTime < 0 ? 0 : minTime;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime;
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        double val = completions;

        return val > 0 ? totalTime / val : 0;
    }

    /** {@inheritDoc} */
    @Override public long totalTime() {
        return totalTime;
    }

    /** {@inheritDoc} */
    @Override public long lastStartTime() {
        return lastStartTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, qryType);
        U.writeString(out, qry);
        out.writeInt(execs);
        out.writeInt(completions);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeLong(totalTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qryType = GridCacheQueryType.fromOrdinal(in.readByte());
        qry = U.readString(in);
        execs = in.readInt();
        completions = in.readInt();
        minTime = in.readLong();
        maxTime = in.readLong();
        totalTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryDetailsMetricsAdapter.class, this);
    }
}
