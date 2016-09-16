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

    /** Number of completed executions. */
    private int completed;

    /** Number of fails. */
    private int fails;

    /** Minimum time of execution. */
    private long minTime;

    /** Maximum time of execution. */
    private long maxTime;

    /** Sum of execution time of completed time. */
    private long totalTime;

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
     * Callback for query execution.
     *
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void onQueryExecute(boolean fail) {
        execs += 1;

        if (fail)
            fails += 1;
    }

    /**
     * Callback for completion of query execution.
     *
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void onQueryCompleted(long duration, boolean fail) {
        if (fail)
            fails += 1;
        else {
            completed += 1;

            totalTime += duration;

            if (minTime == 0 || minTime > duration)
                minTime = duration;

            if (maxTime < duration)
                maxTime = duration;
        }
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
    @Override public int completed() {
        return completed;
    }

    /** {@inheritDoc} */
    @Override public int fails() {
        return execs - completed;
    }

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        return minTime;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime;
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        double val = completed;

        return val > 0 ? totalTime / val : 0;
    }

    /** {@inheritDoc} */
    @Override public long totalTime() {
        return totalTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, qryType);
        U.writeString(out, qry);
        out.writeInt(execs);
        out.writeInt(completed);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeLong(totalTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qryType = GridCacheQueryType.fromOrdinal(in.readByte());
        qry = U.readString(in);
        execs = in.readInt();
        completed = in.readInt();
        minTime = in.readLong();
        maxTime = in.readLong();
        totalTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryDetailsMetricsAdapter.class, this);
    }
}
