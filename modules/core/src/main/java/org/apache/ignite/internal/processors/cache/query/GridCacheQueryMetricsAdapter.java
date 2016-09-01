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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 * Adapter for {@link QueryMetrics}.
 */
public class GridCacheQueryMetricsAdapter extends GridCacheQueryBaseMetricsAdapter implements QueryMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** History size. */
    private final int detailsHistSz;

    /** Map with metrics history for latest queries. */
    private final ConcurrentMap<QueryMetricsKey, GridCacheQueryDetailsMetricsAdapter> details;

    /**
     * @param detailsHistSz Query metrics history size.
     */
    public GridCacheQueryMetricsAdapter(int detailsHistSz) {
        this.detailsHistSz = detailsHistSz;

        details = new ConcurrentLinkedHashMap<>(detailsHistSz, 0.75f, 16, detailsHistSz > 0 ? detailsHistSz : 1);
    }

    /** {@inheritDoc} */
    @Override public List<QueryDetailsMetrics> details() {
        return detailsHistSz > 0 ? new ArrayList<QueryDetailsMetrics>(details.values()) : Collections.<QueryDetailsMetrics>emptyList();
    }

    /**
     * Callback for completion of query execution.
     *
     * @param qryType Query type.
     * @param qry Query description.
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void onQueryCompleted(GridCacheQueryType qryType, String qry, long duration, boolean fail) {
        onQueryCompleted(duration, fail);

        QueryMetricsKey key = new QueryMetricsKey(qryType, qry);

        if (detailsHistSz > 0) {
            if (!details.containsKey(key))
                details.putIfAbsent(key, new GridCacheQueryDetailsMetricsAdapter());

            GridCacheQueryDetailsMetricsAdapter dm = details.get(key);

            dm.onQueryCompleted(duration, fail);
        }
    }

    /**
     * Merge with given metrics.
     *
     * @return Copy.
     */
    public GridCacheQueryMetricsAdapter copy() {
        GridCacheQueryMetricsAdapter m = new GridCacheQueryMetricsAdapter(detailsHistSz);

        // Not synchronized because accuracy isn't critical.
        copy(m);

        if (detailsHistSz > 0)
            m.details.putAll(details);

        return m;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeMap(out, details);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        details.putAll(U.<QueryMetricsKey, GridCacheQueryDetailsMetricsAdapter>readMap(in));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryMetricsAdapter.class, this);
    }

    /**
     * Key for query metrics to store in map.
     */
    private static class QueryMetricsKey implements Externalizable {
        /** Query type. */
        private GridCacheQueryType qryType;

        /** Query text descriptor: SQL, cache name, search text, ... */
        private String qry;


        /**
         * Required by {@link Externalizable}.
         */
        public QueryMetricsKey() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param qryType Query type.
         * @param qry Query text descriptor.
         */
        public QueryMetricsKey(GridCacheQueryType qryType, String qry) {
            this.qryType = qryType;
            this.qry = qry;
        }

        /**
         * @return Query type.
         */
        public GridCacheQueryType queryType() {
            return qryType;
        }

        /**
         * @return Query text descriptor: SQL, cache name, search text, ...
         */
        public String query() {
            return qry;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeEnum(out, qryType);
            U.writeString(out, qry);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            qryType = GridCacheQueryType.fromOrdinal(in.readByte());
            qry = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryMetricsKey key = (QueryMetricsKey)o;

            return qryType == key.qryType && qry.equals(key.qry);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = qryType.hashCode();

            res = 31 * res + qry.hashCode();

            return res;
        }
    }
}
