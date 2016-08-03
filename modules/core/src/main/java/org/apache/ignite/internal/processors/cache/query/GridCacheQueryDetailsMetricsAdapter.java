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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Adapter for {@link QueryMetrics}.
 */
public class GridCacheQueryDetailsMetricsAdapter extends GridCacheQueryBaseMetricsAdapter implements QueryDetailsMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type to track metrics. */
    private CacheQueryType qryType;

    /** Query text representation. */
    private String qry;

    /** {@inheritDoc} */
    @Override  public CacheQueryType queryType() {
        return qryType;
    }

    /** {@inheritDoc} */
    @Override public String query() {
        return qry;
    }

    /**
     * Merge with given metrics.
     *
     * @return Copy.
     */
    public GridCacheQueryDetailsMetricsAdapter copy() {
        GridCacheQueryDetailsMetricsAdapter m = new GridCacheQueryDetailsMetricsAdapter();

        copy(m);

        return m;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeEnum(out, qryType);
        U.writeString(out, qry);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        qryType = CacheQueryType.fromOrdinal(in.readByte());
        qry = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryDetailsMetricsAdapter.class, this);
    }
}
