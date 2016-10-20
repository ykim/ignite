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

import org.apache.ignite.internal.util.typedef.F;

/**
 * Immutable query metrics key used to group metrics.
 */
public class GridCacheQueryDetailsMetricsKey {
    /** Query type to track metrics. */
    private final GridCacheQueryType qryType;

    /** Textual query representation. */
    private final String qry;

    /**
     * Constructor.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     */
    public GridCacheQueryDetailsMetricsKey(GridCacheQueryType qryType, String qry) {
        assert qryType != null;
        assert qry != null;

        this.qryType = qryType;
        this.qry = qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType getQueryType() {
        return qryType;
    }

    /**
     * @return Textual representation of query.
     */
    public String getQuery() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return  31 * qryType.hashCode() + qry.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueryDetailsMetricsKey other = (GridCacheQueryDetailsMetricsKey)o;

        return qryType == other.qryType && F.eq(qry, other.qry);
    }
}
