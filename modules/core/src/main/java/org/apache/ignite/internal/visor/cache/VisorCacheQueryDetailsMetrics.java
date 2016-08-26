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

import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for cache query metrics.
 */
public class VisorCacheQueryDetailsMetrics extends VisorCacheQueryBaseMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type. */
    private CacheQueryType qryType;

    /** Textual representation of query. */
    private String qry;

    /**
     * @param m Cache query metrics.
     * @return Data transfer object for given cache metrics.
     */
    public VisorCacheQueryDetailsMetrics from(QueryDetailsMetrics m) {
        init(m.minimumTime(), m.maximumTime(), m.averageTime(), m.executions(), m.fails());

        qryType = m.queryType();
        qry = m.query();

        return this;
    }

    /**
     * @return Query type.
     */
    public CacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Textual representation of query.
     */
    public String query() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheQueryDetailsMetrics.class, this);
    }
}
