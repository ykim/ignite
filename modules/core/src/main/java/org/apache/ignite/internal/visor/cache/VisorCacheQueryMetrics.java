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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for cache query metrics.
 */
public class VisorCacheQueryMetrics extends VisorCacheQueryBaseMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of query metrics aggregated by query type and textual representation. */
    private List<VisorCacheQueryDetailsMetrics> details;

    /**
     * @param m Cache query metrics.
     * @return Data transfer object for given cache metrics.
     */
    public VisorCacheQueryMetrics from(QueryMetrics m) {
        init(m.minimumTime(), m.maximumTime(), m.averageTime(), m.executions(), m.fails());

        List<QueryDetailsMetrics> mds = m.details();

        details = new ArrayList<>(mds.size());

        for (QueryDetailsMetrics md : mds)
            details.add(new VisorCacheQueryDetailsMetrics().from(md));

        return this;
    }

    /**
     * @return List of query metrics aggregated by query type and textual representation.
     */
    public List<VisorCacheQueryDetailsMetrics> details() {
        return details;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheQueryMetrics.class, this);
    }
}
