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
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for cache query metrics.
 */
public class VisorCacheQueryDetailsMetrics implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type. */
    private GridCacheQueryType qryType;

    /** Textual representation of query. */
    private String qry;

    /** Minimum execution time of query. */
    private long minTime;

    /** Maximum execution time of query. */
    private long maxTime;

    /** Average execution time of query. */
    private double avgTime;

    /** Number of query executions. */
    private int execs;

    /** Number of failed queries. */
    private int failures;

    /** Number of completed queries. */
    private int completions;

    /** Latest time query was stared. */
    private long lastStartTime;

    /**
     * @param m Cache query metrics.
     * @return Data transfer object for given cache metrics.
     */
    public VisorCacheQueryDetailsMetrics from(QueryDetailsMetrics m) {
        qryType = m.queryType();
        qry = m.query();
        minTime = m.minimumTime();
        maxTime = m.maximumTime();
        avgTime = m.averageTime();
        execs = m.executions();
        failures = m.failures();
        completions = m.completions();
        lastStartTime = m.lastStartTime();

        return this;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Textual representation of query.
     */
    public String query() {
        return qry;
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
    public int failures() {
        return failures;
    }

    /**
     * @return Total number of times a query execution completed.
     */
    public int completions() {
        return completions;
    }

    /**
     * @return Latest time query was stared.
     */
    public long lastStartTime() {
        return lastStartTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheQueryDetailsMetrics.class, this);
    }
}
