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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailsMetricsAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isIgfsCache;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;

/**
 * Task to collect cache query metrics.
 */
@GridInternal
public class VisorCacheQueryMetricsCollectorTask extends VisorMultiNodeTask<Long, Collection<? extends QueryDetailsMetrics>,
    Collection<? extends QueryDetailsMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheQueryMetricsCollectorJob job(Long arg) {
        return new VisorCacheQueryMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Collection<? extends QueryDetailsMetrics> reduce0(List<ComputeJobResult> results)
        throws IgniteException {
        Map<Integer, GridCacheQueryDetailsMetricsAdapter> taskRes = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            Collection<QueryDetailsMetrics> metrics = res.getData();

            VisorCacheQueryMetricsCollectorJob.aggregateMetrics(-1, taskRes, metrics);
        }

        return new ArrayList<>(taskRes.values());
    }

    /**
     * Job that will actually collect query metrics.
     */
    private static class VisorCacheQueryMetricsCollectorJob extends VisorJob<Long, Collection<? extends QueryDetailsMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Last time when metrics were collected.
         * @param debug Debug flag.
         */
        protected VisorCacheQueryMetricsCollectorJob(@Nullable Long arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * @param since Time when metrics were collected last time.
         * @param res Response.
         * @param metrics Metrics.
         */
        private static void aggregateMetrics(long since, Map<Integer, GridCacheQueryDetailsMetricsAdapter> res,
            Collection<QueryDetailsMetrics> metrics) {
            if (!metrics.isEmpty()) {
                for (QueryDetailsMetrics m : metrics) {
                    if (m.getLastStartTime() > since) {
                        Integer qryHashCode = GridCacheQueryDetailsMetricsAdapter.queryHashCode(m);

                        GridCacheQueryDetailsMetricsAdapter aggMetrics = res.get(qryHashCode);

                        if (aggMetrics == null) {
                            aggMetrics = new GridCacheQueryDetailsMetricsAdapter(m.getQueryType(), m.getQuery());

                            res.put(qryHashCode, aggMetrics);
                        }

                        aggMetrics.aggregate(m);
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends QueryDetailsMetrics> run(@Nullable Long arg) throws IgniteException {
            assert arg != null;

            IgniteConfiguration cfg = ignite.configuration();

            GridCacheProcessor cacheProc = ignite.context().cache();

            Collection<String> cacheNames = cacheProc.cacheNames();

            Map<Integer, GridCacheQueryDetailsMetricsAdapter> jobRes = new HashMap<>();

            for (String cacheName : cacheNames) {
                if (!isSystemCache(cacheName) && !isIgfsCache(cfg, cacheName)) {
                    IgniteInternalCache<Object, Object> cache = cacheProc.cache(cacheName);

                    if (cache == null || !cache.context().started())
                        continue;

                    aggregateMetrics(arg, jobRes, cache.context().queries().detailsMetrics());
                }
            }

            return new ArrayList<>(jobRes.values());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheQueryMetricsCollectorJob.class, this);
        }
    }
}
