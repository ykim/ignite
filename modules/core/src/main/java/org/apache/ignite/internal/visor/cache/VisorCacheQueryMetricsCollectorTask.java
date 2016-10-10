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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryDetailsMetrics;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailsMetricsAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isIgfsCache;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;

/**
 * Task to collect cache query metrics.
 */
@GridInternal
public class VisorCacheQueryMetricsCollectorTask extends VisorMultiNodeTask<Void,
    Map<String, Collection<QueryDetailsMetrics>>, Map<String, Collection<QueryDetailsMetrics>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheQueryMetricsCollectorJob job(Void arg) {
        return new VisorCacheQueryMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<String, Collection<QueryDetailsMetrics>> reduce0(List<ComputeJobResult> results)
        throws IgniteException {
        Map<String, Collection<QueryDetailsMetrics>> taskRes = U.newHashMap(results.size());

        Map<Integer, GridCacheQueryDetailsMetricsAdapter> aggMetrics = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() == null) {
                Map<String, Collection<QueryDetailsMetrics>> dm = res.getData();

                for (VisorCacheMetrics cm : cms) {
                    VisorCacheAggregatedMetrics am = grpAggrMetrics.get(cm.name());

                    if (am == null) {
                        am = VisorCacheAggregatedMetrics.from(cm);

                        grpAggrMetrics.put(cm.name(), am);
                    }

                    am.metrics().put(res.getNode().id(), cm);
                }
            }
        }
    }

    /**
     * Job that will actually collect query metrics.
     */
    private static class VisorCacheQueryMetricsCollectorJob extends VisorJob<Void, Map<String, Collection<QueryDetailsMetrics>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected VisorCacheQueryMetricsCollectorJob(@Nullable Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, Collection<QueryDetailsMetrics>> run(@Nullable Void arg) throws IgniteException {
            IgniteConfiguration cfg = ignite.configuration();

            GridCacheProcessor cacheProc = ignite.context().cache();

            Collection<String> cacheNames = cacheProc.cacheNames();

            Map<String, Collection<QueryDetailsMetrics>> res = new HashMap<>(cacheNames.size());

            for (String cacheName : cacheNames) {
                if (!isSystemCache(cacheName) && isIgfsCache(cfg, cacheName)) {
                    GridCacheQueryManager<Object, Object> qryMgr = cacheProc.cache(cacheName).context().queries();

                    res.put(cacheName, qryMgr.detailsMetrics());
                }
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheQueryMetricsCollectorJob.class, this);
        }
    }
}
