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

package org.apache.ignite.internal.visor.query;

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailsMetricsAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailsMetricsKey;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task to collect query metrics.
 */
@GridInternal
public class VisorQueryMetricsCollectorTask extends VisorOneNodeTask<Void, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryMetricsCollectorJob job(Void arg) {
        return new VisorQueryMetricsCollectorJob(arg, debug);
    }

    /**
     * Job that will actually collect query metrics.
     */
    private static class VisorQueryMetricsCollectorJob extends VisorJob<Void, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected VisorQueryMetricsCollectorJob(@Nullable Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(@Nullable Void arg) throws IgniteException {
//            IgniteInternalCache<GridCacheQueryDetailsMetricsKey, GridCacheQueryDetailsMetricsAdapter> cache = ignite.utilityCache();
//
//            for (Cache.Entry<GridCacheQueryDetailsMetricsKey, GridCacheQueryDetailsMetricsAdapter> entry : cache) {
//                entry.
//            }

            return "IGNITE-3443: TODO Metrics";
        }
    }
}
