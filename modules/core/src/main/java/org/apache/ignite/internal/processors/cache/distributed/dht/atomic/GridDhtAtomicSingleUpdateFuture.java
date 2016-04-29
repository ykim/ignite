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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic single update future.
 */
public class GridDhtAtomicSingleUpdateFuture extends GridDhtAtomicAbstractUpdateFuture {
    /** Key. */
    private KeyCacheObject key;

    /** Entry with reader. */
    private GridDhtCacheEntry nearReaderEntry;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param completionCb Completion callback.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    public GridDhtAtomicSingleUpdateFuture(
        GridCacheContext cctx,
        CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicAbstractUpdateResponse> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq,
        GridNearAtomicAbstractUpdateResponse updateRes
    ) {
        super(cctx, updateReq, updateRes, completionCb, writeVer);
    }

    /** {@inheritDoc} */
    @Override protected void addKey(KeyCacheObject key) {
        // With current implementation key could be set twice in case of near readers. Though, it will be the same key.
        assert this.key == null || F.eq(this.key, key);

        this.key = key;
    }

    /** {@inheritDoc} */
    @Override protected void markAllKeysFailed(@Nullable Throwable err) {
        updateRes.addFailedKey(key, err);
    }

    /** {@inheritDoc} */
    @Override protected void nearReaderEntry(KeyCacheObject key, GridDhtCacheEntry entry) {
        assert F.eq(this.key, key);
        assert nearReaderEntry == null;

        nearReaderEntry = entry;
    }

    /** {@inheritDoc} */
    @Override protected GridDhtCacheEntry nearReaderEntry(KeyCacheObject key) {
        assert F.eq(this.key, key);

        return nearReaderEntry;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicSingleUpdateFuture.class, this);
    }
}
