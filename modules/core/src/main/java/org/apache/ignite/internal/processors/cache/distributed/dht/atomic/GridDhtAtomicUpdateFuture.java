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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic cache backup update future.
 */
public class GridDhtAtomicUpdateFuture extends GridDhtAtomicAbstractUpdateFuture {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future keys. */
    private final List<KeyCacheObject> keys;

    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    public GridDhtAtomicUpdateFuture(
        GridCacheContext cctx,
        CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicAbstractUpdateResponse> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq,
        GridNearAtomicAbstractUpdateResponse updateRes
    ) {
        super(cctx, updateReq, updateRes, completionCb, writeVer);

        keys = new ArrayList<>(updateReq.keysCount());
    }

    /**
     * Add key.
     *
     * @param key Key.
     */
    protected void addKey(KeyCacheObject key) {
        keys.add(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override protected void markAllKeysFailed(@Nullable Throwable err) {
        for (int i = 0; i < keys.size(); i++)
            updateRes.addFailedKey(keys.get(i), err);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateFuture.class, this);
    }
}
