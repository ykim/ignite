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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.UUID;

/**
 * Abstract near ATOMIC update response.
 */
public abstract class GridNearAtomicAbstractUpdateResponse extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID this response should be sent to.
     */
    public abstract UUID nodeId();

    /**
     * @param nodeId Node ID.
     */
    public abstract void nodeId(UUID nodeId);

    /**
     * @return Future version.
     */
    public abstract GridCacheVersion futureVersion();

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    public abstract void error(IgniteCheckedException err);

    /**
     * @return Failed keys count.
     */
    public abstract int failedKeysCount();

    /**
     * @param idx Index.
     * @return Failed key.
     */
    public abstract KeyCacheObject failedKey(int idx);

    /**
     * @return Return value.
     */
    public abstract GridCacheReturn returnValue();

    /**
     * @param ret Return value.
     */
    @SuppressWarnings("unchecked")
    public abstract void returnValue(GridCacheReturn ret);

    /**
     * @param req Request.
     */
    public abstract void remapKeys(GridNearAtomicAbstractUpdateRequest req);

    /**
     * @param idx Index.
     * @return Remap key.
     */
    public abstract KeyCacheObject remapKey(int idx);

    /**
     * @return Remap keys count.
     */
    public abstract int remapKeysCount();

    /**
     * Adds value to be put in near cache on originating node.
     *
     * @param keyIdx Key index.
     * @param val Value.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    public abstract void addNearValue(int keyIdx, @Nullable CacheObject val, long ttl, long expireTime);

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    public abstract void addNearTtl(int keyIdx, long ttl, long expireTime);

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public abstract long nearExpireTime(int idx);

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public abstract long nearTtl(int idx);

    /**
     * @param nearVer Version generated on primary node to be used for originating node's near cache update.
     */
    public abstract void nearVersion(GridCacheVersion nearVer);

    /**
     * @return Version generated on primary node to be used for originating node's near cache update.
     */
    public abstract GridCacheVersion nearVersion();

    /**
     * @param keyIdx Index of key for which update was skipped
     */
    public abstract void addSkippedIndex(int keyIdx);

    /**
     * Check if update was skipped for the given index.
     *
     * @param idx Index.
     * @return {@code True} if skipped.
     */
    public abstract boolean isNearSkippedIndex(int idx);

    /**
     * Check if this is an index of a key for which values were generated on primary node.
     *
     * @param idx Index.
     * @return {@code True} if values were generated on primary node.
     */
    public abstract boolean isNearValueIndex(int idx);

    /**
     * @param idx Index.
     * @return Value generated on primary node which should be put to originating node's near cache.
     */
    @Nullable public abstract CacheObject nearValue(int idx);

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public abstract void addFailedKey(KeyCacheObject key, Throwable e);

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     */
    public abstract void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e);

    /**
     * Add keys to collection of failed keys.
     *
     * @param req Request.
     * @param e Error cause.
     */
    public abstract void addFailedKeys(GridNearAtomicAbstractUpdateRequest req, Throwable e);

    /**
     * @return {@code True} is this is a response for a single key-value pair update.
     */
    public abstract boolean isSingle();
}
