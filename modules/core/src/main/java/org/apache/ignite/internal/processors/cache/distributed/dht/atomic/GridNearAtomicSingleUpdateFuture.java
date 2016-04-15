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
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicSingleUpdateFuture extends GridAbstractNearAtomicUpdateFuture {
    /** Keys */
    private Object key;

    /** Values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Object val;

    /** Optional arguments for entry processor. */
    private Object[] invokeArgs;

    /** Mappings if operations is mapped to more than one node. */
    @GridToStringInclude
    private Map<UUID, GridNearAtomicUpdateRequest> mappings;

    /** Keys to remap. */
    private boolean remapKey;

    /** Not null is operation is mapped to single node. */
    private GridNearAtomicUpdateRequest singleReq;

    /**
     * @param cctx Cache context.
     * @param cache Cache instance.
     * @param syncMode Write synchronization mode.
     * @param op Update operation.
     * @param key Keys to update.
     * @param val Values or transform closure.
     * @param invokeArgs Optional arguments for entry processor.
     * @param retval Return value require flag.
     * @param rawRetval {@code True} if should return {@code GridCacheReturn} as future result.
     * @param expiryPlc Expiry policy explicitly specified for cache operation.
     * @param filter Entry filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Maximum number of retries.
     * @param waitTopFut If {@code false} does not wait for affinity change future.
     */
    public GridNearAtomicSingleUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        Object key,
        @Nullable Object val,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        final boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        final CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        int remapCnt,
        boolean waitTopFut
    ) {
        super(cctx, cache, syncMode, op, retval, rawRetval, expiryPlc, filter, subjId, taskNameHash, skipStore,
            keepBinary, waitTopFut);

        assert subjId != null;

        this.key = key;
        this.val = val;
        this.invokeArgs = invokeArgs;

        if (!waitTopFut)
            remapCnt = 1;

        this.remapCnt = remapCnt;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        synchronized (mux) {
            return futVer;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        GridNearAtomicUpdateResponse res = null;

        synchronized (mux) {
            GridNearAtomicUpdateRequest req;

            if (singleReq != null)
                req = singleReq.nodeId().equals(nodeId) ? singleReq : null;
            else
                req = mappings != null ? mappings.get(nodeId) : null;

            if (req != null && req.response() == null) {
                res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                    nodeId,
                    req.futureVersion(),
                    cctx.deploymentEnabled());

                ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Primary node left grid " +
                    "before response is received: " + nodeId);

                e.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(req.topologyVersion()));

                res.addFailedKeys(req.keys(), e);
            }
        }

        if (res != null)
            onResult(nodeId, res, true);

        return false;
    }

    /**
     * Performs future mapping.
     */
    public void map() {
        AffinityTopologyVersion topVer = cctx.shared().lockedTopologyVersion(null);

        if (topVer == null)
            mapOnTopology();
        else {
            topLocked = true;

            // Cannot remap.
            remapCnt = 1;

            map(topVer);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        // Wait fast-map near atomic update futures in CLOCK mode.
        if (fastMap) {
            GridFutureAdapter<Void> fut;

            synchronized (mux) {
                if (this.topVer != AffinityTopologyVersion.ZERO && this.topVer.compareTo(topVer) < 0) {
                    if (topCompleteFut == null)
                        topCompleteFut = new GridFutureAdapter<>();

                    fut = topCompleteFut;
                }
                else
                    fut = null;
            }

            if (fut != null && isDone()) {
                fut.onDone();

                return null;
            }

            return fut;
        }

        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        assert res == null || res instanceof GridCacheReturn;

        GridCacheReturn ret = (GridCacheReturn)res;

        Object retval =
            res == null ? null : rawRetval ? ret : (this.retval || op == TRANSFORM) ?
                cctx.unwrapBinaryIfNeeded(ret.value(), keepBinary) : ret.success();

        if (op == TRANSFORM && retval == null)
            retval = Collections.emptyMap();

        if (super.onDone(retval, err)) {
            GridCacheVersion futVer = onFutureDone();

            if (futVer != null)
                cctx.mvcc().removeAtomicFuture(futVer);

            return true;
        }

        return false;
    }

    /**
     * Response callback.
     *
     * @param nodeId Node ID.
     * @param res Update response.
     * @param nodeErr {@code True} if response was created on node failure.
     */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    public void onResult(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr) {
        GridNearAtomicUpdateRequest req;

        AffinityTopologyVersion remapTopVer = null;

        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;

        boolean rcvAll;

        GridFutureAdapter<?> fut0 = null;

        synchronized (mux) {
            if (!res.futureVersion().equals(futVer))
                return;

            if (singleReq != null) {
                if (!singleReq.nodeId().equals(nodeId))
                    return;

                req = singleReq;

                singleReq = null;

                rcvAll = true;
            }
            else {
                req = mappings != null ? mappings.get(nodeId) : null;

                if (req != null && req.onResponse(res)) {
                    resCnt++;

                    rcvAll = mappings.size() == resCnt;
                }
                else
                    return;
            }

            assert req != null && req.topologyVersion().equals(topVer) : req;

            if (res.remapKeys() != null) {
                assert !fastMap || cctx.kernalContext().clientNode();

                remapKey = true;

                if (mapErrTopVer == null || mapErrTopVer.compareTo(req.topologyVersion()) < 0)
                    mapErrTopVer = req.topologyVersion();
            }
            else if (res.error() != null) {
                if (res.failedKeys() != null) {
                    if (err == null)
                        err = new CachePartialUpdateCheckedException(
                            "Failed to update keys (retry update if possible).");

                    Collection<Object> keys = new ArrayList<>(res.failedKeys().size());

                    for (KeyCacheObject key : res.failedKeys())
                        keys.add(cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false));

                    err.add(keys, res.error(), req.topologyVersion());
                }
            }
            else {
                if (!req.fastMap() || req.hasPrimary()) {
                    GridCacheReturn ret = res.returnValue();

                    if (op == TRANSFORM) {
                        if (ret != null) {
                            assert ret.value() == null || ret.value() instanceof Map : ret.value();

                            if (ret.value() != null) {
                                if (opRes != null)
                                    opRes.mergeEntryProcessResults(ret);
                                else
                                    opRes = ret;
                            }
                        }
                    }
                    else
                        opRes = ret;
                }
            }

            if (rcvAll) {
                if (remapKey) {
                    assert mapErrTopVer != null;

                    remapTopVer = cctx.shared().exchange().topologyVersion();
                }
                else {
                    if (err != null &&
                        X.hasCause(err, CachePartialUpdateCheckedException.class) &&
                        X.hasCause(err, ClusterTopologyCheckedException.class) &&
                        storeFuture() &&
                        --remapCnt > 0) {
                        ClusterTopologyCheckedException topErr =
                            X.cause(err, ClusterTopologyCheckedException.class);

                        if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                            CachePartialUpdateCheckedException cause =
                                X.cause(err, CachePartialUpdateCheckedException.class);

                            assert cause != null && cause.topologyVersion() != null : err;

                            remapTopVer =
                                new AffinityTopologyVersion(cause.topologyVersion().topologyVersion() + 1);

                            err = null;

                            remapKey = true;

                            updVer = null;
                        }
                    }
                }

                if (remapTopVer == null) {
                    err0 = err;
                    opRes0 = opRes;
                }
                else {
                    fut0 = topCompleteFut;

                    topCompleteFut = null;

                    cctx.mvcc().removeAtomicFuture(futVer);

                    futVer = null;
                    topVer = AffinityTopologyVersion.ZERO;
                }
            }
        }

        if (res.error() != null && res.failedKeys() == null) {
            onDone(res.error());

            return;
        }

        if (rcvAll && nearEnabled) {
            if (mappings != null) {
                for (GridNearAtomicUpdateRequest req0 : mappings.values()) {
                    GridNearAtomicUpdateResponse res0 = req0.response();

                    assert res0 != null : req0;

                    updateNear(req0, res0);
                }
            }
            else if (!nodeErr)
                updateNear(req, res);
        }

        if (remapTopVer != null) {
            if (fut0 != null)
                fut0.onDone();

            if (!waitTopFut) {
                onDone(new GridCacheTryPutFailedException());

                return;
            }

            if (topLocked) {
                assert remapKey;

                CachePartialUpdateCheckedException e =
                    new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

                ClusterTopologyCheckedException cause = new ClusterTopologyCheckedException(
                    "Failed to update keys, topology changed while execute atomic update inside transaction.");

                cause.retryReadyFuture(cctx.affinity().affinityReadyFuture(remapTopVer));

                e.add(Collections.singleton(key), cause);

                onDone(e);

                return;
            }

            IgniteInternalFuture<AffinityTopologyVersion> fut =
                cctx.shared().exchange().affinityReadyFuture(remapTopVer);

            if (fut == null)
                fut = new GridFinishedFuture<>(remapTopVer);

            fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(final IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            try {
                                AffinityTopologyVersion topVer = fut.get();

                                map(topVer);
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);
                            }
                        }
                    });
                }
            });

            return;
        }

        if (rcvAll)
            onDone(opRes0, err0);
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    private void updateNear(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
        assert nearEnabled;

        if (res.remapKeys() != null || !req.hasPrimary())
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /**
     * Maps future on ready topology.
     */
    private void mapOnTopology() {
        cache.topology().readLock();

        AffinityTopologyVersion topVer = null;

        try {
            if (cache.topology().stopping()) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                    cache.name()));

                return;
            }

            GridDhtTopologyFuture fut = cache.topology().topologyVersionFuture();

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx);

                if (err != null) {
                    onDone(err);

                    return;
                }

                topVer = fut.topologyVersion();
            }
            else {
                if (waitTopFut) {
                    assert !topLocked : this;

                    fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                            cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                                @Override public void run() {
                                    mapOnTopology();
                                }
                            });
                        }
                    });
                }
                else
                    onDone(new GridCacheTryPutFailedException());

                return;
            }
        }
        finally {
            cache.topology().readUnlock();
        }

        map(topVer);
    }

    /**
     * Maps future to single node.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void mapSingle(UUID nodeId, GridNearAtomicUpdateRequest req) {
        if (cctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req,
                new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onResult(res.nodeId(), res, false);
                    }
                });
        }
        else {
            try {
                if (log.isDebugEnabled())
                    log.debug("Sending near atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (syncMode == FULL_ASYNC)
                    onDone(new GridCacheReturn(cctx, true, true, null, true));
            }
            catch (IgniteCheckedException e) {
                onSendError(req, e);
            }
        }
    }

    /**
     * Sends messages to remote nodes and updates local cache.
     *
     * @param mappings Mappings to send.
     */
    private void doUpdate(Map<UUID, GridNearAtomicUpdateRequest> mappings) {
        UUID locNodeId = cctx.localNodeId();

        GridNearAtomicUpdateRequest locUpdate = null;

        // Send messages to remote nodes first, then run local update.
        for (GridNearAtomicUpdateRequest req : mappings.values()) {
            if (locNodeId.equals(req.nodeId())) {
                assert locUpdate == null : "Cannot have more than one local mapping [locUpdate=" + locUpdate +
                    ", req=" + req + ']';

                locUpdate = req;
            }
            else {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending near atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

                    cctx.io().send(req.nodeId(), req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    onSendError(req, e);
                }
            }
        }

        if (locUpdate != null) {
            cache.updateAllAsyncInternal(cctx.localNodeId(), locUpdate,
                new CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onResult(res.nodeId(), res, false);
                    }
                });
        }

        if (syncMode == FULL_ASYNC)
            onDone(new GridCacheReturn(cctx, true, true, null, true));
    }

    /**
     * @param req Request.
     * @param e Error.
     */
    void onSendError(GridNearAtomicUpdateRequest req, IgniteCheckedException e) {
        synchronized (mux) {
            GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                req.nodeId(),
                req.futureVersion(),
                cctx.deploymentEnabled());

            res.addFailedKeys(req.keys(), e);

            onResult(req.nodeId(), res, true);
        }
    }

    /**
     * @param topVer Topology version.
     */
    void map(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> topNodes = CU.affinityNodes(cctx, topVer);

        if (F.isEmpty(topNodes)) {
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid)."));

            return;
        }

        Exception err = null;
        GridNearAtomicUpdateRequest singleReq0 = null;
        Map<UUID, GridNearAtomicUpdateRequest> mappings0 = null;

        GridCacheVersion futVer = cctx.versions().next(topVer);

        GridCacheVersion updVer;

        // Assign version on near node in CLOCK ordering mode even if fastMap is false.
        if (cctx.config().getAtomicWriteOrderMode() == CLOCK) {
            updVer = this.updVer;

            if (updVer == null) {
                updVer = cctx.versions().next(topVer);

                if (log.isDebugEnabled())
                    log.debug("Assigned fast-map version for update on near node: " + updVer);
            }
        }
        else
            updVer = null;

        try {
            if (!fastMap)
                singleReq0 = mapSingleUpdate(topVer, futVer, updVer);
            else {
                Map<UUID, GridNearAtomicUpdateRequest> pendingMappings = mapUpdate(topNodes,
                    topVer,
                    futVer,
                    updVer);

                if (pendingMappings.size() == 1)
                    singleReq0 = F.firstValue(pendingMappings);
                else {
                    if (syncMode == PRIMARY_SYNC) {
                        mappings0 = U.newHashMap(pendingMappings.size());

                        for (GridNearAtomicUpdateRequest req : pendingMappings.values()) {
                            if (req.hasPrimary())
                                mappings0.put(req.nodeId(), req);
                        }
                    }
                    else
                        mappings0 = pendingMappings;

                    assert !mappings0.isEmpty() : this;
                }
            }

            synchronized (mux) {
                assert this.futVer == null : this;
                assert this.topVer == AffinityTopologyVersion.ZERO : this;

                this.topVer = topVer;
                this.updVer = updVer;
                this.futVer = futVer;

                resCnt = 0;

                singleReq = singleReq0;
                mappings = mappings0;

                this.remapKey = false;
            }
        }
        catch (Exception e) {
            err = e;
        }

        if (err != null) {
            onDone(err);

            return;
        }

        if (storeFuture()) {
            if (!cctx.mvcc().addAtomicFuture(futVer, this)) {
                assert isDone() : this;

                return;
            }
        }

        // Optimize mapping for single key.
        if (singleReq0 != null)
            mapSingle(singleReq0.nodeId(), singleReq0);
        else {
            assert mappings0 != null;

            doUpdate(mappings0);
        }
    }

    /**
     * @return Future version.
     */
    GridCacheVersion onFutureDone() {
        GridCacheVersion ver0;

        GridFutureAdapter<Void> fut0;

        synchronized (mux) {
            fut0 = topCompleteFut;

            topCompleteFut = null;

            ver0 = futVer;

            futVer = null;
        }

        if (fut0 != null)
            fut0.onDone();

        return ver0;
    }

    /**
     * @param topNodes Cache nodes.
     * @param topVer Topology version.
     * @param futVer Future version.
     * @param updVer Update version.
     * @return Mapping.
     * @throws Exception If failed.
     */
    private Map<UUID, GridNearAtomicUpdateRequest> mapUpdate(Collection<ClusterNode> topNodes,
        AffinityTopologyVersion topVer,
        GridCacheVersion futVer,
        @Nullable GridCacheVersion updVer) throws Exception {
        Map<UUID, GridNearAtomicUpdateRequest> pendingMappings = U.newHashMap(topNodes.size());

        if (key == null)
            throw new NullPointerException("Null key.");

        Object val = this.val;

        if (val == null && op != GridCacheOperation.DELETE)
            throw new NullPointerException("Null value.");

        KeyCacheObject cacheKey = cctx.toCacheKeyObject(key);

        if (op != TRANSFORM)
            val = cctx.toCacheObject(val);

        Collection<ClusterNode> affNodes = mapKey(cacheKey, topVer);

        if (affNodes.isEmpty())
            throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                "(all partition nodes left the grid).");

        int i = 0;

        for (ClusterNode affNode : affNodes) {
            if (affNode == null)
                throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                    "(all partition nodes left the grid).");

            UUID nodeId = affNode.id();

            GridNearAtomicUpdateRequest mapped = pendingMappings.get(nodeId);

            if (mapped == null) {
                mapped = new GridNearAtomicUpdateRequest(
                    cctx.cacheId(),
                    nodeId,
                    futVer,
                    fastMap,
                    updVer,
                    topVer,
                    topLocked,
                    syncMode,
                    op,
                    retval,
                    expiryPlc,
                    invokeArgs,
                    filter,
                    subjId,
                    taskNameHash,
                    skipStore,
                    keepBinary,
                    cctx.kernalContext().clientNode(),
                    cctx.deploymentEnabled(),
                    1);

                pendingMappings.put(nodeId, mapped);
            }

            mapped.addUpdateEntry(cacheKey, val, CU.TTL_NOT_CHANGED, CU.EXPIRE_TIME_CALCULATE, null, i == 0);

            i++;
        }

        return pendingMappings;
    }

    /**
     * @param topVer Topology version.
     * @param futVer Future version.
     * @param updVer Update version.
     * @return Request.
     * @throws Exception If failed.
     */
    private GridNearAtomicUpdateRequest mapSingleUpdate(AffinityTopologyVersion topVer,
        GridCacheVersion futVer,
        @Nullable GridCacheVersion updVer) throws Exception {
        if (key == null)
            throw new NullPointerException("Null key.");

        Object val = this.val;

        if (val == null && op != GridCacheOperation.DELETE)
            throw new NullPointerException("Null value.");

        KeyCacheObject cacheKey = cctx.toCacheKeyObject(key);

        if (op != TRANSFORM)
            val = cctx.toCacheObject(val);

        ClusterNode primary = cctx.affinity().primary(cacheKey, topVer);

        if (primary == null)
            throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid).");

        GridNearAtomicUpdateRequest req = new GridNearAtomicUpdateRequest(
            cctx.cacheId(),
            primary.id(),
            futVer,
            fastMap,
            updVer,
            topVer,
            topLocked,
            syncMode,
            op,
            retval,
            expiryPlc,
            invokeArgs,
            filter,
            subjId,
            taskNameHash,
            skipStore,
            keepBinary,
            cctx.kernalContext().clientNode(),
            cctx.deploymentEnabled(),
            1);

        req.addUpdateEntry(cacheKey,
            val,
            CU.TTL_NOT_CHANGED,
            CU.EXPIRE_TIME_CALCULATE,
            null,
            true);

        return req;
    }

    /** {@inheritDoc} */
    public String toString() {
        synchronized (mux) {
            return S.toString(GridNearAtomicSingleUpdateFuture.class, this, super.toString());
        }
    }
}
