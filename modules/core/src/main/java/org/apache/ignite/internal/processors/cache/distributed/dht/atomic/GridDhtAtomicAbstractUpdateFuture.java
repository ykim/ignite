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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Abstract DHT atomic update future.
 */
public abstract class GridDhtAtomicAbstractUpdateFuture extends GridFutureAdapter<Void>
    implements GridCacheAtomicFuture<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    protected static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Future version. */
    protected final GridCacheVersion futVer;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Update request. */
    protected final GridNearAtomicAbstractUpdateRequest updateReq;

    /** Update response. */
    protected final GridNearAtomicAbstractUpdateResponse updateRes;

    /** Completion callback. */
    @GridToStringExclude
    protected final CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicAbstractUpdateResponse> completionCb;

    /** Write version. */
    protected final GridCacheVersion writeVer;

    /** */
    protected final boolean waitForExchange;

    /** Force transform backup flag. */
    protected boolean forceTransformBackups;

    /** Response count. */
    protected volatile int resCnt;

    /** Node for single mapping. */
    private UUID mapSingleNode;

    /** Request for single mapping. */
    private GridDhtAtomicUpdateRequest mapSingleReq;

    /** Mappings. */
    @GridToStringInclude
    private Map<UUID, GridDhtAtomicUpdateRequest> mappings;

    /** Continuous query closure. */
    private CI1<Boolean> cntQryClo;

    /** Continuous query closures. */
    private List<CI1<Boolean>> cntQryClos;

    /** Entries with readers. */
    private Map<KeyCacheObject, GridDhtCacheEntry> nearReadersEntries;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param updateReq Near request.
     * @param updateRes Near response.
     * @param completionCb Completion callback.
     * @param writeVer Write version.
     */
    protected GridDhtAtomicAbstractUpdateFuture(
        GridCacheContext cctx,
        GridNearAtomicAbstractUpdateRequest updateReq,
        GridNearAtomicAbstractUpdateResponse updateRes,
        CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicAbstractUpdateResponse> completionCb,
        GridCacheVersion writeVer) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtAtomicAbstractUpdateFuture.class);

        futVer = cctx.versions().next(updateReq.topologyVersion());

        this.cctx = cctx;
        this.updateReq = updateReq;
        this.updateRes = updateRes;
        this.completionCb = completionCb;
        this.writeVer = writeVer;

        waitForExchange = !(updateReq.topologyLocked() || (updateReq.fastMap() && !updateReq.clientRequest()));
    }

    /**
     * @param entry Entry to map.
     * @param val Value to write.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} sends previous value to backups.
     * @param prevVal Previous value.
     * @param updateCntr Partition update counter.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void addWriteEntry(GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr) {
        List<ClusterNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), updateReq.topologyVersion());

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        addKey(entry.key());

        for (int i = 0; i < dhtNodes.size(); i++) {
            ClusterNode node = dhtNodes.get(i);

            UUID nodeId = node.id();

            if (!nodeId.equals(cctx.localNodeId())) {
                GridDhtAtomicUpdateRequest req = mapping(nodeId);

                if (req == null) {
                    req = new GridDhtAtomicUpdateRequest(
                        cctx.cacheId(),
                        nodeId,
                        futVer,
                        writeVer,
                        updateReq.writeSynchronizationMode(),
                        updateReq.topologyVersion(),
                        forceTransformBackups,
                        this.updateReq.subjectId(),
                        this.updateReq.taskNameHash(),
                        forceTransformBackups ? this.updateReq.invokeArguments() : null,
                        cctx.deploymentEnabled(),
                        this.updateReq.keepBinary());

                    mapping(nodeId, req);
                }

                req.addWriteValue(entry.key(),
                    val,
                    entryProcessor,
                    ttl,
                    conflictExpireTime,
                    conflictVer,
                    addPrevVal,
                    entry.partition(),
                    prevVal,
                    updateCntr);
            }
        }
    }

    /**
     * @param readers Entry readers.
     * @param entry Entry.
     * @param val Value.
     * @param entryProcessor Entry processor..
     * @param ttl TTL for near cache update (optional).
     * @param expireTime Expire time for near cache update (optional).
     */
    public void addNearWriteEntries(Iterable<UUID> readers,
        GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        addKey(entry.key());

        for (UUID nodeId : readers) {
            GridDhtAtomicUpdateRequest req = mapping(nodeId);

            if (req == null) {
                ClusterNode node = cctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                req = new GridDhtAtomicUpdateRequest(
                    cctx.cacheId(),
                    nodeId,
                    futVer,
                    writeVer,
                    updateReq.writeSynchronizationMode(),
                    updateReq.topologyVersion(),
                    forceTransformBackups,
                    updateReq.subjectId(),
                    updateReq.taskNameHash(),
                    forceTransformBackups ? updateReq.invokeArguments() : null,
                    cctx.deploymentEnabled(),
                    updateReq.keepBinary());

                mapping(nodeId, req);
            }

            if (nearReadersEntries == null)
                nearReadersEntries = new HashMap<>();

            nearReadersEntries.put(entry.key(), entry);

            req.addNearWriteValue(entry.key(),
                val,
                entryProcessor,
                ttl,
                expireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        if (waitForExchange && updateReq.topologyVersion().compareTo(topVer) < 0)
            return this;

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Processing node leave event [fut=" + this + ", nodeId=" + nodeId + ']');

        return registerResponse(nodeId);
    }

    /**
     * Callback for backup update response.
     *
     * @param nodeId Backup node ID.
     * @param updateRes Update response.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void onResult(UUID nodeId, GridDhtAtomicUpdateResponse updateRes) {
        if (log.isDebugEnabled())
            log.debug("Received DHT atomic update future result [nodeId=" + nodeId + ", updateRes=" + updateRes + ']');

        if (updateRes.error() != null) {
            List<KeyCacheObject> failed = new ArrayList<>(updateRes.failedCount());

            for (int i = 0; i < updateRes.failedCount(); i++)
                failed.add(updateRes.failed(i));

            this.updateRes.addFailedKeys(failed, updateRes.error());
        }

        for (int i = 0; i < updateRes.nearEvictedCount(); i++) {
            KeyCacheObject key = updateRes.nearEvicted(i);

            assert nearReadersEntries != null;

            GridDhtCacheEntry entry = nearReadersEntries.get(key);

            try {
                entry.removeReader(nodeId, updateRes.messageId());
            }
            catch (GridCacheEntryRemovedException e) {
                if (log.isDebugEnabled())
                    log.debug("Entry with evicted reader was removed [entry=" + entry + ", err=" + e + ']');
            }
        }

        registerResponse(nodeId);
    }

    /**
     * Deferred update response.
     *
     * @param nodeId Backup node ID.
     */
    public void onResult(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Received deferred DHT atomic update future result [nodeId=" + nodeId + ']');

        registerResponse(nodeId);
    }

    /**
     * Sends requests to remote nodes.
     */
    public void map() {
        if (mappingsCount() > 0)
            mapAll();
        else
            onDone();

        // Send response right away if no ACKs from backup is required.
        // Backups will send ACKs anyway, future will be completed after all backups have replied.
        if (updateReq.writeSynchronizationMode() != FULL_SYNC)
            completionCb.apply(updateReq, updateRes);
    }

    /**
     * Internal mapping routine.
     */
    private void mapAll() {
        if (mapSingleReq != null)
            sendRequest(mapSingleReq);
        else {
            for (GridDhtAtomicUpdateRequest req : mappings.values())
                sendRequest(req);
        }
    }

    /**
     * @param clo Continuous query closure.
     */
    public void addContinuousQueryClosure(CI1<Boolean> clo){
        assert !isDone() : this;

        if (cntQryClo != null) {
            cntQryClos = new ArrayList<>(2);

            cntQryClos.add(cntQryClo);

            cntQryClo = null;
        }

        if (cntQryClos != null)
            cntQryClos.add(clo);
        else
            cntQryClo = clo;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeAtomicFuture(version());

            boolean suc = err == null;

            if (!suc)
                markAllKeysFailed(err);

            if (cntQryClo != null)
                cntQryClo.apply(suc);
            else if (cntQryClos != null) {
                for (int i = 0; i < cntQryClos.size(); i++)
                    cntQryClos.get(i).apply(suc);
            }

            if (updateReq.writeSynchronizationMode() == FULL_SYNC)
                completionCb.apply(updateReq, updateRes);

            return true;
        }

        return false;
    }

    /**
     * Add key.
     *
     * @param key Key.
     */
    protected abstract void addKey(KeyCacheObject key);

    /**
     * Mark all request keys as failed.
     *
     * @param err Error.
     */
    protected abstract void markAllKeysFailed(@Nullable Throwable err);

    /**
     * Add mapping.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void mapping(UUID nodeId, GridDhtAtomicUpdateRequest req) {
        if (mapSingleNode != null) {
            mappings = U.newHashMap(updateReq.keysCount());

            mappings.put(mapSingleNode, mapSingleReq);

            mapSingleNode = null;
            mapSingleReq = null;
        }

        if (mappings != null)
            mappings.put(nodeId, req);
        else {
            mapSingleNode = nodeId;
            mapSingleReq = req;
        }
    }

    /**
     * Get mapping for the given node ID.
     *
     * @param nodeId Node ID.
     * @return Mapping (if any).
     */
    @Nullable protected GridDhtAtomicUpdateRequest mapping(UUID nodeId) {
        if (mapSingleNode != null) {
            if (F.eq(mapSingleNode, nodeId))
                return mapSingleReq;
        }
        else if (mappings != null)
            return mappings.get(nodeId);

        return null;
    }

    /**
     * @return Mappings number.
     */
    protected int mappingsCount() {
        return mapSingleNode != null ? 1 : mappings != null ? mappings.size() : 0;
    }

    /**
     * Send DHT request.
     *
     * @param req Request.
     */
    protected void sendRequest(GridDhtAtomicUpdateRequest req) {
        try {
            if (log.isDebugEnabled())
                log.debug("Sending DHT atomic update request [nodeId=" + req.nodeId() + ", req=" + req + ']');

            cctx.io().send(req.nodeId(), req, cctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException ignored) {
            U.warn(log, "Failed to send update request to backup node because it left grid: " +
                req.nodeId());

            registerResponse(req.nodeId());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send update request to backup node (did node leave the grid?): "
                + req.nodeId(), e);

            registerResponse(req.nodeId());
        }
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if request found.
     */
    private boolean registerResponse(UUID nodeId) {
        int resCnt0;

        GridDhtAtomicUpdateRequest req = mapping(nodeId);

        if (req != null) {
            synchronized (this) {
                if (req.onResponse()) {
                    resCnt0 = resCnt;

                    resCnt0 += 1;

                    resCnt = resCnt0;
                }
                else
                    return false;
            }

            if (resCnt0 == mappingsCount())
                onDone();

            return true;
        }

        return false;
    }
}
