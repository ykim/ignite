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
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationFilter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Atomic near single update request.
 */
public class GridNearAtomicSingleUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag: fast map. */
    private static final byte FLAG_FAST_MAP = 0x01;

    /** Flag: topology locked. */
    private static final byte FLAG_TOP_LOCKED = 0x02;

    /** Flag: return value. */
    private static final byte FLAG_RET_VAL = 0x04;

    /** Flag: skip store. */
    private static final byte FLAG_SKIP_STORE = 0x08;

    /** Flag: client request. */
    private static final byte FLAG_CLIENT_REQ = 0x10;

    /** Flag: keep binary. */
    private static final byte FLAG_KEEP_BINARY = 0x20;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    private GridCacheOperation op;

    /** Key to update. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** Value to update. */
    private CacheObject val;

    /** Entry processor. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> entryProcessor;

    /** Entry processor bytes. */
    private byte[] entryProcessorBytes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Expiry policy. */
    @GridDirectTransient
    private ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    private byte[] expiryPlcBytes;

    /** Filter. */
    private CacheOperationFilter filter;

    /** Filter value (expected value). */
    private CacheObject filterVal;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Fast map flag. */
    private boolean fastMap;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    private boolean topLocked;

    /** Return value flag. */
    private boolean retval;

    /** Skip write-through to a persistent storage. */
    private boolean skipStore;

    /** */
    private boolean clientReq;

    /** Keep binary flag. */
    private boolean keepBinary;

    /** */
    @GridDirectTransient
    private GridNearAtomicAbstractUpdateResponse res;

    /** Target node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicSingleUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param fastMap Fast map scheme flag.
     * @param updateVer Update version set if fast map is performed.
     * @param topVer Topology version.
     * @param topLocked Topology locked flag.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param expiryPlc Expiry policy.
     * @param invokeArgs Optional arguments for entry processor.
     * @param filter Optional filter for atomic check.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     * @param key Key.
     * @param val Value.
     */
    @SuppressWarnings("unchecked")
    public GridNearAtomicSingleUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean topLocked,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Object[] invokeArgs,
        CacheOperationFilter filter,
        @Nullable CacheObject filterVal,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        boolean clientReq,
        boolean addDepInfo,
        KeyCacheObject key,
        Object val
    ) {
        assert futVer != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.fastMap = fastMap;
        this.updateVer = updateVer;

        this.topVer = topVer;
        this.topLocked = topLocked;
        this.syncMode = syncMode;
        this.op = op;
        this.retval = retval;
        this.expiryPlc = expiryPlc;
        this.invokeArgs = invokeArgs;
        this.filter = filter;
        this.filterVal = filterVal;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.clientReq = clientReq;
        this.addDepInfo = addDepInfo;

        EntryProcessor<Object, Object, Object> entryProcessor = null;

        if (op == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProcessor = (EntryProcessor<Object, Object, Object>) val;
        }

        assert val != null || op == DELETE;

        this.key = key;

        if (entryProcessor != null) {
            this.entryProcessor = entryProcessor;
        }
        else if (val != null) {
            assert val instanceof CacheObject : val;

            this.val = (CacheObject)val;
        }
    }

    /** {@inheritDoc} */
    public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public boolean fastMap() {
        return fastMap;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion updateVersion() {
        return updateVer;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean topologyLocked() {
        return topLocked;
    }

    /** {@inheritDoc} */
    @Override public boolean clientRequest() {
        return clientReq;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /** {@inheritDoc} */
    @Override public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public boolean returnValue() {
        return retval;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheEntryPredicate[] filter() {
        return CU.filterArray(filter.createPredicate(filterVal));
    }

    /** {@inheritDoc} */
    @Override public boolean hasFilter() {
        return filter != null;
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /** {@inheritDoc} */
    @Override public List<KeyCacheObject> keys() {
        return Collections.singletonList(key);
    }

    /** {@inheritDoc} */
    @Override public int keysCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key(int idx) {
        assert idx == 0;

        return key;
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return op == TRANSFORM ? Collections.singletonList(entryProcessor) : Collections.singletonList(val);
    }

    /** {@inheritDoc} */
    @Override public GridCacheOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override public CacheObject value(int idx) {
        assert op == UPDATE : op;
        assert idx == 0;

        return val;
    }

    /** {@inheritDoc} */
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert op == TRANSFORM : op;
        assert idx == 0;

        return entryProcessor;
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        assert idx == 0;

        return val;
    }

    /** {@inheritDoc} */
    @Override @Nullable public List<GridCacheVersion> conflictVersions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override public boolean hasPrimary() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean onResponse(GridNearAtomicAbstractUpdateResponse res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridNearAtomicAbstractUpdateResponse response() {
        return res;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);

        if (expiryPlc != null && expiryPlcBytes == null)
            expiryPlcBytes = CU.marshal(cctx, new IgniteExternalizableExpiryPolicy(expiryPlc));

        if (op == TRANSFORM) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (entryProcessorBytes == null)
                entryProcessorBytes = marshal(entryProcessor, cctx);

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
        }
        else
            prepareMarshalCacheObject(val, cctx);

        prepareMarshalCacheObject(filterVal, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObject(key, cctx, ldr);

        if (op == TRANSFORM) {
            if (entryProcessor == null)
                entryProcessor = unmarshal(entryProcessorBytes, ctx, ldr);

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }
        else
            finishUnmarshalCacheObject(val, cctx, ldr);

        finishUnmarshalCacheObject(filterVal, cctx, ldr);

        if (expiryPlcBytes != null && expiryPlc == null)
            expiryPlc = ctx.marshaller().unmarshal(expiryPlcBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }

    /** {@inheritDoc} */
    @Override public boolean isSingle() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeByte("flags", getFlags()))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("entryProcessorBytes", entryProcessorBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("expiryPlcBytes", expiryPlcBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte("filter", (byte)filter.ordinal()))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("filterVal", filterVal))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMessage("updateVer", updateVer))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMessage("val", val))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                byte flags;

                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                setFlags(flags);

                reader.incrementState();

            case 4:
                entryProcessorBytes = reader.readByteArray("entryProcessorBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                expiryPlcBytes = reader.readByteArray("expiryPlcBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                byte filterOrd;

                filterOrd = reader.readByte("filter");

                if (!reader.isLastRead())
                    return false;

                filter = CacheOperationFilter.fromOrdinal(filterOrd);

                reader.incrementState();

            case 7:
                filterVal = reader.readMessage("filterVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 12:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 14:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicUpdateRequest.class);
    }

    /**
     * @return Flags.
     */
    private byte getFlags() {
        byte flags = 0;

        if (fastMap)
            flags |= FLAG_FAST_MAP;

        if (topLocked)
            flags |= FLAG_TOP_LOCKED;

        if (retval)
            flags |= FLAG_RET_VAL;

        if (skipStore)
            flags |= FLAG_SKIP_STORE;

        if (clientReq)
            flags |= FLAG_CLIENT_REQ;

        if (keepBinary)
            flags |= FLAG_KEEP_BINARY;

        return flags;
    }

    /**
     * @param flags Flags.
     */
    private void setFlags(byte flags) {
        fastMap = hasFlag(flags, FLAG_FAST_MAP);
        topLocked = hasFlag(flags, FLAG_TOP_LOCKED);
        retval = hasFlag(flags, FLAG_RET_VAL);
        skipStore = hasFlag(flags, FLAG_SKIP_STORE);
        clientReq = hasFlag(flags, FLAG_CLIENT_REQ);
        keepBinary = hasFlag(flags, FLAG_KEEP_BINARY);
    }

    /**
     * Check if certain flag is present.
     *
     * @param flags Flags.
     * @param flag Flag.
     * @return {@code True} if flag is present.
     */
    private static boolean hasFlag(byte flags, byte flag) {
        return (flags & flag) == flag;
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKeys) {
        val = null;
        invokeArgs = null;
        invokeArgsBytes = null;

        if (clearKeys)
            key = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -23;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 18;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateRequest.class, this, "parent", super.toString());
    }
}
