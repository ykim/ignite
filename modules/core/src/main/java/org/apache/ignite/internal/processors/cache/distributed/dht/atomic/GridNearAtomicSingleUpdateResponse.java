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
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

/**
 * DHT atomic cache near update response.
 */
public class GridNearAtomicSingleUpdateResponse extends GridNearAtomicAbstractUpdateResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID this reply should be sent to. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Update error. */
    @GridDirectTransient
    private volatile IgniteCheckedException err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Return value. */
    @GridToStringInclude
    private GridCacheReturn ret;

    /** Failed keys. */
    @GridToStringInclude
    private volatile boolean failedKey;

    /** Keys that should be remapped. */
    @GridToStringInclude
    private boolean remapKey;

    /** Near extras. */
    private NearExtras nearExtras;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicSingleUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID this reply should be sent to.
     * @param futVer Future version.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearAtomicSingleUpdateResponse(int cacheId, UUID nodeId, GridCacheVersion futVer, boolean addDepInfo) {
        assert futVer != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.addDepInfo = addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public void error(IgniteCheckedException err){
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public int failedKeysCount() {
        return failedKey ? 1 : 0;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject failedKey(GridNearAtomicAbstractUpdateRequest req, int idx) {
        assert idx == 0;
        assert failedKey;

        return req.key(0);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn returnValue() {
        return ret;
    }

    /**
     * @param ret Return value.
     */
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void returnValue(GridCacheReturn ret) {
        this.ret = ret;
    }

    /** {@inheritDoc} */
    @Override public void remapKeys(GridNearAtomicAbstractUpdateRequest req) {
        assert req instanceof GridNearAtomicSingleUpdateRequest;

        remapKey = true;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject remapKey(GridNearAtomicAbstractUpdateRequest req, int idx) {
        assert idx == 0;

        return req.key(0);
    }

    /** {@inheritDoc} */
    @Override public int remapKeysCount() {
        return remapKey ? 1 : 0;
    }

    /** {@inheritDoc} */
    @Override public void addNearValue(int keyIdx, @Nullable CacheObject val, long ttl, long expireTime) {
        assert keyIdx == 0;

        initializeNearExtras();

        addNearTtl(keyIdx, ttl, expireTime);

        nearExtras.valsIdx = true;
        nearExtras.val = val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void addNearTtl(int keyIdx, long ttl, long expireTime) {
        assert keyIdx == 0;

        initializeNearExtras();

        nearExtras.ttl = ttl;
        nearExtras.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public long nearExpireTime(int idx) {
        assert idx == 0;

        return nearExtras != null ? nearExtras.expireTime : -1;
    }

    /** {@inheritDoc} */
    @Override public long nearTtl(int idx) {
        assert idx == 0;

        return nearExtras != null ? nearExtras.ttl : -1;
    }

    /** {@inheritDoc} */
    @Override public void nearVersion(GridCacheVersion nearVer) {
        initializeNearExtras();

        nearExtras.ver = nearVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion nearVersion() {
        return nearExtras != null ? nearExtras.ver : null;
    }

    /** {@inheritDoc} */
    @Override public void addSkippedIndex(int keyIdx) {
        assert keyIdx == 0;

        initializeNearExtras();

        nearExtras.skipIdx = true;

        addNearTtl(keyIdx, -1L, -1L);
    }

    /** {@inheritDoc} */
    @Override public boolean isNearSkippedIndex(int idx) {
        assert idx == 0;

        return nearExtras != null && nearExtras.skipIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean isNearValueIndex(int idx) {
        assert idx == 0;

        return nearExtras != null && nearExtras.valsIdx;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject nearValue(int idx) {
        assert idx == 0;

        return nearExtras != null ? nearExtras.val : null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void addFailedKey(KeyCacheObject key, Throwable e) {
        failedKey = true;

        setFailedKeysError(e);
    }

    /** {@inheritDoc} */
    @Override public synchronized void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e) {
        if (keys != null) {
            assert keys.size() == 1;

            failedKey = true;
        }

        setFailedKeysError(e);
    }

    /** {@inheritDoc} */
    @Override public synchronized void addFailedKeys(GridNearAtomicAbstractUpdateRequest req, Throwable e) {
        assert req instanceof GridNearAtomicSingleUpdateRequest;

        failedKey = true;

        setFailedKeysError(e);
    }

    /**
     * Set failed keys error.
     *
     * @param e Error.
     */
    private void setFailedKeysError(Throwable e) {
        if (err == null)
            err = new IgniteCheckedException("Failed to update keys on primary node.");

        err.addSuppressed(e);
    }

    /** {@inheritDoc} */
    @Override public boolean isSingle() {
        return true;
    }

    /**
     * Initialize near extras.
     */
    private void initializeNearExtras() {
        if (nearExtras == null)
            nearExtras = new NearExtras();
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null && errBytes == null)
            errBytes = ctx.marshaller().marshal(err);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (nearExtras != null)
            prepareMarshalCacheObject(nearExtras.val, cctx);

        if (ret != null)
            ret.prepareMarshal(cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null && err == null)
            err = ctx.marshaller().unmarshal(errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (nearExtras != null)
            finishUnmarshalCacheObject(nearExtras.val, cctx, ldr);

        if (ret != null)
            ret.finishUnmarshal(cctx, ldr);
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
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeBoolean("failedKey", failedKey))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeBoolean("remapKey", remapKey))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("ret", ret))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeBoolean("hasNearExtras", nearExtras != null))
                    return false;

                writer.incrementState();

                if (nearExtras == null)
                    return true;

            case 9:
                if (!writer.writeLong("nearExpireTime", nearExtras.expireTime))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("nearSkipIdx", nearExtras.skipIdx))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeLong("nearTtl", nearExtras.ttl))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMessage("nearVal", nearExtras.val))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeBoolean("nearValsIdx", nearExtras.valsIdx))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("nearVer", nearExtras.ver))
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
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                failedKey = reader.readBoolean("failedKey");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                remapKey = reader.readBoolean("remapKey");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ret = reader.readMessage("ret");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                boolean hasNearExtras = reader.readBoolean("hasNearExtras");

                if (!reader.isLastRead())
                    return false;

                if (hasNearExtras)
                    nearExtras = new NearExtras();

                reader.incrementState();

                if (!hasNearExtras)
                    return reader.afterMessageRead(GridNearAtomicSingleUpdateResponse.class);

            case 9:
                nearExtras.expireTime = reader.readLong("nearExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                nearExtras.skipIdx = reader.readBoolean("nearSkipIdx");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                nearExtras.ttl = reader.readLong("nearTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                nearExtras.val = reader.readMessage("nearVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                nearExtras.valsIdx = reader.readBoolean("nearValsIdx");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                nearExtras.ver = reader.readMessage("nearVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridNearAtomicSingleUpdateResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -24;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return nearExtras != null ? (byte)15 : (byte)9;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateResponse.class, this, "parent");
    }

    /**
     * Extra data for near nodes.
     */
    private static class NearExtras {
        /** Indexes of keys for which values were generated on primary node (used if originating node has near cache). */
        private boolean valsIdx;

        /** Indexes of keys for which update was skipped (used if originating node has near cache). */
        private boolean skipIdx;

        /** Values generated on primary node which should be put to originating node's near cache. */
        @GridToStringInclude
        private CacheObject val;

        /** Version generated on primary node to be used for originating node's near cache update. */
        private GridCacheVersion ver;

        /** Near TTLs. */
        private long ttl = -1L;

        /** Near expire times. */
        private long expireTime = -1L;
    }
}
