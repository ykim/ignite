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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Key for query details metrics to store in system cache.
 */
public class GridCacheQueryDetailsMetricsKey extends GridCacheUtilityKey<GridCacheQueryDetailsMetricsKey> implements Externalizable {
    /** Query type. */
    private GridCacheQueryType qryType;

    /** Query text descriptor: SQL, cache name, search text, ... */
    private String qry;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueryDetailsMetricsKey() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param qryType Query type.
     * @param qry Query text descriptor.
     */
    public GridCacheQueryDetailsMetricsKey(GridCacheQueryType qryType, String qry) {
        this.qryType = qryType;
        this.qry = qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Query text descriptor: SQL, cache name, search text, ...
     */
    public String query() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, qryType);
        U.writeString(out, qry);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qryType = GridCacheQueryType.fromOrdinal(in.readByte());
        qry = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override protected boolean equalsx(GridCacheQueryDetailsMetricsKey that) {
        return qryType == that.qryType && qry.equals(that.qry);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = qryType.hashCode();

        res = 31 * res + qry.hashCode();

        return res;
    }
}
