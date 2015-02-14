/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze.where;

import com.google.common.base.Optional;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.ValueSymbolVisitor;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DocKeys implements Iterable<DocKeys.DocKey> {

    private final int width;
    private int clusteredByIdx;
    private final boolean withVersions;
    private final List<List<Symbol>> docKeys;

    public class DocKey {

        private final List<Symbol> key;

        public DocKey(int pos) {
            key = docKeys.get(pos);
        }

        @Nullable
        public Optional<Long> version() {
            if (withVersions && key.get(width) != null) {
                return Optional.of(((Literal<Long>) key.get(width)).value());
            }
            return Optional.absent();
        }

        public String routing() {
            return ValueSymbolVisitor.STRING.process(key.get(clusteredByIdx));
        }

        public String id() {
            if (width == 1) {
                return ValueSymbolVisitor.STRING.process(key.get(0));
            } else {
                BytesStreamOutput out = new BytesStreamOutput(width * 16);
                try {
                    out.writeVInt(width);
                    for (int i = 0; i < width; i++) {
                        out.writeBytesRef(ValueSymbolVisitor.BYTES_REF.process(key.get(i)));
                    }
                    out.close();
                } catch (IOException e) {
                    //
                }
                return Base64.encodeBytes(out.bytes().toBytes());
            }
        }

        public List<Symbol> values() {
            return key;
        }
    }

    public DocKeys(List<List<Symbol>> docKeys, boolean withVersions, int clusteredByIdx) {
        assert ((docKeys != null) && (!docKeys.isEmpty()));
        if (withVersions) {
            this.width = docKeys.get(0).size() - 1;
        } else {
            this.width = docKeys.get(0).size();
        }
        this.withVersions = withVersions;
        this.docKeys = docKeys;
        this.clusteredByIdx = clusteredByIdx;
    }

    public boolean withVersions() {
        return withVersions;
    }

    public DocKey getOnlyKey() {
        Preconditions.checkState(width == 1);
        return new DocKey(0);
    }

    public int size() {
        return docKeys.size();
    }

    @Override
    public Iterator<DocKey> iterator() {
        return new Iterator<DocKey>() {
            int i = 0;
            @Override
            public boolean hasNext() {
                return i<docKeys.size();
            }

            @Override
            public DocKey next() {
                return new DocKey(i++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
