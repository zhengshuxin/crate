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

package io.crate.operation.delete;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.operation.collect.DelegatingLuceneCollector;
import io.crate.operation.collect.LuceneDocCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.IdCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;

public class DeleteCollectorDelegate implements DelegatingLuceneCollector.LuceneCollectorDelegate {

    private static final ESLogger LOGGER = Loggers.getLogger(DeleteCollectorDelegate.class);
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final ActionListener<Long> shardResultListener;
    private final SymbolBasedBulkShardProcessor<BulkDeleteRequest, BulkDeleteResponse> bulkShardProcessor;
    private final ShardId shardId;
    private LuceneDocCollector.CollectorFieldsVisitor collectorFieldsVisitor;
    private AtomicReader currentReader;

    public DeleteCollectorDelegate(ShardId shardId,
                                   ActionListener<Long> shardResultListener,
                                   SymbolBasedBulkShardProcessor<BulkDeleteRequest, BulkDeleteResponse> bulkShardProcessor,
                                   Optional<LuceneCollectorExpression<?>> routingColumnExpression) {
        this.shardResultListener = shardResultListener;
        this.bulkShardProcessor = bulkShardProcessor;
        this.shardId = shardId;
        IdCollectorExpression idCollectorExpression = new IdCollectorExpression();
        if (routingColumnExpression.isPresent()) {

            this.collectorExpressions = ImmutableList.of(idCollectorExpression, routingColumnExpression.get());
        } else {
            this.collectorExpressions = ImmutableList.<LuceneCollectorExpression<?>>of(idCollectorExpression);
        }
    }

    @Override
    public void onNextReader(AtomicReaderContext context) {
        for (LuceneCollectorExpression<?> expr : collectorExpressions) {
            expr.setNextReader(context);
        }
        currentReader = context.reader();
    }

    @Override
    public void onScorer(Scorer scorer) {
        for (LuceneCollectorExpression<?> expr : collectorExpressions) {
            expr.setScorer(scorer);
        }
    }

    @Override
    public boolean onDocId(int doc) {
        for (LuceneCollectorExpression<?> expr : collectorExpressions) {
            expr.setNextDocId(doc);
        }
        String id = BytesRefs.toString(collectorExpressions.get(0).value());
        String routing = null;

        if (collectorExpressions.size() == 2) {
            routing = BytesRefs.toString(collectorExpressions.get(1).value());
        }
        try {
            bulkShardProcessor.add(shardId.getIndex(), id, null, null, routing, null);
        } catch (Throwable t) {
            LOGGER.debug("error deleting document with docId {} on shard {}", t, doc, shardId);
            throw t;
        }
        return true;
    }

    @Override
    public void onError(Throwable t) {
        bulkShardProcessor.close();
        shardResultListener.onFailure(t);
    }

    @Override
    public void onCollectStart(CollectorContext collectorContext) {
        for (LuceneCollectorExpression<?> expr : collectorExpressions) {
            expr.startCollect(collectorContext);
        }
        this.collectorFieldsVisitor = collectorContext.visitor();
    }

    @Override
    public void onCollectFinished() {
        bulkShardProcessor.close();
        Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
            @Override
            public void onSuccess(@Nullable BitSet result) {
                assert result != null : "[delete] returned bitset is null";
                shardResultListener.onResponse((long)result.cardinality());
            }

            @Override
            public void onFailure(Throwable t) {
                shardResultListener.onFailure(t);
            }
        });
    }
}
