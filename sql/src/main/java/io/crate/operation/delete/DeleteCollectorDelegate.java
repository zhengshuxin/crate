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
import io.crate.Constants;
import io.crate.operation.collect.DelegatingLuceneCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.IdCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.List;

public class DeleteCollectorDelegate implements DelegatingLuceneCollector.LuceneCollectorDelegate {

    private static final ESLogger LOGGER = Loggers.getLogger(DeleteCollectorDelegate.class);
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final ActionListener<Long> shardResultListener;
    private final TransportDeleteAction transportDeleteAction;
    private final ShardId shardId;

    private long deleteCount = 0L;

    public DeleteCollectorDelegate(ShardId shardId,
                                   ActionListener<Long> shardResultListener,
                                   TransportDeleteAction transportDeleteAction,
                                   Optional<LuceneCollectorExpression<?>> routingColumnExpression) {
        this.shardResultListener = shardResultListener;
        this.transportDeleteAction = transportDeleteAction;
        this.shardId = shardId;

        if (routingColumnExpression.isPresent()) {
            this.collectorExpressions = ImmutableList.of(new IdCollectorExpression(), routingColumnExpression.get());
        } else {
            this.collectorExpressions = ImmutableList.<LuceneCollectorExpression<?>>of(new IdCollectorExpression());
        }
    }

    @Override
    public void onNextReader(AtomicReaderContext context) {
        for (LuceneCollectorExpression<?> expr : collectorExpressions) {
            expr.setNextReader(context);
        }
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
            // TODO: use bulkshardprocessor
            DeleteResponse response = transportDeleteAction.execute(deleteRequest(id, routing)).actionGet();
            if (response.isFound()) {
                deleteCount++;
            }
        } catch (Throwable t) {
            LOGGER.debug("error deleting document with docId {} on shard {}", t, doc, shardId);
            throw t;
        }
        return true;
    }

    private DeleteRequest deleteRequest(String id, @Nullable String routing) {
        return new DeleteRequest()
                .id(id)
                .index(shardId.getIndex())
                .routing(routing)
                .refresh(false)
                .type(Constants.DEFAULT_MAPPING_TYPE);
    }

    @Override
    public void onError(Throwable t) {
        shardResultListener.onFailure(t);
    }

    @Override
    public void onCollectStart(CollectorContext collectorContext) {
        for (LuceneCollectorExpression<?> expr : collectorExpressions) {
            expr.startCollect(collectorContext);
        }
    }

    @Override
    public void onCollectFinished() {
        shardResultListener.onResponse(deleteCount);
    }
}
