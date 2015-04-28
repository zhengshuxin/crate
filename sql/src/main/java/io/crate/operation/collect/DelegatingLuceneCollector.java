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

package io.crate.operation.collect;

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

public class DelegatingLuceneCollector extends Collector implements CrateLuceneCollector {

    protected final JobCollectContext jobCollectContext;
    protected final CrateSearchContext searchContext;
    protected final int jobSearchContextId;
    protected final boolean keepContextForFetcher;

    private final LuceneCollectorDelegate collectorHandle;

    protected AtomicReader currentReader;
    protected RamAccountingContext ramAccountingContext;
    protected Scorer scorer;
    private boolean producedRows = false;

    public DelegatingLuceneCollector(JobCollectContext jobCollectContext,
                                     CrateSearchContext searchContext,
                                     int jobSearchContextId,
                                     boolean keepContextForFetcher,
                                     LuceneCollectorDelegate collectorHandle) {
        this.jobCollectContext = jobCollectContext;
        this.searchContext = searchContext;
        this.jobSearchContextId = jobSearchContextId;
        this.keepContextForFetcher = keepContextForFetcher;
        this.collectorHandle = collectorHandle;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
        collectorHandle.onScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
        // validate minimum score
        if (searchContext.minimumScore() != null
                && scorer.score() < searchContext.minimumScore()) {
            return;
        }
        producedRows = true;
        if (!collectorHandle.onDocId(doc)) {
            throw new CollectionAbortedException();
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.currentReader = context.reader();
        collectorHandle.onNextReader(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    public CrateSearchContext searchContext() {
        return searchContext;
    }

    @Override
    public void doCollect(RamAccountingContext ramAccountingContext) throws Exception {
        boolean failed = false;
        this.ramAccountingContext = ramAccountingContext;
        // start collect
        CollectorContext collectorContext = new CollectorContext()
                .searchContext(searchContext)
                .jobSearchContextId(jobSearchContextId);
        collectorHandle.onCollectStart(collectorContext);
        Query query = searchContext.query();
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        jobCollectContext.acquireContext(searchContext);
        try {
            searchContext.searcher().search(query, this);
            collectorHandle.onCollectFinished();
        } catch (CollectionAbortedException e) {
            collectorHandle.onCollectFinished();
        } catch (Throwable t) {
            failed = true;
            collectorHandle.onError(t);
            throw t;
        } finally {
            jobCollectContext.releaseContext(searchContext);
            if (!keepContextForFetcher || !producedRows || failed) {
                jobCollectContext.closeContext(jobSearchContextId);
            }
        }
    }

    public interface LuceneCollectorDelegate {

        void onNextReader(AtomicReaderContext context);

        void onScorer(Scorer scorer);

        boolean onDocId(int docId);

        void onError(Throwable t);

        void onCollectStart(CollectorContext collectorContext);
        void onCollectFinished();
    }
}
