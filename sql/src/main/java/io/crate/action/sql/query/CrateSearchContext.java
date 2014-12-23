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

package io.crate.action.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CrateSearchContext extends SearchContext {

    private final long id;

    private final SearchShardTarget shardTarget;
    private final Counter timeEstimateCounter;

    private final Engine.Searcher engineSearcher;

    private final ScriptService scriptService;

    private final CacheRecycler cacheRecycler;

    private final PageCacheRecycler pageCacheRecycler;

    private final BigArrays bigArrays;

    private final IndexShard indexShard;

    private final IndexService indexService;

    private final ContextIndexSearcher searcher;

    private final DfsSearchResult dfsResult;

    private final QuerySearchResult queryResult;

    private final FetchSearchResult fetchResult;

    // lazy initialized only if needed
    private ScanContext scanContext;

    private float queryBoost = 1.0f;

    // timeout in millis
    private long timeoutInMillis = -1;

    // terminate after count
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;


    private List<String> groupStats;

    private Scroll scroll;

    private boolean explain;

    private boolean version = false; // by default, we don't return versions

    private List<String> fieldNames;
    private FieldDataFieldsContext fieldDataFields;
    private ScriptFieldsContext scriptFields;
    private PartialFieldsContext partialFields;
    private FetchSourceContext fetchSourceContext;

    private int from = -1;

    private int size = -1;

    private Sort sort;

    private Float minimumScore;

    private boolean trackScores = false; // when sorting, track scores as well...

    private ParsedQuery originalQuery;

    private Query query;

    private ParsedFilter postFilter;

    private Filter aliasFilter;

    private int[] docIdsToLoad;

    private int docsIdsToLoadFrom;

    private int docsIdsToLoadSize;

    private SearchContextAggregations aggregations;

    private SearchContextFacets facets;

    private SearchContextHighlight highlight;

    private SuggestionSearchContext suggest;

    private List<RescoreSearchContext> rescore;

    private SearchLookup searchLookup;

    private boolean queryRewritten;

    private volatile long keepAlive;

    private ScoreDoc lastEmittedDoc;

    private volatile long lastAccessTime = -1;

    private final ShardSearchRequest shardSearchRequest;

    public CrateSearchContext(long id,
                              final int numShards,
                              final String[] types,
                              final long nowInMillis,
                              SearchShardTarget shardTarget,
                              Engine.Searcher engineSearcher,
                              IndexService indexService,
                              final IndexShard indexShard,
                              ScriptService scriptService,
                              CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler,
                              BigArrays bigArrays,
                              Counter timeEstimateCounter,
                              @Nullable final String[] filteringAliases) {
        this.id = id;
        shardSearchRequest = new ShardSearchRequest() {
            @Override
            public String index() {
                return indexShard.indexService().index().name();
            }

            @Override
            public int shardId() {
                return indexShard.shardId().id();
            }

            @Override
            public String[] types() {
                return types;
            }

            @Override
            public BytesReference source() {
                return null;
            }

            @Override
            public void source(BytesReference source) {
                // ignore
            }

            @Override
            public BytesReference extraSource() {
                return null;
            }

            @Override
            public int numberOfShards() {
                return numShards;
            }

            @Override
            public SearchType searchType() {
                return CrateSearchContext.this.searchType();
            }

            @Override
            public String[] filteringAliases() {
                return filteringAliases;
            }

            @Override
            public long nowInMillis() {
                return nowInMillis;
            }

            @Override
            public String templateName() {
                return null;
            }

            @Override
            public ScriptService.ScriptType templateType() {
                return null;
            }

            @Override
            public Map<String, String> templateParams() {
                return null;
            }

            @Override
            public BytesReference templateSource() {
                return null;
            }

            @Override
            public Boolean queryCache() {
                return null;
            }

            @Override
            public Scroll scroll() {
                return scroll;
            }

            @Override
            public boolean useSlowScroll() {
                return false;
            }

            @Override
            public BytesReference cacheKey() throws IOException {
                return null;
            }
        };

        this.engineSearcher = engineSearcher;
        this.searcher = new ContextIndexSearcher(this, engineSearcher);

        this.shardTarget = shardTarget;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        // SearchContexts use a BigArrays that can circuit break
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexShard = indexShard;
        this.indexService = indexService;

        // initialize the filtering alias based on the provided filters
        this.aliasFilter = indexService.aliasesService().aliasFilter(filteringAliases);
        this.timeEstimateCounter = timeEstimateCounter;
    }

    @Override
    protected void doClose() {
        // clear and scope phase we  have
        Releasables.close(searcher, engineSearcher);
    }

    @Override
    public void preProcess() {
        if (!(from() == -1 && size() == -1)) {
            // from and size have been set.
            int numHits = from() + size();
            if (numHits < 0) {
                String msg = "Result window is too large, from + size must be less than or equal to: [" + Integer.MAX_VALUE + "] but was [" + (((long) from()) + ((long) size())) + "]";
                throw new QueryPhaseExecutionException(this, msg);
            }
        }

        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != 1.0f) {
            parsedQuery(new ParsedQuery(new FunctionScoreQuery(query(), new BoostScoreFunction(queryBoost)), parsedQuery()));
        }
        Filter searchFilter = searchFilter(types());
        if (searchFilter != null) {
            if (Queries.isConstantMatchAllQuery(query())) {
                Query q = new XConstantScoreQuery(searchFilter);
                q.setBoost(query().getBoost());
                parsedQuery(new ParsedQuery(q, parsedQuery()));
            } else {
                parsedQuery(new ParsedQuery(new XFilteredQuery(query(), searchFilter), parsedQuery()));
            }
        }
    }

    @Override
    public Filter searchFilter(String[] types) {
        Filter filter = mapperService().searchFilter(types);
        if (filter == null) {
            return aliasFilter;
        } else {
            filter = filterCache().cache(filter);
            if (aliasFilter != null) {
                return new AndFilter(ImmutableList.of(filter, aliasFilter));
            }
            return filter;
        }
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
    public String source() {
        return engineSearcher.source();
    }

    @Override
    public ShardSearchRequest request() {
        return shardSearchRequest;
    }

    @Override
    public SearchType searchType() {
        return SearchType.QUERY_THEN_FETCH;
    }

    @Override
    public SearchContext searchType(SearchType searchType) {
        // ignore for now
        return this;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public int numberOfShards() {
        return request().numberOfShards();
    }

    @Override
    public boolean hasTypes() {
        return request().types() != null && request().types().length > 0;
    }

    @Override
    public String[] types() {
        return request().types();
    }

    @Override
    public float queryBoost() {
        return queryBoost;
    }

    @Override
    public SearchContext queryBoost(float queryBoost) {
        this.queryBoost = queryBoost;
        return this;
    }

    @Override
    protected long nowInMillisImpl() {
        return shardSearchRequest.nowInMillis();
    }

    @Override
    public Scroll scroll() {
        return scroll;
    }

    @Override
    public SearchContext scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return aggregations;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    @Override
    public SearchContextFacets facets() {
        return facets;
    }

    @Override
    public SearchContext facets(SearchContextFacets facets) {
        this.facets = facets;
        return this;
    }

    @Override
    public SearchContextHighlight highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchContextHighlight highlight) {
        this.highlight = highlight;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return suggest;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    @Override
    public List<RescoreSearchContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        }
        return rescore;
    }

    @Override
    public void addRescore(RescoreSearchContext rescore) {
        if (this.rescore == null) {
            this.rescore = new ArrayList<>();
        }
        this.rescore.add(rescore);
    }

    @Override
    public boolean hasFieldDataFields() {
        return fieldDataFields != null;
    }

    @Override
    public FieldDataFieldsContext fieldDataFields() {
        if (fieldDataFields == null) {
            fieldDataFields = new FieldDataFieldsContext();
        }
        return this.fieldDataFields;
    }

    @Override
    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    @Override
    public boolean hasPartialFields() {
        return partialFields != null;
    }

    @Override
    public PartialFieldsContext partialFields() {
        if (partialFields == null) {
            partialFields = new PartialFieldsContext();
        }
        return this.partialFields;
    }

    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return this.fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    @Override
    public IndexShard indexShard() {
        return this.indexShard;
    }

    @Override
    public MapperService mapperService() {
        return indexService.mapperService();
    }

    @Override
    public AnalysisService analysisService() {
        return indexService.analysisService();
    }

    @Override
    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
    }

    @Override
    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    @Override
    public ScriptService scriptService() {
        return scriptService;
    }

    @Override
    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    @Override
    public PageCacheRecycler pageCacheRecycler() {
        return pageCacheRecycler;
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    public FilterCache filterCache() {
        return indexService.cache().filter();
    }

    @Override
    public FixedBitSetFilterCache fixedBitSetFilterCache() {
        return indexService.fixedBitSetFilterCache();
    }

    @Override
    public DocSetCache docSetCache() {
        return indexService.cache().docSet();
    }

    @Override
    public IndexFieldDataService fieldData() {
        return indexService.fieldData();
    }

    @Override
    public long timeoutInMillis() {
        return timeoutInMillis;
    }

    @Override
    public void timeoutInMillis(long timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public int terminateAfter() {
        return terminateAfter;
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        this.minimumScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return this.minimumScore;
    }

    @Override
    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public Sort sort() {
        return sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return trackScores;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedFilter postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    @Override
    public ParsedFilter parsedPostFilter() {
        return postFilter;
    }

    @Override
    public Filter aliasFilter() {
        return aliasFilter;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        queryRewritten = false;
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    @Override
    public Query query() {
        return query;
    }

    @Override
    public boolean queryRewritten() {
        return queryRewritten;
    }

    @Override
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        query = rewriteQuery;
        queryRewritten = true;
        return this;
    }

    @Override
    public int from() {
        return from;
    }

    @Override
    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean hasFieldNames() {
        return fieldNames != null;
    }

    @Override
    public List<String> fieldNames() {
        if (fieldNames == null) {
            fieldNames = Lists.newArrayList();
        }
        return fieldNames;
    }

    @Override
    public void emptyFieldNames() {
        this.fieldNames = ImmutableList.of();
    }

    @Override
    public boolean explain() {
        return explain;
    }

    @Override
    public void explain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public List<String> groupStats() {
        return groupStats;
    }

    @Override
    public void groupStats(List<String> groupStats) {
        this.groupStats = groupStats;
    }

    @Override
    public boolean version() {
        return version;
    }

    @Override
    public void version(boolean version) {
        this.version = version;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    @Override
    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    @Override
    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    @Override
    public long lastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long keepAlive() {
        return keepAlive;
    }

    @Override
    public void keepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public void lastEmittedDoc(ScoreDoc doc) {
        this.lastEmittedDoc = doc;
    }

    @Override
    public ScoreDoc lastEmittedDoc() {
        return lastEmittedDoc;
    }

    @Override
    public SearchLookup lookup() {
        // TODO: The types should take into account the parsing context in QueryParserContext...
        if (searchLookup == null) {
            searchLookup = new SearchLookup(mapperService(), fieldData(),
                    request().types());
        }
        return searchLookup;
    }

    @Override
    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public ScanContext scanContext() {
        if (scanContext == null) {
            scanContext = new ScanContext();
        }
        return scanContext;
    }

    @Override
    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return mapperService().smartName(name, request().types());
    }

    @Override
    public FieldMappers smartNameFieldMappers(String name) {
        return mapperService().smartNameFieldMappers(name, request().types());
    }

    @Override
    public FieldMapper smartNameFieldMapper(String name) {
        return mapperService().smartNameFieldMapper(name, request().types());
    }

    @Override
    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        return mapperService().smartNameObjectMapper(name, request().types());
    }

    @Override
    public boolean useSlowScroll() {
        return false;
    }

    @Override
    public SearchContext useSlowScroll(boolean useSlowScroll) {
        // ignore, we don't use slowscroll
        return this;
    }

    @Override
    public Counter timeEstimateCounter() {
        return timeEstimateCounter;
    }
}
