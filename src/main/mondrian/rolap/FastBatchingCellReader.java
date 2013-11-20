/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.AggGen;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.cache.SegmentCacheIndex;
import mondrian.rolap.cache.SegmentCacheIndexImpl;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.*;
import mondrian.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import java.util.*;
import java.util.concurrent.Future;

/**
 * A <code>FastBatchingCellReader</code> doesn't really Read cells: when asked
 * to look up the values of stored measures, it lies, and records the fact
 * that the value was asked for.  Later, we can look over the values which
 * are required, fetch them in an efficient way, and re-run the evaluation
 * with a real evaluator.
 *
 * <p>NOTE: When it doesn't know the answer, it lies by returning an error
 * object.  The calling code must be able to deal with that.</p>
 *
 * <p>This class tries to minimize the amount of storage needed to record the
 * fact that a cell was requested.</p>
 */
public class FastBatchingCellReader implements CellReader {
    private static final Logger LOGGER =
        Logger.getLogger(FastBatchingCellReader.class);

    private final int cellRequestLimit;

    private final RolapCube cube;

    /**
     * Records the number of requests. The field is used for correctness: if
     * the request count stays the same during an operation, you know that the
     * FastBatchingCellReader has not told any lies during that operation, and
     * therefore the result is true. The field is also useful for debugging.
     */
    private int missCount;

    /**
     * Number of occasions that a requested cell was already in cache.
     */
    private int hitCount;

    /**
     * Number of occasions that requested cell was in the process of being
     * loaded into cache but not ready.
     */
    private int pendingCount;

    private final AggregationManager aggMgr;

    private final boolean cacheEnabled;

    private final SegmentCacheManager cacheMgr;

    private final RolapAggregationManager.PinSet pinnedSegments;

    /**
     * Indicates that the reader has given incorrect results.
     */
    private boolean dirty;

    private final List<CellRequest> cellRequests = new ArrayList<CellRequest>();

    private final Execution execution;

    /**
     * Creates a FastBatchingCellReader.
     *
     * @param execution Execution that calling statement belongs to. Allows us
     *                  to check for cancel
     * @param cube      Cube that requests belong to
     * @param aggMgr    Aggregation manager
     */
    public FastBatchingCellReader(
        Execution execution,
        RolapCube cube,
        AggregationManager aggMgr)
    {
        this.execution = execution;
        assert cube != null;
        assert execution != null;
        this.cube = cube;
        this.aggMgr = aggMgr;
        cacheMgr = aggMgr.cacheMgr;
        pinnedSegments = this.aggMgr.createPinSet();
        cacheEnabled = !MondrianProperties.instance().DisableCaching.get();

        cellRequestLimit =
            MondrianProperties.instance().CellBatchSize.get() <= 0
                ? 100000 // TODO Make this logic into a pluggable algorithm.
                : MondrianProperties.instance().CellBatchSize.get();
    }

    public Object get(RolapEvaluator evaluator) {
        final CellRequest request =
            RolapAggregationManager.makeRequest(evaluator);

        if (request == null || request.isUnsatisfiable()) {
            return Util.nullValue; // request not satisfiable.
        }

        // Try to retrieve a cell and simultaneously pin the segment which
        // contains it.
        final Object o = aggMgr.getCellFromCache(request, pinnedSegments);

        assert o != Boolean.TRUE : "getCellFromCache no longer returns TRUE";
        if (o != null) {
            ++hitCount;
            return o;
        }

        // If this query has not had any cache misses, it's worth doing a
        // synchronous request for the cell segment. If it is in the cache, it
        // will be worth the wait, because we can avoid the effort of batching
        // up requests that could have been satisfied by the same segment.
        if (cacheEnabled
            && missCount == 0)
        {
            SegmentWithData segmentWithData = cacheMgr.peek(request);
            if (segmentWithData != null) {
                segmentWithData.getStar().register(segmentWithData);
                final Object o2 =
                    aggMgr.getCellFromCache(request, pinnedSegments);
                if (o2 != null) {
                    ++hitCount;
                    return o2;
                }
            }
        }

        // if there is no such cell, record that we need to fetch it, and
        // return 'error'
        recordCellRequest(request);
        return RolapUtil.valueNotReadyException;
    }

    public int getMissCount() {
        return missCount;
    }

    public int getHitCount() {
        return hitCount;
    }

    public int getPendingCount() {
        return pendingCount;
    }

    public final void recordCellRequest(CellRequest request) {
        assert !request.isUnsatisfiable();
        ++missCount;
        cellRequests.add(request);
        if (cellRequests.size() % cellRequestLimit == 0) {
            // Signal that it's time to ask the cache manager if it has cells
            // we need in the cache. Not really an exception.
            throw CellRequestQuantumExceededException.INSTANCE;
        }
    }

    /**
     * Returns whether this reader has told a lie. This is the case if there
     * are pending batches to load or if {@link #setDirty(boolean)} has been
     * called.
     */
    public boolean isDirty() {
        return dirty || !cellRequests.isEmpty();
    }

    /**
     * Resolves any pending cell reads using the cache. After calling this
     * method, all cells requested in a given batch are loaded into this
     * statement's local cache.
     *
     * <p>The method is implemented by making an asynchronous call to the cache
     * manager. The result is a list of segments that satisfies every cell
     * request.</p>
     *
     * <p>The client should put the resulting segments into its "query local"
     * cache, to ensure that future cells in that segment can be answered
     * without a call to the cache manager. (That is probably 1000x faster.)</p>
     *
     * <p>The cache manager does not inform where client where each segment
     * came from. There are several possibilities:</p>
     *
     * <ul>
     *     <li>Segment was already in cache (header and body)</li>
     *     <li>Segment is in the process of being loaded by executing a SQL
     *     statement (probably due to a request from another client)</li>
     *     <li>Segment is in an external cache (that is, header is in the cache,
     *        body is not yet)</li>
     *     <li>Segment can be created by rolling up one or more cache segments.
     *        (And of course each of these segments might be "paged out".)</li>
     *     <li>By executing a SQL {@code GROUP BY} statement</li>
     * </ul>
     *
     * <p>Furthermore, segments in external cache may take some time to retrieve
     * (a LAN round trip, say 1 millisecond, is a reasonable guess); and the
     * request may fail. (It depends on the cache, but caches are at liberty
     * to 'forget' segments.) So, any strategy that relies on cache segments
     * should be able to fall back. Even if there are fall backs, only one call
     * needs to be made to the cache manager.</p>
     *
     * @return Whether any aggregations were loaded.
     */
    boolean loadAggregations() {
        if (!isDirty()) {
            return false;
        }

        // List of futures yielding segments populated by SQL statements. If
        // loading requires several iterations, we just append to the list. We
        // don't mind if it takes a while for SQL statements to return.
        final List<Future<Map<Segment, SegmentWithData>>> sqlSegmentMapFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();

        final List<CellRequest> cellRequests1 =
            new ArrayList<CellRequest>(cellRequests);

        preloadColumnCardinality(cellRequests1);

        for (int iteration = 0;; ++iteration) {
            final BatchLoader.LoadBatchResponse response =
                cacheMgr.execute(
                    new BatchLoader.LoadBatchCommand(
                        Locus.peek(),
                        cacheMgr,
                        getDialect(),
                        cube,
                        Collections.unmodifiableList(cellRequests1)));

            int failureCount = 0;

            // Segments that have been retrieved from cache this cycle. Allows
            // us to reduce calls to the external cache.
            Map<SegmentHeader, SegmentBody> headerBodies =
                new HashMap<SegmentHeader, SegmentBody>();

            // Load each suggested segment from cache, and place it in
            // thread-local cache. Note that this step can't be done by the
            // cacheMgr -- it's our cache.
            for (SegmentHeader header : response.cacheSegments) {
                final SegmentBody body = cacheMgr.compositeCache.get(header);
                if (body == null) {
                    // REVIEW: This is an async call. It will return before the
                    // index is informed that this header is there,
                    // so a LoadBatchCommand might still return
                    // it on the next iteration.
                    if (cube.getStar() != null) {
                        cacheMgr.remove(cube.getStar(), header);
                    }
                    ++failureCount;
                    continue;
                }
                headerBodies.put(header, body);
                final SegmentWithData segmentWithData =
                    response.convert(header, body);
                segmentWithData.getStar().register(segmentWithData);
            }

            // Perform each suggested rollup.
            //
            // TODO this could be improved.
            // See http://jira.pentaho.com/browse/MONDRIAN-1195

            // Rollups that succeeded. Will tell cache mgr to put the headers
            // into the index and the header/bodies in cache.
            final Map<SegmentHeader, SegmentBody> succeededRollups =
                new HashMap<SegmentHeader, SegmentBody>();

            for (final BatchLoader.RollupInfo rollup : response.rollups) {
                // Gather the required segments.
                Map<SegmentHeader, SegmentBody> map =
                    findResidentRollupCandidate(headerBodies, rollup);
                if (map == null) {
                    // None of the candidate segment-sets for this rollup was
                    // all present in the cache.
                    continue;
                }

                final Set<String> keepColumns = new HashSet<String>();
                for (RolapStar.Column column : rollup.constrainedColumns) {
                    keepColumns.add(
                        column.getExpression().getGenericExpression());
                }
                Pair<SegmentHeader, SegmentBody> rollupHeaderBody =
                    SegmentBuilder.rollup(
                        map,
                        keepColumns,
                        rollup.constrainedColumnsBitKey,
                        rollup.measure.getAggregator().getRollup(),
                        rollup.measure.getDatatype());

                final SegmentHeader header = rollupHeaderBody.left;
                final SegmentBody body = rollupHeaderBody.right;

                if (headerBodies.containsKey(header)) {
                    // We had already created this segment, somehow.
                    continue;
                }

                headerBodies.put(header, body);
                succeededRollups.put(header, body);

                final SegmentWithData segmentWithData =
                    response.convert(header, body);

                // Register this segment with the local star.
                segmentWithData.getStar().register(segmentWithData);

                // Make sure that the cache manager knows about this new
                // segment. First thing we do is to add it to the index.
                // Then we insert the segment body into the SlotFuture.
                // This has to be done on the SegmentCacheManager's
                // Actor thread to ensure thread safety.
                if (!MondrianProperties.instance().DisableCaching.get()) {
                    final Locus locus = Locus.peek();
                    cacheMgr.execute(
                        new SegmentCacheManager.Command<Void>() {
                            public Void call() throws Exception {
                                SegmentCacheIndex index =
                                    cacheMgr.getIndexRegistry()
                                    .getIndex(segmentWithData.getStar());
                                boolean added = index.add(
                                    segmentWithData.getHeader(),
                                    true,
                                    response.converterMap.get(
                                        SegmentCacheIndexImpl
                                            .makeConverterKey(
                                                segmentWithData.getHeader())));
                                if (added) {
                                    index.loadSucceeded(
                                        segmentWithData.getHeader(), body);
                                }
                                return null;
                            }
                            public Locus getLocus() {
                                return locus;
                            }
                        });
                }
            }

            // Wait for SQL statements to end -- but only if there are no
            // failures.
            //
            // If there are failures, and its the first iteration, it's more
            // urgent that we create and execute a follow-up request. We will
            // wait for the pending SQL statements at the end of that.
            //
            // If there are failures on later iterations, wait for SQL
            // statements to end. The cache might be porous. SQL might be the
            // only way to make progress.
            sqlSegmentMapFutures.addAll(response.sqlSegmentMapFutures);
            if (failureCount == 0 || iteration > 0) {
                // Wait on segments being loaded by someone else.
                for (Map.Entry<SegmentHeader, Future<SegmentBody>> entry
                    : response.futures.entrySet())
                {
                    final SegmentHeader header = entry.getKey();
                    final Future<SegmentBody> bodyFuture = entry.getValue();
                    final SegmentBody body = Util.safeGet(
                        bodyFuture,
                        "Waiting for someone else's segment to load via SQL");
                    final SegmentWithData segmentWithData =
                        response.convert(header, body);
                    segmentWithData.getStar().register(segmentWithData);
                }

                // Wait on segments being loaded by SQL statements we asked for.
                for (Future<Map<Segment, SegmentWithData>> sqlSegmentMapFuture
                    : sqlSegmentMapFutures)
                {
                    final Map<Segment, SegmentWithData> segmentMap =
                        Util.safeGet(
                            sqlSegmentMapFuture,
                            "Waiting for segment to load via SQL");
                    for (SegmentWithData segmentWithData : segmentMap.values())
                    {
                        segmentWithData.getStar().register(segmentWithData);
                    }
                    // TODO: also pass back SegmentHeader and SegmentBody,
                    // and add these to headerBodies. Might help?
                }
            }

            if (failureCount == 0) {
                break;
            }

            // Figure out which cell requests are not satisfied by any of the
            // segments retrieved.
            @SuppressWarnings("unchecked")
            List<CellRequest> old = new ArrayList<CellRequest>(cellRequests1);
            cellRequests1.clear();
            for (CellRequest cellRequest : old) {
                if (cellRequest.getMeasure().getStar()
                    .getCellFromCache(cellRequest, null) == null)
                {
                    cellRequests1.add(cellRequest);
                }
            }

            if (cellRequests1.isEmpty()) {
                break;
            }

            if (cellRequests1.size() >= old.size()
                && iteration > 10)
            {
                throw Util.newError(
                    "Cache round-trip did not resolve any cell requests. "
                    + "Iteration #" + iteration
                    + "; request count " + cellRequests1.size()
                    + "; requested headers: " + response.cacheSegments.size()
                    + "; requested rollups: " + response.rollups.size()
                    + "; requested SQL: "
                    + response.sqlSegmentMapFutures.size());
            }

            // Continue loop; form and execute a new request with the smaller
            // set of cell requests.
        }

        dirty = false;
        cellRequests.clear();
        return true;
    }

    /**
     * Iterates through cell requests and makes sure .getCardinality has
     * been called on all constrained columns.  This is a  workaround
     * to an issue in which cardinality queries can be fired on the Actor
     * thread, potentially causing a deadlock when interleaved with
     * other threads that depend both on db connections and Actor responses.
     *
     */
    private void preloadColumnCardinality(List<CellRequest> cellRequests) {
        List<BitKey> loaded = new ArrayList<BitKey>();
        for (CellRequest req : cellRequests) {
            if (!loaded.contains(req.getConstrainedColumnsBitKey())) {
                for (RolapStar.Column col : req.getConstrainedColumns()) {
                    col.getCardinality();
                }
                loaded.add(req.getConstrainedColumnsBitKey());
            }
        }
    }

    /**
     * Finds a segment-list among a list of candidate segment-lists
     * for which the bodies of all segments are in cache. Returns a map
     * from segment-to-body if found, or null if not found.
     *
     * @param headerBodies Cache of bodies previously retrieved from external
     *                     cache
     *
     * @param rollup       Specifies what segments to roll up, and the
     *                     target dimensionality
     *
     * @return Collection of segment headers and bodies suitable for rollup,
     * or null
     */
    private Map<SegmentHeader, SegmentBody> findResidentRollupCandidate(
        Map<SegmentHeader, SegmentBody> headerBodies,
        BatchLoader.RollupInfo rollup)
    {
        candidateLoop:
        for (List<SegmentHeader> headers : rollup.candidateLists) {
            final Map<SegmentHeader, SegmentBody> map =
                new HashMap<SegmentHeader, SegmentBody>();
            for (SegmentHeader header : headers) {
                SegmentBody body = loadSegmentFromCache(headerBodies, header);
                if (body == null) {
                    // To proceed with a candidate, require all headers to
                    // be in cache.
                    continue candidateLoop;
                }
                map.put(header, body);
            }
            return map;
        }
        return null;
    }

    private SegmentBody loadSegmentFromCache(
        Map<SegmentHeader, SegmentBody> headerBodies,
        SegmentHeader header)
    {
        SegmentBody body = headerBodies.get(header);
        if (body != null) {
            return body;
        }
        body = cacheMgr.compositeCache.get(header);
        if (body == null) {
            if (cube.getStar() != null) {
                cacheMgr.remove(cube.getStar(), header);
            }
            return null;
        }
        headerBodies.put(header, body);
        return body;
    }

    /**
     * Returns the SQL dialect. Overridden in some unit tests.
     *
     * @return Dialect
     */
    Dialect getDialect() {
        final RolapStar star = cube.getStar();
        if (star != null) {
            return star.getSqlQueryDialect();
        } else {
            return cube.getSchema().getDialect();
        }
    }

    /**
     * Sets the flag indicating that the reader has told a lie.
     */
    void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

}

/**
 * Context for processing a request to the cache manager for segments matching a
 * collection of cell requests. All methods except the constructor are executed
 * by the cache manager's dedicated thread.
 */
class BatchLoader {
    private static final Logger LOGGER =
        Logger.getLogger(FastBatchingCellReader.class);

    private final Locus locus;
    private final SegmentCacheManager cacheMgr;
    private final Dialect dialect;
    private final RolapCube cube;

    private final Map<AggregationKey, Batch> batches =
        new HashMap<AggregationKey, Batch>();

    private final Set<SegmentHeader> cacheHeaders =
        new LinkedHashSet<SegmentHeader>();

    private final Map<SegmentHeader, Future<SegmentBody>> futures =
        new HashMap<SegmentHeader, Future<SegmentBody>>();

    private final List<RollupInfo> rollups = new ArrayList<RollupInfo>();

    private final Set<BitKey> rollupBitmaps = new HashSet<BitKey>();

    private final Map<List, SegmentBuilder.SegmentConverter> converterMap =
        new HashMap<List, SegmentBuilder.SegmentConverter>();

    public BatchLoader(
        Locus locus,
        SegmentCacheManager cacheMgr,
        Dialect dialect,
        RolapCube cube)
    {
        this.locus = locus;
        this.cacheMgr = cacheMgr;
        this.dialect = dialect;
        this.cube = cube;
    }

    final boolean shouldUseGroupingFunction() {
        return MondrianProperties.instance().EnableGroupingSets.get()
            && dialect.supportsGroupingSets();
    }

    private void recordCellRequest2(final CellRequest request) {
        // If there is a segment matching these criteria, write it to the list
        // of found segments, and remove the cell request from the list.
        final AggregationKey key = new AggregationKey(request);

        final SegmentBuilder.SegmentConverterImpl converter =
                new SegmentBuilder.SegmentConverterImpl(key, request);

        boolean success =
            loadFromCaches(request, key, converter);
        // Skip the batch if we already have a rollup for it.
        if (rollupBitmaps.contains(request.getConstrainedColumnsBitKey())) {
            return;
        }

        // As a last resort, we load from SQL.
        if (!success) {
            loadFromSql(request, key, converter);
        }
    }

    /**
     * Loads a cell from caches. If the cell is successfully loaded,
     * we return true.
     */
    private boolean loadFromCaches(
        final CellRequest request,
        final AggregationKey key,
        final SegmentBuilder.SegmentConverterImpl converter)
    {
        if (MondrianProperties.instance().DisableCaching.get()) {
            // Caching is disabled. Return always false.
            return false;
        }

        // Is request matched by one of the headers we intend to load?
        final Map<String, Comparable> mappedCellValues =
            request.getMappedCellValues();
        final List<String> compoundPredicates =
            AggregationKey.getCompoundPredicateStringList(
                key.getStar(),
                key.getCompoundPredicateList());

        for (SegmentHeader header : cacheHeaders) {
            if (SegmentCacheIndexImpl.matches(
                    header,
                    mappedCellValues,
                    compoundPredicates))
            {
                // It's likely that the header will be in the cache, so this
                // request will be satisfied. If not, the header will be removed
                // from the segment index, and we'll be back.
                return true;
            }
        }
        final RolapStar.Measure measure = request.getMeasure();
        final RolapStar star = measure.getStar();
        final RolapSchema schema = star.getSchema();
        final SegmentCacheIndex index =
            cacheMgr.getIndexRegistry().getIndex(star);
        final List<SegmentHeader> headersInCache =
            index.locate(
                schema.getName(),
                schema.getChecksum(),
                measure.getCubeName(),
                measure.getName(),
                star.getFactTable().getAlias(),
                request.getConstrainedColumnsBitKey(),
                mappedCellValues,
                compoundPredicates);

        // Ask for the first segment to be loaded from cache. (If it's no longer
        // in cache, we'll be back, and presumably we'll try the second
        // segment.)

        if (!headersInCache.isEmpty()) {
            final SegmentHeader headerInCache = headersInCache.get(0);

            final Future<SegmentBody> future =
                index.getFuture(locus.execution, headerInCache);

            if (future != null) {
                // Segment header is in cache, body is being loaded. Worker will
                // need to wait for load to complete.
                futures.put(headerInCache, future);
            } else {
                // Segment is in cache.
                cacheHeaders.add(headerInCache);
            }
            index.setConverter(
                headerInCache.schemaName,
                headerInCache.schemaChecksum,
                headerInCache.cubeName,
                headerInCache.rolapStarFactTableName,
                headerInCache.measureName,
                headerInCache.compoundPredicates,
                converter);
            converterMap.put(
                SegmentCacheIndexImpl.makeConverterKey(request, key),
                converter);
            return true;
        }

        // Try to roll up if the measure's rollup aggregator supports
        // "fast" aggregation from raw objects.
        //
        // Do not try to roll up if this request has already chosen a rollup
        // with the same target dimensionality. It is quite likely that the
        // other rollup will satisfy this request, and it's complicated to be
        // 100% sure. If we're wrong, we'll be back.

        // Also make sure that we don't try to rollup a measure which
        // doesn't support rollup from raw data, like a distinct count
        // for example. Both the measure's aggregator and its rollup
        // aggregator must support raw data aggregation. We call
        // Aggregator.supportsFastAggregates() to verify.
        if (MondrianProperties.instance()
                .EnableInMemoryRollup.get()
            && measure.getAggregator().supportsFastAggregates(
                measure.getDatatype())
            && measure.getAggregator().getRollup().supportsFastAggregates(
                measure.getDatatype())
            && !isRequestCoveredByRollups(request))
        {
            // Don't even bother doing a segment lookup if we can't
            // rollup that measure.
            final List<List<SegmentHeader>> rollup =
                index.findRollupCandidates(
                    schema.getName(),
                    schema.getChecksum(),
                    measure.getCubeName(),
                    measure.getName(),
                    star.getFactTable().getAlias(),
                    request.getConstrainedColumnsBitKey(),
                    mappedCellValues,
                    AggregationKey.getCompoundPredicateStringList(
                        star,
                        key.getCompoundPredicateList()));
            if (!rollup.isEmpty()) {
                rollups.add(
                    new RollupInfo(
                        request,
                        rollup));
                rollupBitmaps.add(request.getConstrainedColumnsBitKey());
                converterMap.put(
                    SegmentCacheIndexImpl.makeConverterKey(request, key),
                    new SegmentBuilder.StarSegmentConverter(
                        measure,
                        key.getCompoundPredicateList()));
                return true;
            }
        }
        return false;
    }

      /**
       * Checks if the request can be satisfied by a rollup already in place
       * and moves that rollup to the top of the list if not there.
       */
      private boolean isRequestCoveredByRollups(CellRequest request) {
          BitKey bitKey = request.getConstrainedColumnsBitKey();
          if (!rollupBitmaps.contains(bitKey)) {
              return false;
          }
          List<SegmentHeader> firstOkList = null;
          for (RollupInfo rollupInfo : rollups) {
              if (!rollupInfo.constrainedColumnsBitKey.equals(bitKey)) {
                  continue;
              }
              int candidateListsIdx = 0;
              // bitkey is the same, are the constrained values compatible?
              candidatesLoop:
                  for (List<SegmentHeader> candList
                      : rollupInfo.candidateLists)
                  {
                      for (SegmentHeader header : candList) {
                          if (headerCoversRequest(header, request)) {
                              firstOkList = candList;
                              break candidatesLoop;
                          }
                      }
                      candidateListsIdx++;
                  }
              if (firstOkList != null) {
                  if (candidateListsIdx > 0) {
                      // move good candidate list to first position
                      rollupInfo.candidateLists.remove(candidateListsIdx);
                      rollupInfo.candidateLists.set(0, firstOkList);
                  }
                  return true;
              }
          }
          return false;
      }

      /**
       * Check constraint compatibility
       */
      private boolean headerCoversRequest(
          SegmentHeader header,
          CellRequest request)
      {
          BitKey bitKey = request.getConstrainedColumnsBitKey();
          assert header.getConstrainedColumnsBitKey().cardinality()
                >= bitKey.cardinality();
          BitKey headerBitKey = header.getConstrainedColumnsBitKey();
          // get all constrained values for relevant bitKey positions
          List<SortedSet<Comparable>> headerValues =
              new ArrayList<SortedSet<Comparable>>(bitKey.cardinality());
          Map<Integer, Integer> valueIndexes = new HashMap<Integer, Integer>();
          int relevantCCIdx = 0, keyValuesIdx = 0;
          for (int bitPos : headerBitKey) {
              if (bitKey.get(bitPos)) {
                  headerValues.add(
                      header.getConstrainedColumns().get(relevantCCIdx).values);
                  valueIndexes.put(bitPos, keyValuesIdx++);
              }
              relevantCCIdx++;
          }
          assert request.getConstrainedColumns().length
              == request.getSingleValues().length;
          // match header constraints against request values
          for (int i = 0; i < request.getConstrainedColumns().length; i++) {
              RolapStar.Column col = request.getConstrainedColumns()[i];
              Integer valueIdx = valueIndexes.get(col.getBitPosition());
              if (headerValues.get(valueIdx) != null
                  && !headerValues.get(valueIdx).contains(
                      request.getSingleValues()[i]))
              {
                return false;
              }
          }
          return true;
      }

    private void loadFromSql(
        final CellRequest request,
        final AggregationKey key,
        final SegmentBuilder.SegmentConverterImpl converter)
    {
        // Finally, add to a batch. It will turn in to a SQL request.
        Batch batch = batches.get(key);
        if (batch == null) {
            batch = new Batch(request);
            batches.put(key, batch);
            converterMap.put(
                SegmentCacheIndexImpl.makeConverterKey(request, key),
                converter);

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("FastBatchingCellReader: bitkey=");
                buf.append(request.getConstrainedColumnsBitKey());
                buf.append(Util.nl);

                for (RolapStar.Column column
                    : request.getConstrainedColumns())
                {
                    buf.append("  ");
                    buf.append(column);
                    buf.append(Util.nl);
                }
                LOGGER.debug(buf.toString());
            }
        }
        batch.add(request);
    }

    /**
     * Determines which segments need to be loaded from external cache,
     * created using roll up, or created using SQL to satisfy a given list
     * of cell requests.
     *
     * @return List of segment futures. Each segment future may or may not be
     *    already present (it depends on the current location of the segment
     *    body). Each future will return a not-null segment (or throw).
     */
    LoadBatchResponse load(List<CellRequest> cellRequests) {
        // Check for cancel/timeout. The request might have been on the queue
        // for a while.
        if (locus.execution != null) {
            locus.execution.checkCancelOrTimeout();
        }

        final long t1 = System.currentTimeMillis();

        // Now we're inside the cache manager, we can see which of our cell
        // requests can be answered from cache. Those that can will be added
        // to the segments list; those that can not will be converted into
        // batches and rolled up or loaded using SQL.
        for (CellRequest cellRequest : cellRequests) {
            recordCellRequest2(cellRequest);
        }

        // Sort the batches into deterministic order.
        List<Batch> batchList =
            new ArrayList<Batch>(batches.values());
        Collections.sort(batchList, BatchComparator.instance);
        final List<Future<Map<Segment, SegmentWithData>>> segmentMapFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        if (shouldUseGroupingFunction()) {
            LOGGER.debug("Using grouping sets");
            List<CompositeBatch> groupedBatches = groupBatches(batchList);
            for (CompositeBatch batch : groupedBatches) {
                batch.load(segmentMapFutures);
            }
        } else {
            // Load batches in turn.
            for (Batch batch : batchList) {
                batch.loadAggregation(segmentMapFutures);
            }
        }

        if (LOGGER.isDebugEnabled()) {
            final long t2 = System.currentTimeMillis();
            LOGGER.debug("load (millis): " + (t2 - t1));
        }

        // Create a response and return it to the client. The response is a
        // bunch of work to be done (waiting for segments to load from SQL, to
        // come from cache, and so forth) on the client's time. Some of the bets
        // may not come off, in which case, the client will send us another
        // request.
        return new LoadBatchResponse(
            cellRequests,
            new ArrayList<SegmentHeader>(cacheHeaders),
            rollups,
            converterMap,
            segmentMapFutures,
            futures);
    }

    static List<CompositeBatch> groupBatches(List<Batch> batchList) {
        Map<AggregationKey, CompositeBatch> batchGroups =
            new HashMap<AggregationKey, CompositeBatch>();
        for (int i = 0; i < batchList.size(); i++) {
            for (int j = i + 1; j < batchList.size();) {
                final Batch iBatch = batchList.get(i);
                final Batch jBatch = batchList.get(j);
                if (iBatch.canBatch(jBatch)) {
                    batchList.remove(j);
                    addToCompositeBatch(batchGroups, iBatch, jBatch);
                } else if (jBatch.canBatch(iBatch)) {
                    batchList.set(i, jBatch);
                    batchList.remove(j);
                    addToCompositeBatch(batchGroups, jBatch, iBatch);
                    j = i + 1;
                } else {
                    j++;
                }
            }
        }

        wrapNonBatchedBatchesWithCompositeBatches(batchList, batchGroups);
        final CompositeBatch[] compositeBatches =
            batchGroups.values().toArray(
                new CompositeBatch[batchGroups.size()]);
        Arrays.sort(compositeBatches, CompositeBatchComparator.instance);
        return Arrays.asList(compositeBatches);
    }

    private static void wrapNonBatchedBatchesWithCompositeBatches(
        List<Batch> batchList,
        Map<AggregationKey, CompositeBatch> batchGroups)
    {
        for (Batch batch : batchList) {
            if (batchGroups.get(batch.batchKey) == null) {
                batchGroups.put(batch.batchKey, new CompositeBatch(batch));
            }
        }
    }

    static void addToCompositeBatch(
        Map<AggregationKey, CompositeBatch> batchGroups,
        Batch detailedBatch,
        Batch summaryBatch)
    {
        CompositeBatch compositeBatch = batchGroups.get(detailedBatch.batchKey);

        if (compositeBatch == null) {
            compositeBatch = new CompositeBatch(detailedBatch);
            batchGroups.put(detailedBatch.batchKey, compositeBatch);
        }

        CompositeBatch compositeBatchOfSummaryBatch =
            batchGroups.remove(summaryBatch.batchKey);

        if (compositeBatchOfSummaryBatch != null) {
            compositeBatch.merge(compositeBatchOfSummaryBatch);
        } else {
            compositeBatch.add(summaryBatch);
        }
    }

    /**
     * Command that loads the segments required for a collection of cell
     * requests. Returns the collection of segments.
     */
    public static class LoadBatchCommand
        implements SegmentCacheManager.Command<LoadBatchResponse>
    {
        private final Locus locus;
        private final SegmentCacheManager cacheMgr;
        private final Dialect dialect;
        private final RolapCube cube;
        private final List<CellRequest> cellRequests;
        private final Map<String, Object> mdc =
            new HashMap<String, Object>();

        public LoadBatchCommand(
            Locus locus,
            SegmentCacheManager cacheMgr,
            Dialect dialect,
            RolapCube cube,
            List<CellRequest> cellRequests)
        {
            this.locus = locus;
            this.cacheMgr = cacheMgr;
            this.dialect = dialect;
            this.cube = cube;
            this.cellRequests = cellRequests;

            if (MDC.getContext() != null) {
                this.mdc.putAll(MDC.getContext());
            }
        }

        public LoadBatchResponse call() {
            if (MDC.getContext() != null) {
                final Map<String, Object> old = MDC.getContext();
                old.clear();
                old.putAll(mdc);
            }
            return new BatchLoader(locus, cacheMgr, dialect, cube)
                .load(cellRequests);
        }

        public Locus getLocus() {
            return locus;
        }
    }

    /**
     * Set of Batches which can grouped together.
     */
    static class CompositeBatch {
        /** Batch with most number of constraint columns */
        final Batch detailedBatch;

        /** Batches whose data can be fetched using rollup on detailed batch */
        final List<Batch> summaryBatches = new ArrayList<Batch>();

        CompositeBatch(Batch detailedBatch) {
            this.detailedBatch = detailedBatch;
        }

        void add(Batch summaryBatch) {
            summaryBatches.add(summaryBatch);
        }

        void merge(CompositeBatch summaryBatch) {
            summaryBatches.add(summaryBatch.detailedBatch);
            summaryBatches.addAll(summaryBatch.summaryBatches);
        }

        public void load(
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            GroupingSetsCollector batchCollector =
                new GroupingSetsCollector(true);
            this.detailedBatch.loadAggregation(batchCollector, segmentFutures);

            int cellRequestCount = 0;
            for (Batch batch : summaryBatches) {
                batch.loadAggregation(batchCollector, segmentFutures);
                cellRequestCount += batch.cellRequestCount;
            }

            getSegmentLoader().load(
                cellRequestCount,
                batchCollector.getGroupingSets(),
                detailedBatch.batchKey.getCompoundPredicateList(),
                segmentFutures);
        }

        SegmentLoader getSegmentLoader() {
            return new SegmentLoader(detailedBatch.getCacheMgr());
        }
    }

    private static final Logger BATCH_LOGGER = Logger.getLogger(Batch.class);

    public static class RollupInfo {
        final RolapStar.Column[] constrainedColumns;
        final BitKey constrainedColumnsBitKey;
        final RolapStar.Measure measure;
        final List<List<SegmentHeader>> candidateLists;

        RollupInfo(
            CellRequest request,
            List<List<SegmentHeader>> candidateLists)
        {
            this.candidateLists = candidateLists;
            constrainedColumns = request.getConstrainedColumns();
            constrainedColumnsBitKey = request.getConstrainedColumnsBitKey();
            measure = request.getMeasure();
        }
    }

    /**
     * Request sent from cache manager to a worker to load segments into
     * the cache, create segments by rolling up, and to wait for segments
     * being loaded via SQL.
     */
    static class LoadBatchResponse {
        /**
         * List of segments that are being loaded using SQL.
         *
         * <p>Other workers are executing the SQL. When done, they will write a
         * segment body or an error into the respective futures. The thread
         * processing this request will wait on those futures, once all segments
         * have successfully arrived from cache.</p>
         */
        final List<Future<Map<Segment, SegmentWithData>>> sqlSegmentMapFutures;

        /**
         * List of segments we are trying to load from the cache.
         */
        final List<SegmentHeader> cacheSegments;

        /**
         * List of cell requests that will be satisfied by segments we are
         * trying to load from the cache (or create by rolling up).
         */
        final List<CellRequest> cellRequests;

        /**
         * List of segments to be created from segments in the cache, provided
         * that the cache segments come through.
         *
         * <p>If they do not, we will need to tell the cache manager to remove
         * the pending segments.</p>
         */
        final List<RollupInfo> rollups;

        final Map<List, SegmentBuilder.SegmentConverter> converterMap;

        final Map<SegmentHeader, Future<SegmentBody>> futures;

        LoadBatchResponse(
            List<CellRequest> cellRequests,
            List<SegmentHeader> cacheSegments,
            List<RollupInfo> rollups,
            Map<List, SegmentBuilder.SegmentConverter> converterMap,
            List<Future<Map<Segment, SegmentWithData>>> sqlSegmentMapFutures,
            Map<SegmentHeader, Future<SegmentBody>> futures)
        {
            this.cellRequests = cellRequests;
            this.sqlSegmentMapFutures = sqlSegmentMapFutures;
            this.cacheSegments = cacheSegments;
            this.rollups = rollups;
            this.converterMap = converterMap;
            this.futures = futures;
        }

        public SegmentWithData convert(
            SegmentHeader header,
            SegmentBody body)
        {
            final SegmentBuilder.SegmentConverter converter =
                converterMap.get(
                    SegmentCacheIndexImpl.makeConverterKey(header));
            return converter.convert(header, body);
        }
    }

    public class Batch {
        // the CellRequest's constrained columns
        final RolapStar.Column[] columns;
        final List<RolapStar.Measure> measuresList =
            new ArrayList<RolapStar.Measure>();
        final Set<StarColumnPredicate>[] valueSets;
        final AggregationKey batchKey;
        // string representation; for debug; set lazily in toString
        private String string;
        private int cellRequestCount;
        private List<StarColumnPredicate[]> tuples =
            new ArrayList<StarColumnPredicate[]>();

        public Batch(CellRequest request) {
            columns = request.getConstrainedColumns();
            valueSets = new HashSet[columns.length];
            for (int i = 0; i < valueSets.length; i++) {
                valueSets[i] = new HashSet<StarColumnPredicate>();
            }
            batchKey = new AggregationKey(request);
        }

        public String toString() {
            if (string == null) {
                final StringBuilder buf = new StringBuilder();
                buf.append("Batch {\n")
                    .append("  columns={").append(Arrays.toString(columns))
                    .append("}\n")
                    .append("  measures={").append(measuresList).append("}\n")
                    .append("  valueSets={").append(Arrays.toString(valueSets))
                    .append("}\n")
                    .append("  batchKey=").append(batchKey).append("}\n")
                    .append("}");
                string = buf.toString();
            }
            return string;
        }

        public final void add(CellRequest request) {
            ++cellRequestCount;
            final int valueCount = request.getNumValues();
            final StarColumnPredicate[] tuple =
                new StarColumnPredicate[valueCount];
            for (int j = 0; j < valueCount; j++) {
                final StarColumnPredicate value = request.getValueAt(j);
                valueSets[j].add(value);
                tuple[j] = value;
            }
            tuples.add(tuple);
            final RolapStar.Measure measure = request.getMeasure();
            if (!measuresList.contains(measure)) {
                assert (measuresList.size() == 0)
                       || (measure.getStar()
                           == (measuresList.get(0)).getStar())
                    : "Measure must belong to same star as other measures";
                measuresList.add(measure);
            }
        }

        /**
         * Returns the RolapStar associated with the Batch's first Measure.
         *
         * <p>This method can only be called after the {@link #add} method has
         * been called.
         *
         * @return the RolapStar associated with the Batch's first Measure
         */
        private RolapStar getStar() {
            RolapStar.Measure measure = measuresList.get(0);
            return measure.getStar();
        }

        public BitKey getConstrainedColumnsBitKey() {
            return batchKey.getConstrainedColumnsBitKey();
        }

        public SegmentCacheManager getCacheMgr() {
            return cacheMgr;
        }

        public final void loadAggregation(
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            GroupingSetsCollector collectorWithGroupingSetsTurnedOff =
                new GroupingSetsCollector(false);
            loadAggregation(collectorWithGroupingSetsTurnedOff, segmentFutures);
        }

        final void loadAggregation(
            GroupingSetsCollector groupingSetsCollector,
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            if (MondrianProperties.instance().GenerateAggregateSql.get()) {
                generateAggregateSql();
            }
            final StarColumnPredicate[] predicates = initPredicates();
            final long t1 = System.currentTimeMillis();

            // TODO: optimize key sets; drop a constraint if more than x% of
            // the members are requested; whether we should get just the cells
            // requested or expand to a n-cube

            // If the database cannot execute "count(distinct ...)", split the
            // distinct aggregations out.
            int distinctMeasureCount = getDistinctMeasureCount(measuresList);
            boolean tooManyDistinctMeasures =
                distinctMeasureCount > 0
                && !dialect.allowsCountDistinct()
                || distinctMeasureCount > 1
                   && !dialect.allowsMultipleCountDistinct();

            if (tooManyDistinctMeasures) {
                doSpecialHandlingOfDistinctCountMeasures(
                    predicates,
                    groupingSetsCollector,
                    segmentFutures);
            }

            // Load agg(distinct <SQL expression>) measures individually
            // for DBs that does allow multiple distinct SQL measures.
            if (!dialect.allowsMultipleDistinctSqlMeasures()) {
                // Note that the intention was originally to capture the
                // subquery SQL measures and separate them out; However,
                // without parsing the SQL string, Mondrian cannot distinguish
                // between "col1" + "col2" and subquery. Here the measure list
                // contains both types.

                // See the test case testLoadDistinctSqlMeasure() in
                //  mondrian.rolap.FastBatchingCellReaderTest

                List<RolapStar.Measure> distinctSqlMeasureList =
                    getDistinctSqlMeasures(measuresList);
                for (RolapStar.Measure measure : distinctSqlMeasureList) {
                    AggregationManager.loadAggregation(
                        cacheMgr,
                        cellRequestCount,
                        Collections.singletonList(measure),
                        columns,
                        batchKey,
                        predicates,
                        groupingSetsCollector,
                        segmentFutures);
                    measuresList.remove(measure);
                }
            }

            final int measureCount = measuresList.size();
            if (measureCount > 0) {
                AggregationManager.loadAggregation(
                    cacheMgr,
                    cellRequestCount,
                    measuresList,
                    columns,
                    batchKey,
                    predicates,
                    groupingSetsCollector,
                    segmentFutures);
            }

            if (BATCH_LOGGER.isDebugEnabled()) {
                final long t2 = System.currentTimeMillis();
                BATCH_LOGGER.debug(
                    "Batch.load (millis) " + (t2 - t1));
            }
        }

        private void doSpecialHandlingOfDistinctCountMeasures(
            StarColumnPredicate[] predicates,
            GroupingSetsCollector groupingSetsCollector,
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            while (true) {
                // Scan for a measure based upon a distinct aggregation.
                final RolapStar.Measure distinctMeasure =
                    getFirstDistinctMeasure(measuresList);
                if (distinctMeasure == null) {
                    break;
                }
                final String expr =
                    distinctMeasure.getExpression().getGenericExpression();
                final List<RolapStar.Measure> distinctMeasuresList =
                    new ArrayList<RolapStar.Measure>();
                for (int i = 0; i < measuresList.size();) {
                    final RolapStar.Measure measure = measuresList.get(i);
                    if (measure.getAggregator().isDistinct()
                        && measure.getExpression().getGenericExpression()
                        .equals(expr))
                    {
                        measuresList.remove(i);
                        distinctMeasuresList.add(distinctMeasure);
                    } else {
                        i++;
                    }
                }

                // Load all the distinct measures based on the same expression
                // together
                AggregationManager.loadAggregation(
                    cacheMgr,
                    cellRequestCount,
                    distinctMeasuresList,
                    columns,
                    batchKey,
                    predicates,
                    groupingSetsCollector,
                    segmentFutures);
            }
        }

        private StarColumnPredicate[] initPredicates() {
            StarColumnPredicate[] predicates =
                new StarColumnPredicate[columns.length];
            for (int j = 0; j < columns.length; j++) {
                Set<StarColumnPredicate> valueSet = valueSets[j];

                StarColumnPredicate predicate;
                if (valueSet == null) {
                    predicate = LiteralStarPredicate.FALSE;
                } else {
                    ValueColumnPredicate[] values =
                        valueSet.toArray(
                            new ValueColumnPredicate[valueSet.size()]);
                    // Sort array to achieve determinism in generated SQL.
                    Arrays.sort(
                        values,
                        ValueColumnConstraintComparator.instance);

                    predicate =
                        new ListColumnPredicate(
                            columns[j],
                            Arrays.asList((StarColumnPredicate[]) values));
                }

                predicates[j] = predicate;
            }
            return predicates;
        }

        private void generateAggregateSql() {
            if (cube == null || cube.isVirtual()) {
                final StringBuilder buf = new StringBuilder(64);
                buf.append(
                    "AggGen: Sorry, can not create SQL for virtual Cube \"")
                    .append(cube == null ? null : cube.getName())
                    .append("\", operation not currently supported");
                BATCH_LOGGER.error(buf.toString());

            } else {
                final AggGen aggGen =
                    new AggGen(cube.getName(), cube.getStar(), columns);
                if (aggGen.isReady()) {
                    // PRINT TO STDOUT - DO NOT USE BATCH_LOGGER
                    System.out.println(
                        "createLost:" + Util.nl + aggGen.createLost());
                    System.out.println(
                        "insertIntoLost:" + Util.nl + aggGen.insertIntoLost());
                    System.out.println(
                        "createCollapsed:" + Util.nl
                        + aggGen.createCollapsed());
                    System.out.println(
                        "insertIntoCollapsed:" + Util.nl
                        + aggGen.insertIntoCollapsed());
                } else {
                    BATCH_LOGGER.error("AggGen failed");
                }
            }
        }

        /**
         * Returns the first measure based upon a distinct aggregation, or null
         * if there is none.
         */
        final RolapStar.Measure getFirstDistinctMeasure(
            List<RolapStar.Measure> measuresList)
        {
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()) {
                    return measure;
                }
            }
            return null;
        }

        /**
         * Returns the number of the measures based upon a distinct
         * aggregation.
         */
        private int getDistinctMeasureCount(
            List<RolapStar.Measure> measuresList)
        {
            int count = 0;
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()) {
                    ++count;
                }
            }
            return count;
        }

        /**
         * Returns the list of measures based upon a distinct aggregation
         * containing SQL measure expressions(as opposed to column expressions).
         *
         * This method was initially intended for only those measures that are
         * defined using subqueries(for DBs that support them). However, since
         * Mondrian does not parse the SQL string, the method will count both
         * queries as well as some non query SQL expressions.
         */
        private List<RolapStar.Measure> getDistinctSqlMeasures(
            List<RolapStar.Measure> measuresList)
        {
            List<RolapStar.Measure> distinctSqlMeasureList =
                new ArrayList<RolapStar.Measure>();
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()
                    && measure.getExpression() instanceof
                        MondrianDef.MeasureExpression)
                {
                    MondrianDef.MeasureExpression measureExpr =
                        (MondrianDef.MeasureExpression) measure.getExpression();
                    MondrianDef.SQL measureSql = measureExpr.expressions[0];
                    // Checks if the SQL contains "SELECT" to detect the case a
                    // subquery is used to define the measure. This is not a
                    // perfect check, because a SQL expression on column names
                    // containing "SELECT" will also be detected. e,g,
                    // count("select beef" + "regular beef").
                    if (measureSql.cdata.toUpperCase().contains("SELECT")) {
                        distinctSqlMeasureList.add(measure);
                    }
                }
            }
            return distinctSqlMeasureList;
        }

        /**
         * Returns whether another Batch can be batched to this Batch.
         *
         * <p>This is possible if:
         * <li>columns list is super set of other batch's constraint columns;
         *     and
         * <li>both have same Fact Table; and
         * <li>matching columns of this and other batch has the same value; and
         * <li>non matching columns of this batch have ALL VALUES
         * </ul>
         */
        boolean canBatch(Batch other) {
            return hasOverlappingBitKeys(other)
                && constraintsMatch(other)
                && hasSameMeasureList(other)
                && !hasDistinctCountMeasure()
                && !other.hasDistinctCountMeasure()
                && haveSameStarAndAggregation(other)
                && haveSameClosureColumns(other);
        }

        /**
         * Returns whether the constraints on this Batch subsume the constraints
         * on another Batch and therefore the other Batch can be subsumed into
         * this one for GROUPING SETS purposes. Not symmetric.
         *
         * @param other Other batch
         * @return Whether other batch can be subsumed into this one
         */
        private boolean constraintsMatch(Batch other) {
            if (areBothDistinctCountBatches(other)) {
                if (getConstrainedColumnsBitKey().equals(
                        other.getConstrainedColumnsBitKey()))
                {
                    return hasSameCompoundPredicate(other)
                        && haveSameValues(other);
                } else {
                    return hasSameCompoundPredicate(other)
                        || (other.batchKey.getCompoundPredicateList().isEmpty()
                            || equalConstraint(
                                batchKey.getCompoundPredicateList(),
                                other.batchKey.getCompoundPredicateList()))
                        && haveSameValues(other);
                }
            } else {
                return haveSameValues(other);
            }
        }

        private boolean equalConstraint(
            List<StarPredicate> predList1,
            List<StarPredicate> predList2)
        {
            if (predList1.size() != predList2.size()) {
                return false;
            }
            for (int i = 0; i < predList1.size(); i++) {
                StarPredicate pred1 = predList1.get(i);
                StarPredicate pred2 = predList2.get(i);
                if (!pred1.equalConstraint(pred2)) {
                    return false;
                }
            }
            return true;
        }

        private boolean areBothDistinctCountBatches(Batch other) {
            return this.hasDistinctCountMeasure()
                && !this.hasNormalMeasures()
                && other.hasDistinctCountMeasure()
                && !other.hasNormalMeasures();
        }

        private boolean hasNormalMeasures() {
            return getDistinctMeasureCount(measuresList)
                !=  measuresList.size();
        }

        private boolean hasSameMeasureList(Batch other) {
            return this.measuresList.size() == other.measuresList.size()
                   && this.measuresList.containsAll(other.measuresList);
        }

        boolean hasOverlappingBitKeys(Batch other) {
            return getConstrainedColumnsBitKey()
                .isSuperSetOf(other.getConstrainedColumnsBitKey());
        }

        boolean hasDistinctCountMeasure() {
            return getDistinctMeasureCount(measuresList) > 0;
        }

        boolean hasSameCompoundPredicate(Batch other) {
            final StarPredicate starPredicate = compoundPredicate();
            final StarPredicate otherStarPredicate = other.compoundPredicate();
            if (starPredicate == null && otherStarPredicate == null) {
                return true;
            } else if (starPredicate != null && otherStarPredicate != null) {
                return starPredicate.equalConstraint(otherStarPredicate);
            }
            return false;
        }

        private StarPredicate compoundPredicate() {
            StarPredicate predicate = null;
            for (Set<StarColumnPredicate> valueSet : valueSets) {
                StarPredicate orPredicate = null;
                for (StarColumnPredicate starColumnPredicate : valueSet) {
                    if (orPredicate == null) {
                        orPredicate = starColumnPredicate;
                    } else {
                        orPredicate = orPredicate.or(starColumnPredicate);
                    }
                }
                if (predicate == null) {
                    predicate = orPredicate;
                } else {
                    predicate = predicate.and(orPredicate);
                }
            }
            for (StarPredicate starPredicate
                : batchKey.getCompoundPredicateList())
            {
                if (predicate == null) {
                    predicate = starPredicate;
                } else {
                    predicate = predicate.and(starPredicate);
                }
            }
            return predicate;
        }

        boolean haveSameStarAndAggregation(Batch other) {
            boolean rollup[] = {false};
            boolean otherRollup[] = {false};

            boolean hasSameAggregation =
                getAgg(rollup) == other.getAgg(otherRollup);
            boolean hasSameRollupOption = rollup[0] == otherRollup[0];

            boolean hasSameStar = getStar().equals(other.getStar());
            return hasSameStar && hasSameAggregation && hasSameRollupOption;
        }

        /**
         * Returns whether this batch has the same closure columns as another.
         *
         * <p>Ensures that we do not group together a batch that includes a
         * level of a parent-child closure dimension with a batch that does not.
         * It is not safe to roll up from a parent-child closure level; due to
         * multiple accounting, the 'all' level is less than the sum of the
         * members of the closure level.
         *
         * @param other Other batch
         * @return Whether batches have the same closure columns
         */
        boolean haveSameClosureColumns(Batch other) {
            final BitKey cubeClosureColumnBitKey = cube.closureColumnBitKey;
            if (cubeClosureColumnBitKey == null) {
                // Virtual cubes have a null bitkey. For now, punt; should do
                // better.
                return true;
            }
            final BitKey closureColumns =
                this.batchKey.getConstrainedColumnsBitKey()
                    .and(cubeClosureColumnBitKey);
            final BitKey otherClosureColumns =
                other.batchKey.getConstrainedColumnsBitKey()
                    .and(cubeClosureColumnBitKey);
            return closureColumns.equals(otherClosureColumns);
        }

        /**
         * @param rollup Out parameter
         * @return AggStar
         */
        private AggStar getAgg(boolean[] rollup) {
            return AggregationManager.findAgg(
                getStar(),
                getConstrainedColumnsBitKey(),
                makeMeasureBitKey(),
                rollup);
        }

        private BitKey makeMeasureBitKey() {
            BitKey bitKey = getConstrainedColumnsBitKey().emptyCopy();
            for (RolapStar.Measure measure : measuresList) {
                bitKey.set(measure.getBitPosition());
            }
            return bitKey;
        }

        /**
         * Return whether have same values for overlapping columns or
         * has all children for others.
         */
        boolean haveSameValues(
            Batch other)
        {
            for (int j = 0; j < columns.length; j++) {
                boolean isCommonColumn = false;
                for (int i = 0; i < other.columns.length; i++) {
                    if (areSameColumns(other.columns[i], columns[j])) {
                        if (hasSameValues(other.valueSets[i], valueSets[j])) {
                            isCommonColumn = true;
                            break;
                        } else {
                            return false;
                        }
                    }
                }
                if (!isCommonColumn
                    && !hasAllValues(columns[j], valueSets[j]))
                {
                    return false;
                }
            }
            return true;
        }

        private boolean hasAllValues(
            RolapStar.Column column,
            Set<StarColumnPredicate> valueSet)
        {
            return column.getCardinality() == valueSet.size();
        }

        private boolean areSameColumns(
            RolapStar.Column otherColumn,
            RolapStar.Column thisColumn)
        {
            return otherColumn.equals(thisColumn);
        }

        private boolean hasSameValues(
            Set<StarColumnPredicate> otherValueSet,
            Set<StarColumnPredicate> thisValueSet)
        {
            return otherValueSet.equals(thisValueSet);
        }
    }

    private static class CompositeBatchComparator
        implements Comparator<CompositeBatch>
    {
        static final CompositeBatchComparator instance =
            new CompositeBatchComparator();

        public int compare(CompositeBatch o1, CompositeBatch o2) {
            return BatchComparator.instance.compare(
                o1.detailedBatch,
                o2.detailedBatch);
        }
    }

    private static class BatchComparator implements Comparator<Batch> {
        static final BatchComparator instance = new BatchComparator();

        private BatchComparator() {
        }

        public int compare(
            Batch o1, Batch o2)
        {
            if (o1.columns.length != o2.columns.length) {
                return o1.columns.length - o2.columns.length;
            }
            for (int i = 0; i < o1.columns.length; i++) {
                int c = o1.columns[i].getName().compareTo(
                    o2.columns[i].getName());
                if (c != 0) {
                    return c;
                }
            }
            for (int i = 0; i < o1.columns.length; i++) {
                int c = compare(o1.valueSets[i], o2.valueSets[i]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        <T> int compare(Set<T> set1, Set<T> set2) {
            if (set1.size() != set2.size()) {
                return set1.size() - set2.size();
            }
            Iterator<T> iter1 = set1.iterator();
            Iterator<T> iter2 = set2.iterator();
            while (iter1.hasNext()) {
                T v1 = iter1.next();
                T v2 = iter2.next();
                int c = Util.compareKey(v1, v2);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

    private static class ValueColumnConstraintComparator
        implements Comparator<ValueColumnPredicate>
    {
        static final ValueColumnConstraintComparator instance =
            new ValueColumnConstraintComparator();

        private ValueColumnConstraintComparator() {
        }

        public int compare(
            ValueColumnPredicate o1,
            ValueColumnPredicate o2)
        {
            Object v1 = o1.getValue();
            Object v2 = o2.getValue();
            if (v1.getClass() == v2.getClass()
                && v1 instanceof Comparable)
            {
                return ((Comparable) v1).compareTo(v2);
            } else {
                return v1.toString().compareTo(v2.toString());
            }
        }
    }

}

// End FastBatchingCellReader.java
