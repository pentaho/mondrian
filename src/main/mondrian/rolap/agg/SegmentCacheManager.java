/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.olap.CacheControl.CellRegion;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.rolap.cache.*;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.monitor.*;
import mondrian.spi.*;
import mondrian.util.*;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * Active object that maintains the "global cache" (in JVM, but shared between
 * connections using a particular schema) and "external cache" (as implemented
 * by a {@link mondrian.spi.SegmentCache}.
 *
 * <p>Segment states</p>
 *
 * <table>
 *     <tr><th>State</th><th>Meaning</th></tr>
 *     <tr><td>Local</td><td>Initial state of a segment</td></tr>
 * </table>
 *
 * <h2>Decisions to be reviewed</h2>
 *
 * <p>1. Create variant of actor that processes all requests synchronously,
 * and does not need a thread. This would be a more 'embedded' mode of operation
 * (albeit with worse scale-out).</p>
 *
 * <p>2. Move functionality into AggregationManager?</p>
 *
 * <p>3. Delete {@link mondrian.rolap.RolapStar#lookupOrCreateAggregation}
 * and {@link mondrian.rolap.RolapStar#lookupSegment}
 * and {@link mondrian.rolap.RolapStar}.lookupAggregationShared
 * (formerly RolapStar.lookupAggregation).</p>
 *
 *
 *
 *
 * <h2>Moved methods</h2>
 *
 * <p>(Keeping track of where methods came from will make it easier to merge
 * to the mondrian-4 code line.)</p>
 *
 * <p>1. {@link mondrian.rolap.RolapStar#getCellFromCache} moved from
 * {@link Aggregation}.getCellValue</p>
 *
 *
 *
 * <h2>Done</h2>
 *
 * <p>1. Obsolete CountingAggregationManager, and property
 * mondrian.rolap.agg.enableCacheHitCounters.</p>
 *
 * <p>2. AggregationManager becomes non-singleton.</p>
 *
 * <p>3. SegmentCacheWorker methods and segmentCache field become
 * non-static. initCache() is called on construction. SegmentCache is passed
 * into constructor (therefore move ServiceDiscovery into
 * client). AggregationManager (or maybe MondrianServer) is another constructor
 * parameter.</p>
 *
 * <p>5. Move SegmentHeader, SegmentBody, ConstrainedColumn into
 * mondrian.spi. Leave behind dependencies on mondrian.rolap.agg. In particular,
 * put code that converts Segment + SegmentWithData to and from SegmentHeader
 * + SegmentBody (e.g. {@link SegmentHeader}#forSegment) into a utility class.
 * (Do this as CLEANUP, after functionality is complete?)</p>
 *
 * <p>6. Move functionality Aggregation to Segment. Long-term, Aggregation
 * should not be used as a 'gatekeeper' to Segment. Remove Aggregation fields
 * columns and axes.</p>
 *
 * <p>9. Obsolete {@link RolapStar#cacheAggregations}. Similar effect will be
 * achieved by removing the 'jvm cache' from the chain of caches.</p>
 *
 * <p>10. Rename Aggregation.Axis to SegmentAxis.</p>
 *
 * <p>11. Remove Segment.setData and instead split out subclass
 * SegmentWithData. Now segment is immutable. You don't have to wait for its
 * state to change. You wait for a Future&lt;SegmentWithData&gt; to become
 * ready.</p>
 *
 * <p>12. Remove methods: RolapCube.checkAggregateModifications,
 * RolapStar.checkAggregateModifications,
 * RolapSchema.checkAggregateModifications,
 * RolapStar.pushAggregateModificationsToGlobalCache,
 * RolapSchema.pushAggregateModificationsToGlobalCache,
 * RolapCube.pushAggregateModificationsToGlobalCache.</p>
 *
 * <p>13. Add new implementations of Future: CompletedFuture and SlotFuture.</p>
 *
 * <p>14. Remove methods:<p>
 * <ul>
 *
 * <li>Remove {@link SegmentLoader}.loadSegmentsFromCache - creates a
 *   {@link SegmentHeader} that has PRECISELY same specification as the
 *   requested segment, very unlikely to have a hit</li>
 *
 * <li>Remove {@link SegmentLoader}.loadSegmentFromCacheRollup</li>
 *
 * <li>Break up {@link SegmentLoader}.cacheSegmentData, and
 *   place code that is called after a segment has arrived</li>
 *
 * </ul>
 *
 * <p>13. Fix flush. Obsolete {@link Aggregation}.flush, and
 * {@link RolapStar}.flush, which called it.</p>
 *
 * <p>18. {@code SegmentCacheManager#locateHeaderBody} (and maybe other
 * methods) call {@link SegmentCacheWorker#get}, and that's a slow blocking
 * call. Make waits for segment futures should be called from a worker or
 * client, not an agent.</p>
 *
 *
 * <h2>Ideas and tasks</h2>
 *
 * <p>7. RolapStar.localAggregations and .sharedAggregations. Obsolete
 * sharedAggregations.</p>
 *
 * <p>8. Longer term. Move {@link mondrian.rolap.RolapStar.Bar}.segmentRefs to
 * {@link mondrian.server.Execution}. Would it still be thread-local?</p>
 *
 * <p>10. Call
 * {@link mondrian.spi.DataSourceChangeListener#isAggregationChanged}.
 * Previously called from
 * {@link RolapStar}.checkAggregateModifications, now never called.</p>
 *
 * <p>12. We can quickly identify segments affected by a flush using
 * {@link SegmentCacheIndex#intersectRegion}. But then what? Options:</p>
 *
 * <ol>
 *
 * <li>Option #1. Pull them in, trim them, write them out? But: causes
 *     a lot of I/O, and we may never use these
 *     segments. Easiest.</li>
 *
 * <li>Option #2. Mark the segments in the index as needing to be trimmed; trim
 *     them when read, and write out again. But: doesn't propagate to other
 *     nodes.</li>
 *
 * <li>Option #3. (Best?) Write a mapping SegmentHeader->Restrictions into the
 *     cache.  Less I/O than #1. Method
 *     "SegmentCache.addRestriction(SegmentHeader, CacheRegion)"</li>
 *
 * </ol>
 *
 * <p>14. Move {@link AggregationManager#getCellFromCache} somewhere else.
 *   It's concerned with local segments, not the global/external cache.</p>
 *
 * <p>15. Method to convert SegmentHeader + SegmentBody to Segment +
 * SegmentWithData is imperfect. Cannot parse predicates, compound predicates.
 * Need mapping in star to do it properly and efficiently?
 * {@link mondrian.rolap.agg.SegmentBuilder.SegmentConverter} is a hack that
 * can be removed when this is fixed.
 * See {@link SegmentBuilder#toSegment}. Also see #20.</p>
 *
 * <p>17. Revisit the strategy for finding segments that can be copied from
 * global and external cache into local cache. The strategy of sending N
 * {@link CellRequest}s at a time, then executing SQL to fill in the gaps, is
 * flawed. We need to maximize N in order to reduce segment fragmentation, but
 * if too high, we blow memory. BasicQueryTest.testAnalysis is an example of
 * this. Instead, we should send cell-requests in batches (is ~1000 the right
 * size?), identify those that can be answered from global or external cache,
 * return those segments, but not execute SQL until the end of the phase.
 * If so, {@link CellRequestQuantumExceededException} be obsoleted.</p>
 *
 * <p>19. Tracing.
 * a. Remove or re-purpose {@link FastBatchingCellReader#pendingCount};
 * b. Add counter to measure requests satisfied by calling
 * {@link mondrian.rolap.agg.SegmentCacheManager#peek}.</p>
 *
 * <p>20. Obsolete {@link SegmentDataset} and its implementing classes.
 * {@link SegmentWithData} can use {@link SegmentBody} instead. Will save
 * copying.</p>
 *
 * <p>21. Obsolete {@link mondrian.util.CombiningGenerator}.</p>
 *
 * <p>22. {@link SegmentHeader#constrain(mondrian.spi.SegmentColumn[])} is
 * broken for N-dimensional regions where N &gt; 1. Each call currently
 * creates N more 1-dimensional regions, but should create 1 more N-dimensional
 * region. {@link SegmentHeader#excludedRegions} should be a list of
 * {@link SegmentColumn} arrays.</p>
 *
 * <p>23. All code that calls {@link Future#get} should probably handle
 * {@link CancellationException}.</p>
 *
 * <p>24. Obsolete {@link #handler}. Indirection doesn't win anything.</p>
 *
 *
 * @author jhyde
 */
public class SegmentCacheManager {
    private final Handler handler = new Handler();
    private final Actor ACTOR;
    public final Thread thread;

    /**
     * Executor with which to send requests to external caches.
     */
    public final ExecutorService cacheExecutor =
        Util.getExecutorService(
            MondrianProperties.instance()
                .SegmentCacheManagerNumberCacheThreads.get(),
            0, 1,
            "mondrian.rolap.agg.SegmentCacheManager$cacheExecutor",
            new RejectedExecutionHandler() {
                public void rejectedExecution(
                    Runnable r,
                    ThreadPoolExecutor executor)
                {
                    throw MondrianResource.instance()
                        .SegmentCacheLimitReached.ex();
                }
            });

    /**
     * Executor with which to execute SQL requests.
     *
     * <p>TODO: create using factory and/or configuration parameters. Executor
     * should be shared within MondrianServer or target JDBC database.
     */
    public final ExecutorService sqlExecutor =
        Util.getExecutorService(
            MondrianProperties.instance()
                .SegmentCacheManagerNumberSqlThreads.get(),
            0, 1,
            "mondrian.rolap.agg.SegmentCacheManager$sqlExecutor",
            new RejectedExecutionHandler() {
                public void rejectedExecution(
                    Runnable r,
                    ThreadPoolExecutor executor)
                {
                    throw MondrianResource.instance()
                        .SqlQueryLimitReached.ex();
                }
            });

    // NOTE: This list is only mutable for testing purposes. Would rather it
    // were immutable.
    public final List<SegmentCacheWorker> segmentCacheWorkers =
        new CopyOnWriteArrayList<SegmentCacheWorker>();

    public final SegmentCache compositeCache;
    private final SegmentCacheIndexRegistry indexRegistry;

    private static final Logger LOGGER =
        Logger.getLogger(AggregationManager.class);
    private final MondrianServer server;

    public SegmentCacheManager(MondrianServer server) {
        this.server = server;
        ACTOR = new Actor();
        thread = new Thread(
            ACTOR, "mondrian.rolap.agg.SegmentCacheManager$ACTOR");
        thread.setDaemon(true);
        thread.start();

        // Create the index registry.
        this.indexRegistry = new SegmentCacheIndexRegistry();

        // Add a local cache, if needed.
        if (!MondrianProperties.instance().DisableLocalSegmentCache.get()
            && !MondrianProperties.instance().DisableCaching.get())
        {
            final MemorySegmentCache cache = new MemorySegmentCache();
            segmentCacheWorkers.add(
                new SegmentCacheWorker(cache, thread));
        }

        // Add an external cache, if configured.
        final List<SegmentCache> externalCache = SegmentCacheWorker.initCache();
        for (SegmentCache cache : externalCache) {
            // Create a worker for this external cache
            segmentCacheWorkers.add(
                new SegmentCacheWorker(cache, thread));
            // Hook up a listener so it can update
            // the segment index.
            cache.addListener(
                new AsyncCacheListener(this, server));
        }

        compositeCache = new CompositeSegmentCache(segmentCacheWorkers);
    }

    public <T> T execute(Command<T> command) {
        return ACTOR.execute(handler, command);
    }

    public SegmentCacheIndexRegistry getIndexRegistry() {
        return indexRegistry;
    }

    /**
     * Adds a segment to segment index.
     *
     * <p>Called when a SQL statement has finished loading a segment.</p>
     *
     * <p>Does not add the segment to the external cache. That is a potentially
     * long-duration operation, better carried out by a worker.</p>
     *
     * @param header segment header
     * @param body segment body
     */
    public void loadSucceeded(
        RolapStar star,
        SegmentHeader header,
        SegmentBody body)
    {
        final Locus locus = Locus.peek();
        ACTOR.event(
            handler,
            new SegmentLoadSucceededEvent(
                System.currentTimeMillis(),
                locus.getServer().getMonitor(),
                locus.getServer().getId(),
                locus.execution.getMondrianStatement()
                    .getMondrianConnection().getId(),
                locus.execution.getMondrianStatement().getId(),
                locus.execution.getId(),
                star,
                header,
                body));
    }

    /**
     * Informs cache manager that a segment load failed.
     *
     * <p>Called when a SQL statement receives an error while loading a
     * segment.</p>
     *
     * @param header segment header
     * @param throwable Error
     */
    public void loadFailed(
        RolapStar star,
        SegmentHeader header,
        Throwable throwable)
    {
        final Locus locus = Locus.peek();
        ACTOR.event(
            handler,
            new SegmentLoadFailedEvent(
                System.currentTimeMillis(),
                locus.getServer().getMonitor(),
                locus.getServer().getId(),
                locus.execution.getMondrianStatement()
                    .getMondrianConnection().getId(),
                locus.execution.getMondrianStatement().getId(),
                locus.execution.getId(),
                star,
                header,
                throwable));
    }

    /**
     * Removes a segment from segment index.
     *
     * <p>Call is asynchronous. It comes back immediately.</p>
     *
     * <p>Does not remove it from the external cache.</p>
     *
     * @param header segment header
     */
    public void remove(
        RolapStar star,
        SegmentHeader header)
    {
        final Locus locus = Locus.peek();
        ACTOR.event(
            handler,
            new SegmentRemoveEvent(
                System.currentTimeMillis(),
                locus.getServer().getMonitor(),
                locus.getServer().getId(),
                locus.execution.getMondrianStatement()
                    .getMondrianConnection().getId(),
                locus.execution.getMondrianStatement().getId(),
                locus.execution.getId(),
                this,
                star,
                header));
    }

    /**
     * Tells the cache that a segment is newly available in an external cache.
     */
    public void externalSegmentCreated(
        SegmentHeader header,
        MondrianServer server)
    {
        if (MondrianProperties.instance().DisableCaching.get()) {
            // Ignore cache requests.
            return;
        }
        ACTOR.event(
            handler,
            new ExternalSegmentCreatedEvent(
                System.currentTimeMillis(),
                server.getMonitor(),
                server.getId(),
                0,
                0,
                0,
                this,
                header));
    }

    /**
     * Tells the cache that a segment is no longer available in an external
     * cache.
     */
    public void externalSegmentDeleted(
        SegmentHeader header,
        MondrianServer server)
    {
        if (MondrianProperties.instance().DisableCaching.get()) {
            // Ignore cache requests.
            return;
        }
        ACTOR.event(
            handler,
            new ExternalSegmentDeletedEvent(
                System.currentTimeMillis(),
                server.getMonitor(),
                server.getId(),
                0,
                0,
                0,
                this,
                header));
    }

    public void printCacheState(
        CellRegion region,
        PrintWriter pw,
        Locus locus)
    {
        ACTOR.execute(
            handler,
            new PrintCacheStateCommand(region, pw, locus));
    }

    /**
     * Shuts down this cache manager and all active threads and indexes.
     */
    public void shutdown() {
        execute(new ShutdownCommand());
        cacheExecutor.shutdown();
        sqlExecutor.shutdown();
    }

    public SegmentBuilder.SegmentConverter getConverter(
        RolapStar star,
        SegmentHeader header)
    {
        return indexRegistry.getIndex(star)
                .getConverter(
                    header.schemaName,
                    header.schemaChecksum,
                    header.cubeName,
                    header.rolapStarFactTableName,
                    header.measureName,
                    header.compoundPredicates);
    }

    /**
     * Makes a quick request to the aggregation manager to see whether the
     * cell value required by a particular cell request is in external cache.
     *
     * <p>'Quick' is relative. It is an asynchronous request (due to
     * the aggregation manager being an actor) and therefore somewhat slow. If
     * the segment is in cache, will save batching up future requests and
     * re-executing the query. Win should be particularly noticeable for queries
     * running on a populated cache. Without this feature, every query would
     * require at least two iterations.</p>
     *
     * <p>Request does not issue SQL to populate the segment. Nor does it
     * try to find existing segments for rollup. Those operations can wait until
     * next phase.</p>
     *
     * <p>Client is responsible for adding the segment to its private cache.</p>
     *
     * @param request Cell request
     * @return Segment with data, or null if not in cache
     */
    public SegmentWithData peek(final CellRequest request) {
        final SegmentCacheManager.PeekResponse response =
            execute(
                new PeekCommand(request, Locus.peek()));
        for (SegmentHeader header : response.headerMap.keySet()) {
            final SegmentBody body = compositeCache.get(header);
            if (body != null) {
                final SegmentBuilder.SegmentConverter converter =
                    response.converterMap.get(
                        SegmentCacheIndexImpl.makeConverterKey(header));
                if (converter != null) {
                    return converter.convert(header, body);
                }
            }
        }
        for (Map.Entry<SegmentHeader, Future<SegmentBody>> entry
            : response.headerMap.entrySet())
        {
            final Future<SegmentBody> bodyFuture = entry.getValue();
            if (bodyFuture != null) {
                final SegmentBody body =
                    Util.safeGet(
                        bodyFuture,
                        "Waiting for segment to load");
                final SegmentHeader header = entry.getKey();
                final SegmentBuilder.SegmentConverter converter =
                    response.converterMap.get(
                        SegmentCacheIndexImpl.makeConverterKey(header));
                if (converter != null) {
                    return converter.convert(header, body);
                }
            }
        }
        return null;
    }

    /**
     * Visitor for messages (commands and events).
     */
    public interface Visitor {
        void visit(SegmentLoadSucceededEvent event);
        void visit(SegmentLoadFailedEvent event);
        void visit(SegmentRemoveEvent event);
        void visit(ExternalSegmentCreatedEvent event);
        void visit(ExternalSegmentDeletedEvent event);
    }

    private class Handler implements Visitor {
        public void visit(SegmentLoadSucceededEvent event) {
            indexRegistry.getIndex(event.star)
                .loadSucceeded(
                    event.header,
                    event.body);

            event.monitor.sendEvent(
                new CellCacheSegmentCreateEvent(
                    event.timestamp,
                    event.serverId,
                    event.connectionId,
                    event.statementId,
                    event.executionId,
                    event.header.getConstrainedColumns().size(),
                    event.body == null
                        ? 0
                        : event.body.getValueMap().size(),
                    CellCacheSegmentCreateEvent.Source.SQL));
        }

        public void visit(SegmentLoadFailedEvent event) {
            indexRegistry.getIndex(event.star)
                .loadFailed(
                    event.header,
                    event.throwable);
        }

        public void visit(final SegmentRemoveEvent event) {
            indexRegistry.getIndex(event.star)
                .remove(event.header);

            event.monitor.sendEvent(
                new CellCacheSegmentDeleteEvent(
                    event.timestamp,
                    event.serverId,
                    event.connectionId,
                    event.statementId,
                    event.executionId,
                    event.header.getConstrainedColumns().size(),
                    CellCacheEvent.Source.CACHE_CONTROL));

            // Remove the segment from external caches. Use an executor, because
            // it may take some time. We discard the future, because we don't
            // care too much if it fails.
            final Future<?> future = event.cacheMgr.cacheExecutor.submit(
                new Runnable() {
                    public void run() {
                        try {
                            // Note that the SegmentCache API doesn't require
                            // us to verify that the segment exists (by calling
                            // "contains") before we call "remove".
                            event.cacheMgr.compositeCache.remove(event.header);
                        } catch (Throwable e) {
                            LOGGER.warn(
                                "remove header failed: " + event.header,
                                e);
                        }
                    }
                }
            );
            Util.safeGet(future, "SegmentCacheManager.segmentremoved");
        }

        public void visit(ExternalSegmentCreatedEvent event) {
            final SegmentCacheIndex index =
                event.cacheMgr.indexRegistry.getIndex(event.header);
            if (index != null) {
                index.add(event.header, false, null);
                event.monitor.sendEvent(
                    new CellCacheSegmentCreateEvent(
                        event.timestamp,
                        event.serverId,
                        event.connectionId,
                        event.statementId,
                        event.executionId,
                        event.header.getConstrainedColumns().size(),
                        0,
                        CellCacheEvent.Source.EXTERNAL));
            }
        }

        public void visit(ExternalSegmentDeletedEvent event) {
            final SegmentCacheIndex index =
                event.cacheMgr.indexRegistry.getIndex(event.header);
            if (index != null) {
                index.remove(event.header);
                event.monitor.sendEvent(
                    new CellCacheSegmentDeleteEvent(
                        event.timestamp,
                        event.serverId,
                        event.connectionId,
                        event.statementId,
                        event.executionId,
                        event.header.getConstrainedColumns().size(),
                        CellCacheEvent.Source.EXTERNAL));
            }
        }
    }

    interface Message {
    }

    public static interface Command<T> extends Message, Callable<T> {
        Locus getLocus();
    }

    /**
     * Command to flush a particular region from cache.
     */
    public static final class FlushCommand implements Command<FlushResult> {
        private final CellRegion region;
        private final CacheControlImpl cacheControlImpl;
        private final Locus locus;
        private final SegmentCacheManager cacheMgr;

        public FlushCommand(
            Locus locus,
            SegmentCacheManager mgr,
            CellRegion region,
            CacheControlImpl cacheControlImpl)
        {
            this.locus = locus;
            this.cacheMgr = mgr;
            this.region = region;
            this.cacheControlImpl = cacheControlImpl;
        }

        public Locus getLocus() {
            return locus;
        }

        public FlushResult call() throws Exception {
            // For each measure and each star, ask the index
            // which headers intersect.
            final List<SegmentHeader> headers =
                new ArrayList<SegmentHeader>();
            final List<Member> measures =
                CacheControlImpl.findMeasures(region);
            final SegmentColumn[] flushRegion =
                CacheControlImpl.findAxisValues(region);
            final List<RolapStar> starList =
                CacheControlImpl.getStarList(region);

            for (Member member : measures) {
                if (!(member instanceof RolapStoredMeasure)) {
                    continue;
                }
                final RolapStoredMeasure storedMeasure =
                    (RolapStoredMeasure) member;
                final RolapStar star = storedMeasure.getCube().getStar();
                final SegmentCacheIndex index =
                    cacheMgr.indexRegistry.getIndex(star);
                headers.addAll(
                    index.intersectRegion(
                        member.getDimension().getSchema().getName(),
                        ((RolapSchema) member.getDimension().getSchema())
                            .getChecksum(),
                        storedMeasure.getCube().getName(),
                        storedMeasure.getName(),
                        storedMeasure.getCube().getStar()
                            .getFactTable().getAlias(),
                        flushRegion));
                if (cacheControlImpl.isTraceEnabled()) {
                    Collections.sort(
                        headers,
                        new Comparator<SegmentHeader>() {
                            public int compare(
                                SegmentHeader o1,
                                SegmentHeader o2)
                            {
                                return o1.getUniqueID()
                                    .compareTo(o2.getUniqueID());
                            }
                        });
                }
            }

            // If flushRegion is empty, this means we must clear all
            // segments for the region's measures.
            if (flushRegion.length == 0) {
                for (final SegmentHeader header : headers) {
                    for (RolapStar star : starList) {
                        cacheMgr.indexRegistry.getIndex(star).remove(header);
                    }
                    // Remove the segment from external caches. Use an
                    // executor, because it may take some time. We discard
                    // the future, because we don't care too much if it fails.
                    cacheControlImpl.trace(
                        "discard segment - it cannot be constrained and maintain consistency:\n"
                        + header.getDescription());

                    final Future<?> task = cacheMgr.cacheExecutor.submit(
                        new Runnable() {
                            public void run() {
                                try {
                                    // Note that the SegmentCache API doesn't
                                    // require us to verify that the segment
                                    // exists (by calling "contains") before we
                                    // call "remove".
                                    cacheMgr.compositeCache.remove(header);
                                } catch (Throwable e) {
                                    LOGGER.warn(
                                        "remove header failed: " + header,
                                        e);
                                }
                            }
                        });
                    Util.safeGet(task, "SegmentCacheManager.flush");
                }
                return new FlushResult(
                    Collections.<Callable<Boolean>>emptyList());
            }

            // Now we know which headers intersect. For each of them,
            // we append an excluded region.
            //
            // TODO: Optimize the logic here. If a segment is mostly
            // empty, we should trash it completely.
            final List<Callable<Boolean>> callableList =
                new ArrayList<Callable<Boolean>>();
            for (final SegmentHeader header : headers) {
                if (!header.canConstrain(flushRegion)) {
                    // We have to delete that segment altogether.
                    cacheControlImpl.trace(
                        "discard segment - it cannot be constrained and maintain consistency:\n"
                        + header.getDescription());
                    for (RolapStar star : starList) {
                        cacheMgr.indexRegistry.getIndex(star).remove(header);
                    }
                    continue;
                }
                final SegmentHeader newHeader =
                    header.constrain(flushRegion);
                for (final SegmentCacheWorker worker
                    : cacheMgr.segmentCacheWorkers)
                {
                    callableList.add(
                        new Callable<Boolean>() {
                            public Boolean call() throws Exception {
                                boolean existed;
                                if (worker.supportsRichIndex()) {
                                    final SegmentBody sb = worker.get(header);
                                    existed = worker.remove(header);
                                    if (sb != null) {
                                        worker.put(newHeader, sb);
                                    }
                                } else {
                                    // The cache doesn't support rich index. We
                                    // have to clear the segment entirely.
                                    existed = worker.remove(header);
                                }
                                return existed;
                            }
                        });
                }
                for (RolapStar star : starList) {
                    SegmentCacheIndex index =
                        cacheMgr.indexRegistry.getIndex(star);
                    index.remove(header);
                    index.add(newHeader, false, null);
                }
            }

            // Done
            return new FlushResult(callableList);
        }
    }

    private class PrintCacheStateCommand
        implements SegmentCacheManager.Command<Void>
    {
        private final PrintWriter pw;
        private final Locus locus;
        private final CellRegion region;

        public PrintCacheStateCommand(
            CellRegion region,
            PrintWriter pw,
            Locus locus)
        {
            this.region = region;
            this.pw = pw;
            this.locus = locus;
        }

        public Void call() {
            final List<RolapStar> starList =
                CacheControlImpl.getStarList(region);
            Collections.sort(
                starList,
                new Comparator<RolapStar>() {
                    public int compare(RolapStar o1, RolapStar o2) {
                        return o1.getFactTable().getAlias().compareTo(
                            o2.getFactTable().getAlias());
                    }
                });
            for (RolapStar star : starList) {
                indexRegistry.getIndex(star)
                    .printCacheState(pw);
            }
            return null;
        }

        public Locus getLocus() {
            return locus;
        }
    }

    /**
     * Result of a {@link FlushCommand}. Contains a list of tasks that must
     * be executed by the caller (or by an executor) to flush segments from the
     * external cache(s).
     */
    public static class FlushResult {
        public final List<Callable<Boolean>> tasks;

        public FlushResult(List<Callable<Boolean>> tasks) {
            this.tasks = tasks;
        }
    }

    /**
     * Special exception, thrown only by {@link ShutdownCommand}, telling
     * the actor to shut down.
     */
    private static class PleaseShutdownException extends RuntimeException {
        private PleaseShutdownException() {
        }
    }

    private static class ShutdownCommand implements Command<String> {
        public ShutdownCommand() {
        }

        public String call() throws Exception {
            throw new PleaseShutdownException();
        }

        public Locus getLocus() {
            return null;
        }
    }

    private static abstract class Event implements Message {
        /**
         * Dispatches a call to the appropriate {@code visit} method on
         * {@link mondrian.server.monitor.Visitor}.
         *
         * @param visitor Visitor
         */
        public abstract void acceptWithoutResponse(Visitor visitor);
    }

    /**
     * Copy-pasted from {@link mondrian.server.monitor.Monitor}. Consider
     * abstracting common code.
     */
    private static class Actor implements Runnable {

        private final BlockingQueue<Pair<Handler, Message>> eventQueue =
            new ArrayBlockingQueue<Pair<Handler, Message>>(1000);

        private final BlockingHashMap<Command<?>, Pair<Object, Throwable>>
            responseMap =
            new BlockingHashMap<Command<?>, Pair<Object, Throwable>>(1000);

        public void run() {
            try {
                for (;;) {
                    final Pair<Handler, Message> entry = eventQueue.take();
                    final Handler handler = entry.left;
                    final Message message = entry.right;
                    try {
                        // A message is either a command or an event.
                        // A command returns a value that must be read by
                        // the caller.
                        if (message instanceof Command<?>) {
                            Command<?> command = (Command<?>) message;
                            try {
                                Locus.push(command.getLocus());
                                Object result = command.call();
                                responseMap.put(
                                    command,
                                    Pair.of(result, (Throwable) null));
                            } catch (AbortException e) {
                                responseMap.put(
                                    command,
                                    Pair.of(null, (Throwable) e));
                            } catch (PleaseShutdownException e) {
                                responseMap.put(
                                    command,
                                    Pair.of(null, (Throwable) null));
                                return; // exit event loop
                            } catch (Throwable e) {
                                responseMap.put(
                                    command,
                                    Pair.of(null, e));
                            } finally {
                                Locus.pop(command.getLocus());
                            }
                        } else {
                            Event event = (Event) message;
                            event.acceptWithoutResponse(handler);

                            // Broadcast the event to anyone who is interested.
                            RolapUtil.MONITOR_LOGGER.debug(message);
                        }
                    } catch (Throwable e) {
                        // REVIEW: Somewhere better to send it?
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                // REVIEW: Somewhere better to send it?
                e.printStackTrace();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        <T> T execute(Handler handler, Command<T> command) {
            try {
                eventQueue.put(Pair.<Handler, Message>of(handler, command));
            } catch (InterruptedException e) {
                throw Util.newError(e, "Exception while executing " + command);
            }
            try {
                final Pair<Object, Throwable> pair =
                    responseMap.get(command);
                if (pair.right != null) {
                    if (pair.right instanceof RuntimeException) {
                        throw (RuntimeException) pair.right;
                    } else if (pair.right instanceof Error) {
                        throw (Error) pair.right;
                    } else {
                        throw new RuntimeException(pair.right);
                    }
                } else {
                    return (T) pair.left;
                }
            } catch (InterruptedException e) {
                throw Util.newError(e, "Exception while executing " + command);
            }
        }

        public void event(Handler handler, Event event) {
            try {
                eventQueue.put(Pair.<Handler, Message>of(handler, event));
            } catch (InterruptedException e) {
                throw Util.newError(e, "Exception while executing " + event);
            }
        }
    }

    private static class SegmentLoadSucceededEvent extends Event {
        private final SegmentHeader header;
        private final SegmentBody body;
        private final long timestamp;
        private final RolapStar star;
        private final int serverId;
        private final int connectionId;
        private final long statementId;
        private final long executionId;
        private final Monitor monitor;

        public SegmentLoadSucceededEvent(
            long timestamp,
            Monitor monitor,
            int serverId,
            int connectionId,
            long statementId,
            long executionId,
            RolapStar star,
            SegmentHeader header,
            SegmentBody body)
        {
            this.timestamp = timestamp;
            this.monitor = monitor;
            this.serverId = serverId;
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.executionId = executionId;
            assert header != null;
            assert star != null;
            this.star = star;
            this.header = header;
            this.body = body; // may be null
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class SegmentLoadFailedEvent extends Event {
        private final SegmentHeader header;
        private final Throwable throwable;
        private final long timestamp;
        private final RolapStar star;
        private final Monitor monitor;
        private final int serverId;
        private final int connectionId;
        private final long statementId;
        private final long executionId;

        public SegmentLoadFailedEvent(
            long timestamp,
            Monitor monitor,
            int serverId,
            int connectionId,
            long statementId,
            long executionId,
            RolapStar star,
            SegmentHeader header,
            Throwable throwable)
        {
            this.timestamp = timestamp;
            this.monitor = monitor;
            this.serverId = serverId;
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.executionId = executionId;
            this.star = star;
            this.throwable = throwable;
            assert header != null;
            this.header = header;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class SegmentRemoveEvent extends Event {
        private final SegmentHeader header;
        private final long timestamp;
        private final Monitor monitor;
        private final int serverId;
        private final int connectionId;
        private final long statementId;
        private final long executionId;
        private final RolapStar star;
        private final SegmentCacheManager cacheMgr;

        public SegmentRemoveEvent(
            long timestamp,
            Monitor monitor,
            int serverId,
            int connectionId,
            long statementId,
            long executionId,
            SegmentCacheManager cacheMgr,
            RolapStar star,
            SegmentHeader header)
        {
            this.timestamp = timestamp;
            this.monitor = monitor;
            this.serverId = serverId;
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.executionId = executionId;
            this.cacheMgr = cacheMgr;
            this.star = star;
            assert header != null;
            this.header = header;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class ExternalSegmentCreatedEvent extends Event {
        private final SegmentCacheManager cacheMgr;
        private final SegmentHeader header;
        private final long timestamp;
        private final Monitor monitor;
        private final int serverId;
        private final int connectionId;
        private final long statementId;
        private final long executionId;

        public ExternalSegmentCreatedEvent(
            long timestamp,
            Monitor monitor,
            int serverId,
            int connectionId,
            long statementId,
            long executionId,
            SegmentCacheManager cacheMgr,
            SegmentHeader header)
        {
            this.timestamp = timestamp;
            this.monitor = monitor;
            this.serverId = serverId;
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.executionId = executionId;
            assert header != null;
            assert cacheMgr != null;
            this.cacheMgr = cacheMgr;
            this.header = header;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    private static class ExternalSegmentDeletedEvent extends Event {
        private final SegmentCacheManager cacheMgr;
        private final SegmentHeader header;
        private final long timestamp;
        private final Monitor monitor;
        private final int serverId;
        private final int connectionId;
        private final long statementId;
        private final long executionId;

        public ExternalSegmentDeletedEvent(
            long timestamp,
            Monitor monitor,
            int serverId,
            int connectionId,
            long statementId,
            long executionId,
            SegmentCacheManager cacheMgr,
            SegmentHeader header)
        {
            this.timestamp = timestamp;
            this.monitor = monitor;
            this.serverId = serverId;
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.executionId = executionId;
            assert header != null;
            assert cacheMgr != null;
            this.cacheMgr = cacheMgr;
            this.header = header;
        }

        public void acceptWithoutResponse(Visitor visitor) {
            visitor.visit(this);
        }
    }

    /**
     * Implementation of SegmentCacheListener that updates the
     * segment index of its aggregation manager instance when it receives
     * events from its assigned SegmentCache implementation.
     */
    private static class AsyncCacheListener
        implements SegmentCache.SegmentCacheListener
    {
        private final SegmentCacheManager cacheMgr;
        private final MondrianServer server;

        public AsyncCacheListener(
            SegmentCacheManager cacheMgr,
            MondrianServer server)
        {
            this.cacheMgr = cacheMgr;
            this.server = server;
        }

        public void handle(final SegmentCacheEvent e) {
            if (e.isLocal()) {
                return;
            }
            Locus.execute(
                Execution.NONE,
                "AsyncCacheListener.handle",
                new Locus.Action<Void>() {
                    public Void execute() {
                        final SegmentCacheManager.Command<Void> command;
                        final Locus locus = Locus.peek();
                        switch (e.getEventType()) {
                        case ENTRY_CREATED:
                            command =
                                new Command<Void>() {
                                    public Void call() {
                                        cacheMgr.externalSegmentCreated(
                                            e.getSource(),
                                            server);
                                        return null;
                                    }
                                    public Locus getLocus() {
                                        return locus;
                                    }
                                };
                            break;
                        case ENTRY_DELETED:
                            command =
                                new Command<Void>() {
                                    public Void call() {
                                        cacheMgr.externalSegmentDeleted(
                                            e.getSource(),
                                            server);
                                        return null;
                                    }
                                    public Locus getLocus() {
                                        return locus;
                                    }
                                };
                            break;
                        default:
                            throw new UnsupportedOperationException();
                        }
                        cacheMgr.execute(command);
                        return null;
                    }
                });
        }
    }

    /**
     * Makes a collection of {@link SegmentCacheWorker} objects (each of which
     * is backed by a {@link SegmentCache} appear to be a SegmentCache.
     *
     * <p>For most operations, it is easier to operate on a single cache.
     * It is usually clear whether operations should quit when finding the first
     * match, or to operate on all workers. (For example, {@link #remove} tries
     * to remove the segment header from all workers, and returns whether it
     * was removed from any of them.) This class just does what seems
     * most typical. If you want another behavior for a particular operation,
     * operate on the workers directly.</p>
     */
    static class CompositeSegmentCache implements SegmentCache {
        final List<SegmentCacheWorker> workers;

        public CompositeSegmentCache(List<SegmentCacheWorker> workers) {
            this.workers = workers;
        }

        public SegmentBody get(SegmentHeader header) {
            for (SegmentCacheWorker worker : workers) {
                final SegmentBody body = worker.get(header);
                if (body != null) {
                    return body;
                }
            }
            return null;
        }

        public List<SegmentHeader> getSegmentHeaders() {
            if (MondrianProperties.instance().DisableCaching.get()) {
                return Collections.emptyList();
            }
            // Special case 0 and 1 workers, for which the 'union' operation
            // is trivial.
            switch (workers.size()) {
            case 0:
                return Collections.emptyList();
            case 1:
                return workers.get(0).getSegmentHeaders();
            default:
                final List<SegmentHeader> list = new ArrayList<SegmentHeader>();
                final Set<SegmentHeader> set = new HashSet<SegmentHeader>();
                for (SegmentCacheWorker worker : workers) {
                    for (SegmentHeader header : worker.getSegmentHeaders()) {
                        if (set.add(header)) {
                            list.add(header);
                        }
                    }
                }
                return list;
            }
        }

        public boolean put(SegmentHeader header, SegmentBody body) {
            if (MondrianProperties.instance().DisableCaching.get()) {
                return true;
            }
            for (SegmentCacheWorker worker : workers) {
                worker.put(header, body);
            }
            return true;
        }

        public boolean remove(SegmentHeader header) {
            boolean result = false;
            for (SegmentCacheWorker worker : workers) {
                if (worker.remove(header)) {
                    result = true;
                }
            }
            return result;
        }

        public void tearDown() {
            for (SegmentCacheWorker worker : workers) {
                worker.shutdown();
            }
        }

        public void addListener(SegmentCacheListener listener) {
            for (SegmentCacheWorker worker : workers) {
                worker.cache.addListener(listener);
            }
        }

        public void removeListener(SegmentCacheListener listener) {
            for (SegmentCacheWorker worker : workers) {
                worker.cache.removeListener(listener);
            }
        }

        public boolean supportsRichIndex() {
            for (SegmentCacheWorker worker : workers) {
                if (!worker.supportsRichIndex()) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Locates segments in the cache that satisfy a given request.
     *
     * <p>The result consists of (a) a list of segment headers, (b) a list
     * of futures for segment bodies that are currently being loaded, (c)
     * converters to convert headers into {@link SegmentWithData}.</p>
     *
     * <p>For (a), the client should call the cache to get the body for each
     * segment header; it is possible that there is no body in the cache.
     * For (b), the client will have to wait for the segment to arrive.</p>
     */
    private class PeekCommand
        implements SegmentCacheManager.Command<PeekResponse>
    {
        private final CellRequest request;
        private final Locus locus;

        /**
         * Creates a PeekCommand.
         *
         * @param request Cell request
         * @param locus Locus
         */
        public PeekCommand(
            CellRequest request,
            Locus locus)
        {
            this.request = request;
            this.locus = locus;
        }

        public PeekResponse call() {
            final RolapStar.Measure measure = request.getMeasure();
            final RolapStar star = measure.getStar();
            final RolapSchema schema = star.getSchema();
            final AggregationKey key = new AggregationKey(request);
            final List<SegmentHeader> headers =
                indexRegistry.getIndex(star)
                    .locate(
                        schema.getName(),
                        schema.getChecksum(),
                        measure.getCubeName(),
                        measure.getName(),
                        star.getFactTable().getAlias(),
                        request.getConstrainedColumnsBitKey(),
                        request.getMappedCellValues(),
                        AggregationKey.getCompoundPredicateStringList(
                            star,
                            key.getCompoundPredicateList()));

            final Map<SegmentHeader, Future<SegmentBody>> headerMap =
                new HashMap<SegmentHeader, Future<SegmentBody>>();
            final Map<List, SegmentBuilder.SegmentConverter> converterMap =
                new HashMap<List, SegmentBuilder.SegmentConverter>();

            // Is there a pending segment? (A segment that has been created and
            // is loading via SQL.)
            for (final SegmentHeader header : headers) {
                final Future<SegmentBody> bodyFuture =
                    indexRegistry.getIndex(star)
                        .getFuture(locus.execution, header);
                if (bodyFuture != null) {
                    // Check if the DataSourceChangeListener wants us to clear
                    // the current segment
                    if (star.getChangeListener() != null
                        && star.getChangeListener().isAggregationChanged(key))
                    {
                        // We can't satisfy this request, and we must clear the
                        // data from our cache. This must be in sync with the
                        // actor thread to maintain consistency.
                        indexRegistry.getIndex(star).remove(header);
                        Util.safeGet(
                            cacheExecutor.submit(
                                new Runnable() {
                                    public void run() {
                                        try {
                                            compositeCache.remove(header);
                                        } catch (Throwable e) {
                                            LOGGER.warn(
                                                "remove header failed: "
                                                + header,
                                                e);
                                        }
                                    }
                                }),
                            "SegmentCacheManager.peek");
                        continue;
                    }
                    converterMap.put(
                        SegmentCacheIndexImpl.makeConverterKey(header),
                        getConverter(star, header));
                    headerMap.put(
                        header, bodyFuture);
                }
            }

            return new PeekResponse(headerMap, converterMap);
        }

        public Locus getLocus() {
            return locus;
        }
    }

    private static class PeekResponse {
        public final Map<SegmentHeader, Future<SegmentBody>> headerMap;
        public final Map<List, SegmentBuilder.SegmentConverter> converterMap;

        public PeekResponse(
            Map<SegmentHeader, Future<SegmentBody>> headerMap,
            Map<List, SegmentBuilder.SegmentConverter> converterMap)
        {
            this.headerMap = headerMap;
            this.converterMap = converterMap;
        }
    }

    /**
     * Registry of all the indexes that were created for this
     * cache manager, per {@link RolapStar}.
     */
    public class SegmentCacheIndexRegistry {
        private final Map<RolapStar, SegmentCacheIndex> indexes =
            new WeakHashMap<RolapStar, SegmentCacheIndex>();
        /**
         * Returns the {@link SegmentCacheIndex} for a given
         * {@link RolapStar}.
         */
        public SegmentCacheIndex getIndex(RolapStar star) {
            if (!indexes.containsKey(star)) {
                indexes.put(star, new SegmentCacheIndexImpl(thread));
            }
            return indexes.get(star);
        }
        /**
         * Returns the {@link SegmentCacheIndex} for a given
         * {@link SegmentHeader}.
         */
        private SegmentCacheIndex getIndex(
            SegmentHeader header)
        {
            // First we check the indexes that already exist.
            // This is fast.
            for (Entry<RolapStar, SegmentCacheIndex> entry
                : indexes.entrySet())
            {
                final String factTableName =
                    entry.getKey().getFactTable().getTableName();
                final ByteString schemaChecksum =
                    entry.getKey().getSchema().getChecksum();
                if (!factTableName.equals(header.rolapStarFactTableName)) {
                    continue;
                }
                if (!schemaChecksum.equals(header.schemaChecksum)) {
                    continue;
                }
                return entry.getValue();
            }
            // The index doesn't exist. Let's create it.
            for (RolapSchema schema : RolapSchema.getRolapSchemas()) {
                if (!schema.getChecksum().equals(header.schemaChecksum)) {
                    continue;
                }
                // We have a schema match.
                RolapStar star =
                    schema.getStar(header.rolapStarFactTableName);
                return getIndex(star);
            }
            return null;
        }
        public void cancelExecutionSegments(Execution exec) {
            for (SegmentCacheIndex index : indexes.values()) {
                index.cancel(exec);
            }
        }
    }

    /**
     * Exception which someone can throw to indicate to the Actor that
     * whatever it was doing is not needed anymore. Won't trigger any output
     * to the logs.
     *
     * <p>If your {@link Command} throws this, it will be sent back at you.
     * You must handle it.
     */
    public static final class AbortException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    };
}

// End SegmentCacheManager.java
