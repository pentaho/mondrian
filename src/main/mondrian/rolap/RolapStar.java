/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.Aggregation;
import mondrian.rolap.agg.AggregationKey;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.DataSourceChangeListener;
import mondrian.spi.Dialect;
import mondrian.util.Bug;
import org.apache.log4j.Logger;
import org.eigenbase.util.property.Property;
import org.eigenbase.util.property.TriggerBase;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.*;
import java.util.*;

/**
 * A <code>RolapStar</code> is a star schema. It is the means to read cell
 * values.
 *
 * <p>todo: put this in package which specicializes in relational aggregation,
 * doesn't know anything about hierarchies etc.
 *
 * @author jhyde
 * @since 12 August, 2001
 * @version $Id$
 */
public class RolapStar {
    private static final Logger LOGGER = Logger.getLogger(RolapStar.class);

    /**
     * Controls the aggregate data cache for all RolapStars.
     * An administrator or tester might selectively enable or
     * disable in memory caching to allow direct measurement of database
     * performance.
     */
    private static boolean disableCaching =
        MondrianProperties.instance().DisableCaching.get();

    static {
        // Trigger is used to lookup and change the value of the
        // variable that controls aggregate data caching
        // Using a trigger means we don't have to look up the property eveytime.
        MondrianProperties.instance().DisableCaching.addTrigger(
            new TriggerBase(true) {
                public void execute(Property property, String value) {
                    disableCaching = property.booleanValue();
                    // must flush all caches
                    if (disableCaching) {
                        // REVIEW: could replace following code with call to
                        // CacheControl.flush(CellRegion)
                        for (Iterator<RolapSchema> itSchemas =
                            RolapSchema.getRolapSchemas();
                             itSchemas.hasNext();)
                        {
                            RolapSchema schema1 = itSchemas.next();
                            for (RolapStar star : schema1.getStars()) {
                                star.clearCachedAggregations(true);
                            }
                        }
                    }
                }
            }
       );
    }


    private final RolapSchema schema;

    // not final for test purposes
    private DataSource dataSource;

    private final Table factTable;

    /** Holds all global aggregations of this star. */
    private final Map<AggregationKey, Aggregation> sharedAggregations;

    /** Holds all thread-local aggregations of this star. */
    private final ThreadLocal<Map<AggregationKey, Aggregation>>
        localAggregations =
            new ThreadLocal<Map<AggregationKey, Aggregation>>() {
            protected Map<AggregationKey, Aggregation> initialValue() {
                return new HashMap<AggregationKey, Aggregation>();
            }
        };

    /**
     * Holds all pending aggregations of this star that are waiting to
     * be pushed into the global cache.  They cannot be pushed yet, because
     * the aggregates in question are currently in use by other threads.
     */
    private final Map<AggregationKey, Aggregation> pendingAggregations;

    /**
     * Holds all requests for aggregations.
     */
    private final List<AggregationKey> aggregationRequests;

    /**
     * Holds all requests of aggregations per thread.
     */
    private final ThreadLocal<List<AggregationKey>>
        localAggregationRequests =
            new ThreadLocal<List<AggregationKey>>() {
            protected List<AggregationKey> initialValue() {
                return new ArrayList<AggregationKey>();
            }
        };

    /**
     * Number of columns (column and columnName).
     */
    private int columnCount;

    private final Dialect sqlQueryDialect;

    /**
     * If true, then database aggregation information is cached, otherwise
     * it is flushed after each query.
     */
    private boolean cacheAggregations;

    /**
     * Partially ordered list of AggStars associated with this RolapStar's fact
     * table
     */
    private List<AggStar> aggStars;

    private DataSourceChangeListener changeListener;

    // temporary model, should eventually use RolapStar.Table and
    // RolapStar.Column
    private StarNetworkNode factNode;
    private Map<String, StarNetworkNode> nodeLookup =
        new HashMap<String, StarNetworkNode>();

    private final List<Column> columnList = new ArrayList<Column>();

    /**
     * Creates a RolapStar. Please use
     * {@link RolapSchema.RolapStarRegistry#getOrCreateStar} to create a
     * {@link RolapStar}.
     */
    RolapStar(
        final RolapSchema schema,
        final DataSource dataSource,
        final MondrianDef.Relation fact)
    {
        this.cacheAggregations = true;
        this.schema = schema;
        this.dataSource = dataSource;
        this.factTable = new RolapStar.Table(this, fact, null, null);

        // phase out and replace with Table, Column network
        this.factNode =
            new StarNetworkNode(null, factTable.alias, null, null, null);

        this.sharedAggregations = new HashMap<AggregationKey, Aggregation>();

        this.pendingAggregations = new HashMap<AggregationKey, Aggregation>();

        this.aggregationRequests = new ArrayList<AggregationKey>();

        clearAggStarList();

        this.sqlQueryDialect = schema.getDialect();

        this.changeListener = schema.getDataSourceChangeListener();
    }

    private static class StarNetworkNode {
        private StarNetworkNode parent;
        private MondrianDef.Relation origRel;
        private String foreignKey;
        private String joinKey;

        private StarNetworkNode(
            StarNetworkNode parent,
            String alias,
            MondrianDef.Relation origRel,
            String foreignKey,
            String joinKey)
        {
            this.parent = parent;
            this.origRel = origRel;
            this.foreignKey = foreignKey;
            this.joinKey = joinKey;
        }

        private boolean isCompatible(
            StarNetworkNode compatibleParent,
            MondrianDef.Relation rel,
            String compatibleForeignKey,
            String compatibleJoinKey)
        {
            return parent == compatibleParent
                && origRel.getClass().equals(rel.getClass())
                && foreignKey.equals(compatibleForeignKey)
                && joinKey.equals(compatibleJoinKey);
        }
    }

    private MondrianDef.RelationOrJoin cloneRelation(
        MondrianDef.Relation rel,
        String possibleName)
    {
        if (rel instanceof MondrianDef.Table) {
            MondrianDef.Table tbl = (MondrianDef.Table)rel;
            return new MondrianDef.Table(
                tbl.schema,
                tbl.name,
                possibleName,
                tbl.tableHints);
        } else if (rel instanceof MondrianDef.View) {
            MondrianDef.View view = (MondrianDef.View)rel;
            MondrianDef.View newView = new MondrianDef.View(view);
            newView.alias = possibleName;
            return newView;
        } else if (rel instanceof MondrianDef.InlineTable) {
            MondrianDef.InlineTable inlineTable =
                (MondrianDef.InlineTable) rel;
            MondrianDef.InlineTable newInlineTable =
                new MondrianDef.InlineTable(inlineTable);
            newInlineTable.alias = possibleName;
            return newInlineTable;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Generates a unique relational join to the fact table via re-aliasing
     * MondrianDef.Relations
     *
     * currently called in the RolapCubeHierarchy constructor.  This should
     * eventually be phased out and replaced with RolapStar.Table and
     * RolapStar.Column references
     *
     * @param rel the relation needing uniqueness
     * @param factForeignKey the foreign key of the fact table
     * @param primaryKey the join key of the relation
     * @param primaryKeyTable the join table of the relation
     * @return if necessary a new relation that has been re-aliased
     */
    public MondrianDef.RelationOrJoin getUniqueRelation(
        MondrianDef.RelationOrJoin rel,
        String factForeignKey,
        String primaryKey,
        String primaryKeyTable)
    {
        return getUniqueRelation(
            factNode, rel, factForeignKey, primaryKey, primaryKeyTable);
    }

    private MondrianDef.RelationOrJoin getUniqueRelation(
        StarNetworkNode parent,
        MondrianDef.RelationOrJoin relOrJoin,
        String foreignKey,
        String joinKey,
        String joinKeyTable)
    {
        if (relOrJoin == null) {
            return null;
        } else if (relOrJoin instanceof MondrianDef.Relation) {
            int val = 0;
            MondrianDef.Relation rel =
                (MondrianDef.Relation) relOrJoin;
            String newAlias =
                joinKeyTable != null ? joinKeyTable : rel.getAlias();
            while (true) {
                StarNetworkNode node = nodeLookup.get(newAlias);
                if (node == null) {
                    if (val != 0) {
                        rel = (MondrianDef.Relation)
                            cloneRelation(rel, newAlias);
                    }
                    node =
                        new StarNetworkNode(
                            parent, newAlias, rel, foreignKey, joinKey);
                    nodeLookup.put(newAlias, node);
                    return rel;
                } else if (node.isCompatible(
                    parent, rel, foreignKey, joinKey))
                {
                    return node.origRel;
                }
                newAlias = rel.getAlias() + "_" + (++val);
            }
        } else if (relOrJoin instanceof MondrianDef.Join) {
            // determine if the join starts from the left or right side
            MondrianDef.Join join = (MondrianDef.Join)relOrJoin;

            if (join.left instanceof MondrianDef.Join) {
                throw MondrianResource.instance().IllegalLeftDeepJoin.ex();
            }
            MondrianDef.RelationOrJoin left;
            MondrianDef.RelationOrJoin right;
            if (join.getLeftAlias().equals(joinKeyTable)) {
                // first manage left then right
                left =
                    getUniqueRelation(
                        parent, join.left, foreignKey,
                        joinKey, joinKeyTable);
                parent = nodeLookup.get(
                    ((MondrianDef.Relation) left).getAlias());
                right =
                    getUniqueRelation(
                        parent, join.right, join.leftKey,
                        join.rightKey, join.getRightAlias());
            } else if (join.getRightAlias().equals(joinKeyTable)) {
                // right side must equal
                right =
                    getUniqueRelation(
                        parent, join.right, foreignKey,
                        joinKey, joinKeyTable);
                parent = nodeLookup.get(
                    ((MondrianDef.Relation) right).getAlias());
                left =
                    getUniqueRelation(
                        parent, join.left, join.rightKey,
                        join.leftKey, join.getLeftAlias());
            } else {
                throw new MondrianException(
                    "failed to match primary key table to join tables");
            }

            if (join.left != left || join.right != right) {
                join =
                    new MondrianDef.Join(
                        left instanceof MondrianDef.Relation
                            ? ((MondrianDef.Relation) left).getAlias()
                            : null,
                        join.leftKey,
                        left,
                        right instanceof MondrianDef.Relation
                            ? ((MondrianDef.Relation) right).getAlias()
                            : null,
                        join.rightKey,
                        right);
            }
            return join;
        }
        return null;
    }

    /**
     * Returns this RolapStar's column count. After a star has been created with
     * all of its columns, this is the number of columns in the star.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     * This is used by the {@link Column} constructor to get a unique id (per
     * its parent {@link RolapStar}).
     */
    private int nextColumnCount() {
        return columnCount++;
    }

    /**
     * This is used to decrement the column counter and is used if a newly
     * created column is found to already exist.
     */
    private int decrementColumnCount() {
        return columnCount--;
    }

    /**
     * This is a place holder in case in the future we wish to be able to
     * reload aggregates. In that case, if aggregates had already been loaded,
     * i.e., this star has some aggstars, then those aggstars are cleared.
     */
    public void prepareToLoadAggregates() {
        aggStars = Collections.emptyList();
    }

    /**
     * Adds an {@link AggStar} to this star.
     *
     * <p>Internally the AggStars are added in sort order, smallest row count
     * to biggest, so that the most efficient AggStar is encountered first;
     * ties do not matter.
     */
    public void addAggStar(AggStar aggStar) {
        if (aggStars == Collections.EMPTY_LIST) {
            // if this is NOT a LinkedList, then the insertion time is longer.
            aggStars = new LinkedList<AggStar>();
        }

        // Add it before the first AggStar which is larger, if there is one.
        int size = aggStar.getSize();
        ListIterator<AggStar> lit = aggStars.listIterator();
        while (lit.hasNext()) {
            AggStar as = lit.next();
            if (as.getSize() >= size) {
                lit.previous();
                lit.add(aggStar);
                return;
            }
        }

        // There is no larger star. Add at the end of the list.
        aggStars.add(aggStar);
    }

    /**
     * Set the agg star list to empty.
     */
    void clearAggStarList() {
        aggStars = Collections.emptyList();
    }

    /**
     * Reorder the list of aggregate stars. This should be called if the
     * algorithm used to order the AggStars has been changed.
     */
    public void reOrderAggStarList() {
        // the order of these two lines is important
        List<AggStar> l = aggStars;
        clearAggStarList();

        for (AggStar aggStar : l) {
            addAggStar(aggStar);
        }
    }

    /**
     * Returns this RolapStar's aggregate table AggStars, ordered in ascending
     * order of size.
     */
    public List<AggStar> getAggStars() {
        return aggStars;
    }

    /**
     * Returns the fact table at the center of this RolapStar.
     *
     * @return fact table
     */
    public Table getFactTable() {
        return factTable;
    }

    /**
     * Clones an existing SqlQuery to create a new one (this cloning creates one
     * with an empty sql query).
     */
    public SqlQuery getSqlQuery() {
        return new SqlQuery(getSqlQueryDialect());
    }

    /**
     * Returns this RolapStar's SQL dialect.
     */
    public Dialect getSqlQueryDialect() {
        return sqlQueryDialect;
    }

    /**
     * Sets whether to cache database aggregation information; if false, cache
     * is flushed after each query.
     *
     * <p>This method is called only by the RolapCube and is only called if
     * caching is to be turned off. Note that the same RolapStar can be
     * associated with more than on RolapCube. If any one of those cubes has
     * caching turned off, then caching is turned off for all of them.
     *
     * @param cacheAggregations Whether to cache database aggregation
     */
    void setCacheAggregations(boolean cacheAggregations) {
        // this can only change from true to false
        this.cacheAggregations = cacheAggregations;
        clearCachedAggregations(false);
    }

    /**
     * Returns whether the this RolapStar cache aggregates.
     *
     * @see #setCacheAggregations(boolean)
     */
    boolean isCacheAggregations() {
        return this.cacheAggregations;
    }

    /**
     * Clears the aggregate cache. This only does something if aggregate caching
     * is disabled (see {@link #setCacheAggregations(boolean)}).
     *
     * @param forced If true, clears cached aggregations regardless of any other
     *   settings.  If false, clears only cache from the current thread
     */
    void clearCachedAggregations(boolean forced) {
        if (forced || !cacheAggregations || RolapStar.disableCaching) {
            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("RolapStar.clearCachedAggregations: schema=");
                buf.append(schema.getName());
                buf.append(", star=");
                buf.append(getFactTable().getAlias());
                LOGGER.debug(buf.toString());
            }

            if (forced) {
                synchronized (sharedAggregations) {
                    sharedAggregations.clear();
                }
                localAggregations.get().clear();
            } else {
                // Only clear aggregation cache for the currect thread context.
                localAggregations.get().clear();
            }
        }
    }

    /**
     * Looks up an aggregation or creates one if it does not exist in an
     * atomic (synchronized) operation.
     *
     * <p>When a new aggregation is created, it is marked as thread local.
     *
     * @param aggregationKey this is the contrained column bitkey
     */
    public Aggregation lookupOrCreateAggregation(
        AggregationKey aggregationKey)
    {
        Aggregation aggregation = lookupAggregation(aggregationKey);

        if (aggregation == null) {
            aggregation = new Aggregation(aggregationKey);

            this.localAggregations.get().put(aggregationKey, aggregation);

            // Let the change listener get the opportunity to register the
            // first time the aggregation is used
            if ((this.cacheAggregations) && (!RolapStar.disableCaching)) {
                if (changeListener != null) {
                    changeListener.isAggregationChanged(aggregation);
                }
            }
        }
        return aggregation;
    }

    /**
     * Looks for an existing aggregation over a given set of columns, or
     * returns <code>null</code> if there is none.
     *
     * <p>Thread local cache is taken first.
     *
     * <p>Must be called from synchronized context.
     */
    public Aggregation lookupAggregation(AggregationKey aggregationKey) {
        // First try thread local cache
        Aggregation aggregation = localAggregations.get().get(aggregationKey);
        if (aggregation != null) {
            return aggregation;
        }

        if (cacheAggregations && !RolapStar.disableCaching) {
            // Look in global cache
            synchronized (sharedAggregations) {
                aggregation = sharedAggregations.get(aggregationKey);
                if (aggregation != null) {
                    // Keep track of global aggregates that a query is using
                    recordAggregationRequest(aggregationKey);
                }
            }
        }

        return aggregation;
    }

    /**
     * Checks whether an aggregation has changed since the last the time
     * loaded.
     *
     * <p>If so, a new thread local aggregation will be made and added after
     * the query has finished.
     *
     * <p>This method should be called before a query is executed and afterwards
     * the function {@link #pushAggregateModificationsToGlobalCache()} should
     * be called.
     */
    public void checkAggregateModifications() {
        // Clear own aggregation requests at the beginning of a query
        // made by request to materialize results after RolapResult constructor
        // is finished
        clearAggregationRequests();

        if (changeListener != null) {
            if (cacheAggregations && !RolapStar.disableCaching) {
                synchronized (sharedAggregations) {
                    for (Map.Entry<AggregationKey, Aggregation> e
                        : sharedAggregations.entrySet())
                    {
                        AggregationKey aggregationKey = e.getKey();

                        Aggregation aggregation = e.getValue();
                        if (changeListener.isAggregationChanged(aggregation)) {
                            // Create new thread local aggregation
                            // This thread will renew aggregations
                            // And these will be checked in if all queries
                            // that are currently using these aggregates
                            // are finished
                            aggregation = new Aggregation(aggregationKey);

                            localAggregations.get().put(
                                aggregationKey, aggregation);
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks whether changed modifications may be pushed into global cache.
     *
     * <p>The method checks whether there are other running queries that are
     * using the requested modifications.  If this is the case, modifications
     * are not pushed yet.
     */
    public void pushAggregateModificationsToGlobalCache() {
        // Need synchronized access to both aggregationRequests as to
        // aggregations, synchronize this instead
        synchronized (this) {
            if (cacheAggregations && !RolapStar.disableCaching) {
                // Push pending modifications other thread could not push
                // to global cache, because it was in use
                Iterator<Map.Entry<AggregationKey, Aggregation>>
                    it = pendingAggregations.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<AggregationKey, Aggregation> e = it.next();
                    AggregationKey aggregationKey = e.getKey();
                    Aggregation aggregation = e.getValue();
                    // In case this aggregation is not requested by anyone
                    // this aggregation may be pushed into global cache
                    // otherwise put it in pending cache, that will be pushed
                    // when another query finishes
                    if (!isAggregationRequested(aggregationKey)) {
                        pushAggregateModification(
                            aggregationKey, aggregation, sharedAggregations);
                        it.remove();
                    }
                }
                // Push thread local modifications
                it = localAggregations.get().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<AggregationKey, Aggregation> e = it.next();
                    AggregationKey aggregationKey = e.getKey();
                    Aggregation aggregation = e.getValue();
                    // In case this aggregation is not requested by anyone
                    // this aggregation may be pushed into global cache
                    // otherwise put it in pending cache, that will be pushed
                    // when another query finishes
                    Map<AggregationKey, Aggregation> targetMap;
                    if (isAggregationRequested(aggregationKey)) {
                        targetMap = pendingAggregations;
                    } else {
                        targetMap = sharedAggregations;
                    }
                    pushAggregateModification(
                        aggregationKey, aggregation, targetMap);
                }
                localAggregations.get().clear();
            }
            // Clear own aggregation requests
            clearAggregationRequests();
        }
    }

    /**
     * Pushes aggregations in destination aggregations, replacing older
     * entries.
     */
    private void pushAggregateModification(
        AggregationKey localAggregationKey,
        Aggregation localAggregation,
        Map<AggregationKey, Aggregation> destAggregations)
    {
        if (cacheAggregations && !RolapStar.disableCaching) {
            synchronized (destAggregations) {
                boolean found = false;
                Iterator<Map.Entry<AggregationKey, Aggregation>>
                        it = destAggregations.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<AggregationKey, Aggregation> e =
                        it.next();
                    AggregationKey aggregationKey = e.getKey();
                    Aggregation aggregation = e.getValue();

                    if (localAggregationKey.equals(aggregationKey)) {
                        if (localAggregation.getCreationTimestamp().after(
                            aggregation.getCreationTimestamp()))
                        {
                            it.remove();
                        } else {
                            // Entry is newer, do not replace
                            found = true;
                        }
                        break;
                    }
                }
                if (!found) {
                    destAggregations.put(localAggregationKey, localAggregation);
                }
            }
        }
    }

    /**
     * Records global cache requests per thread.
     */
    private void recordAggregationRequest(AggregationKey aggregationKey) {
        if (!localAggregationRequests.get().contains(aggregationKey)) {
            synchronized (aggregationRequests) {
                aggregationRequests.add(aggregationKey);
            }
            // Store own request for cleanup afterwards
            localAggregationRequests.get().add(aggregationKey);
        }
    }

    /**
     * Checks whether an aggregation is requested by another thread.
     */
    private boolean isAggregationRequested(AggregationKey aggregationKey) {
        synchronized (aggregationRequests) {
            return aggregationRequests.contains(aggregationKey);
        }
    }

    /**
     * Clears the aggregation requests created by the current thread.
     */
    private void clearAggregationRequests() {
        synchronized (aggregationRequests) {
            if (localAggregationRequests.get().isEmpty()) {
                return;
            }
            // Build a set of requests for efficient probing. Negligible cost
            // if this thread's localAggregationRequests is small, but avoids a
            // quadratic algorithm if it is large.
            Set<AggregationKey> localAggregationRequestSet =
                new HashSet<AggregationKey>(localAggregationRequests.get());
            Iterator<AggregationKey> iter = aggregationRequests.iterator();
            while (iter.hasNext()) {
                AggregationKey aggregationKey = iter.next();
                if (localAggregationRequestSet.contains(aggregationKey)) {
                    iter.remove();
                    // Make sure that bitKey is not removed more than once:
                    // other occurrences might exist for other threads.
                    localAggregationRequestSet.remove(aggregationKey);
                    if (localAggregationRequestSet.isEmpty()) {
                        // Nothing further to do
                        break;
                    }
                }
            }
            localAggregationRequests.get().clear();
        }
    }

    /** For testing purposes only.  */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Returns the DataSource used to connect to the underlying DBMS.
     *
     * @return DataSource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Retrieves the {@link RolapStar.Measure} in which a measure is stored.
     */
    public static Measure getStarMeasure(Member member) {
        return (Measure) ((RolapStoredMeasure) member).getStarMeasure();
    }

    /**
     * Retrieves a named column, returns null if not found.
     */
    public Column[] lookupColumns(String tableAlias, String columnName) {
        final Table table = factTable.findDescendant(tableAlias);
        return (table == null) ? null : table.lookupColumns(columnName);
    }

    /**
     * This is used by TestAggregationManager only.
     */
    public Column lookupColumn(String tableAlias, String columnName) {
        final Table table = factTable.findDescendant(tableAlias);
        return (table == null) ? null : table.lookupColumn(columnName);
    }

    public BitKey getBitKey(String[] tableAlias, String[] columnName) {
        BitKey bitKey = BitKey.Factory.makeBitKey(getColumnCount());
        Column starColumn;
        for (int i = 0; i < tableAlias.length; i ++) {
            starColumn = lookupColumn(tableAlias[i], columnName[i]);
            if (starColumn != null) {
                bitKey.set(starColumn.getBitPosition());
            }
        }
        return bitKey;
    }

    /**
     * Returns a list of all aliases used in this star.
     */
    public List<String> getAliasList() {
        List<String> aliasList = new ArrayList<String>();
        if (factTable != null) {
            collectAliases(aliasList, factTable);
        }
        return aliasList;
    }

    /**
     * Finds all of the table aliases in a table and its children.
     */
    private static void collectAliases(List<String> aliasList, Table table) {
        aliasList.add(table.getAlias());
        for (Table child : table.children) {
            collectAliases(aliasList, child);
        }
    }

    /**
     * Collects all columns in this table and its children.
     * If <code>joinColumn</code> is specified, only considers child tables
     * joined by the given column.
     */
    public static void collectColumns(
        Collection<Column> columnList,
        Table table,
        MondrianDef.Column joinColumn)
    {
        if (joinColumn == null) {
            columnList.addAll(table.columnList);
        }
        for (Table child : table.children) {
            if (joinColumn == null
                || child.getJoinCondition().left.equals(joinColumn))
            {
                collectColumns(columnList, child, null);
            }
        }
    }

    private boolean containsColumn(String tableName, String columnName) {
        Connection jdbcConnection;
        try {
            jdbcConnection = dataSource.getConnection();
        } catch (SQLException e1) {
            throw Util.newInternal(
                e1, "Error while creating connection from data source");
        }
        try {
            final DatabaseMetaData metaData = jdbcConnection.getMetaData();
            final ResultSet columns =
                metaData.getColumns(null, null, tableName, columnName);
            return columns.next();
        } catch (SQLException e) {
            throw Util.newInternal(
                "Error while retrieving metadata for table '" + tableName
                + "', column '" + columnName + "'");
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public RolapSchema getSchema() {
        return schema;
    }

    /**
     * Generates a SQL statement to read all instances of the given attributes.
     *
     * <p>The SQL statement is of the form {@code SELECT ... FROM ... JOIN ...
     * GROUP BY ...}. It is useful for populating an aggregate table.
     *
     * @param columnList List of columns (attributes and measures)
     * @param columnNameList List of column names (must have same cardinality
     *     as {@code columnList})
     * @return SQL SELECT statement
     */
    public String generateSql(
        List<Column> columnList,
        List<String> columnNameList)
    {
        final SqlQuery query = new SqlQuery(sqlQueryDialect, true);
        query.addFrom(
            factTable.relation,
            factTable.relation.getAlias(),
            false);
        int k = -1;
        for (Column column : columnList) {
            ++k;
            column.table.addToFrom(query,  false, true);
            String columnExpr = column.generateExprString(query);
            if (column instanceof Measure) {
                Measure measure = (Measure) column;
                columnExpr = measure.getAggregator().getExpression(columnExpr);
            }
            final String columnName = columnNameList.get(k);
            String alias = query.addSelect(columnExpr, columnName);
            if (!(column instanceof Measure)) {
                query.addGroupBy(columnExpr, alias);
            }
        }
        // remove whitespace from query - in particular, the trailing newline
        return query.toString().trim();
    }

    public String toString() {
        StringWriter sw = new StringWriter(256);
        PrintWriter pw = new PrintWriter(sw);
        print(pw, "", true);
        pw.flush();
        return sw.toString();
    }

    /**
     * Prints the state of this <code>RolapStar</code>
     *
     * @param pw Writer
     * @param prefix Prefix to print at the start of each line
     * @param structure Whether to print the structure of the star
     */
    public void print(PrintWriter pw, String prefix, boolean structure) {
        if (structure) {
            pw.print(prefix);
            pw.println("RolapStar:");
            String subprefix = prefix + "  ";
            factTable.print(pw, subprefix);

            for (AggStar aggStar : getAggStars()) {
                aggStar.print(pw, subprefix);
            }
        }

        List<Aggregation> aggregationList =
            new ArrayList<Aggregation>(sharedAggregations.values());
        Collections.sort(
            aggregationList,
            new Comparator<Aggregation>() {
                public int compare(Aggregation o1, Aggregation o2) {
                    return o1.getConstrainedColumnsBitKey().compareTo(
                        o2.getConstrainedColumnsBitKey());
                }
            }
       );

        for (Aggregation aggregation : aggregationList) {
            aggregation.print(pw);
        }
    }

    /**
     * Flushes the contents of a given region of cells from this star.
     *
     * @param cacheControl Cache control API
     * @param region Predicate defining a region of cells
     */
    public void flush(
        CacheControl cacheControl,
        CacheControl.CellRegion region)
    {
        // Translate the region into a set of (column, value) constraints.
        final RolapCacheRegion cacheRegion =
            RolapAggregationManager.makeCacheRegion(this, region);
        for (Aggregation aggregation : sharedAggregations.values()) {
            aggregation.flush(cacheControl, cacheRegion);
        }
    }

    /**
     * Returns the listener for changes to this star's underlying database.
     *
     * @return Returns the Data source change listener.
     */
    public DataSourceChangeListener getChangeListener() {
        return changeListener;
    }

    /**
     * Sets the listener for changes to this star's underlying database.
     *
     * @param changeListener The Data source change listener to set
     */
    public void setChangeListener(DataSourceChangeListener changeListener) {
        this.changeListener = changeListener;
    }

    /**
     * Adds a column to the star's list of all columns across all tables.
     *
     * @param c the column to add
     */
    private void addColumn(Column c) {
        columnList.add(c.getBitPosition(), c);
    }

    /**
     * Look up the column at the given bit position.
     *
     * @param bitPos bit position to look up
     * @return column at the given position
     */
    public Column getColumn(int bitPos) {
        return columnList.get(bitPos);
    }

    // -- Inner classes --------------------------------------------------------

    /**
     * A column in a star schema.
     */
    public static class Column {
        private final Table table;
        private final MondrianDef.Expression expression;
        private final Dialect.Datatype datatype;
        private final SqlStatement.Type internalType;
        private final String name;

        /**
         * When a Column is a column, and not a Measure, the parent column
         * is the coloumn associated with next highest Level.
         */
        private final Column parentColumn;
        /**
         * This is used during both aggregate table recognition and aggregate
         * table generation. For multiple dimension usages, multiple shared
         * dimension or unshared dimension with the same column names,
         * this is used to disambiguate aggregate column names.
         */
        private final String usagePrefix;
        /**
         * This is only used in RolapAggregationManager and adds
         * non-constraining columns making the drill-through queries easier for
         * humans to understand.
         */
        private final Column nameColumn;

        private boolean isNameColumn;

        /** this has a unique value per star */
        private final int bitPosition;
        /**
         * The estimated cardinality of the column.
         * {@link Integer#MIN_VALUE} means unknown.
         */
        private int approxCardinality = Integer.MIN_VALUE;

        private Column(
            String name,
            Table table,
            MondrianDef.Expression expression,
            Dialect.Datatype datatype)
        {
            this(
                name, table, expression, datatype, null, null,
                null, null, Integer.MIN_VALUE, table.star.nextColumnCount());
        }

        private Column(
            String name,
            Table table,
            MondrianDef.Expression expression,
            Dialect.Datatype datatype,
            SqlStatement.Type internalType,
            Column nameColumn,
            Column parentColumn,
            String usagePrefix,
            int approxCardinality,
            int bitPosition)
        {
            this.name = name;
            this.table = table;
            this.expression = expression;
            this.datatype = datatype;
            this.internalType = internalType;
            this.bitPosition = bitPosition;
            this.nameColumn = nameColumn;
            this.parentColumn = parentColumn;
            this.usagePrefix = usagePrefix;
            this.approxCardinality = approxCardinality;
            if (nameColumn != null) {
                nameColumn.isNameColumn = true;
            }

            if (table != null) {
                table.star.addColumn(this);
            }
        }

        /**
         * Fake column.
         *
         * @param datatype Datatype
         */
        protected Column(Dialect.Datatype datatype)
        {
            this(
                null,
                null,
                null,
                datatype,
                null,
                null,
                null,
                null,
                Integer.MIN_VALUE,
                0);
        }

        public boolean equals(Object obj) {
            if (! (obj instanceof RolapStar.Column)) {
                return false;
            }
            RolapStar.Column other = (RolapStar.Column) obj;
            // Note: both columns have to be from the same table
            return
                other.table == this.table
                && Util.equals(other.expression, this.expression)
                && other.datatype == this.datatype
                && other.name.equals(this.name);
        }

        public int hashCode() {
            int h = name.hashCode();
            h = Util.hash(h, table);
            return h;
        }

        public String getName() {
            return name;
        }

        public int getBitPosition() {
            return bitPosition;
        }

        public RolapStar getStar() {
            return table.star;
        }

        public RolapStar.Table getTable() {
            return table;
        }

        public SqlQuery getSqlQuery() {
            return getTable().getStar().getSqlQuery();
        }

        public RolapStar.Column getNameColumn() {
            return nameColumn;
        }

        public RolapStar.Column getParentColumn() {
            return parentColumn;
        }

        public String getUsagePrefix() {
            return usagePrefix;
        }

        public boolean isNameColumn() {
            return isNameColumn;
        }

        public MondrianDef.Expression getExpression() {
            return expression;
        }

        /**
         * Generates a SQL expression, which typically this looks like
         * this: <code><i>tableName</i>.<i>columnName</i></code>.
         */
        public String generateExprString(SqlQuery query) {
            return getExpression().getExpression(query);
        }

        /**
         * Get column cardinality from the schema cache if possible;
         * otherwise issue a select count(distinct) query to retrieve
         * the cardinality and stores it in the cache.
         *
         * @return the column cardinality.
         */
        public int getCardinality() {
            if (approxCardinality == Integer.MIN_VALUE) {
                RolapStar star = getStar();
                RolapSchema schema = star.getSchema();
                Integer card =
                    schema.getCachedRelationExprCardinality(
                        table.getRelation(),
                        expression);

                if (card != null) {
                    approxCardinality = card.intValue();
                } else {
                    // If not cached, issue SQL to get the cardinality for
                    // this column.
                    approxCardinality = getCardinality(star.getDataSource());
                    schema.putCachedRelationExprCardinality(
                        table.getRelation(),
                        expression,
                        approxCardinality);
                }
            }
            return approxCardinality;
        }

        private int getCardinality(DataSource dataSource) {
            SqlQuery sqlQuery = getSqlQuery();
            if (sqlQuery.getDialect().allowsCountDistinct()) {
                // e.g. "select count(distinct product_id) from product"
                sqlQuery.addSelect(
                    "count(distinct "
                    + generateExprString(sqlQuery) + ")");

                // no need to join fact table here
                table.addToFrom(sqlQuery, true, false);
            } else if (sqlQuery.getDialect().allowsFromQuery()) {
                // Some databases (e.g. Access) don't like 'count(distinct)',
                // so use, e.g., "select count(*) from (select distinct
                // product_id from product)"
                SqlQuery inner = sqlQuery.cloneEmpty();
                inner.setDistinct(true);
                inner.addSelect(generateExprString(inner));
                boolean failIfExists = true,
                    joinToParent = false;
                table.addToFrom(inner, failIfExists, joinToParent);
                sqlQuery.addSelect("count(*)");
                sqlQuery.addFrom(inner, "init", failIfExists);
            } else {
                throw Util.newInternal(
                    "Cannot compute cardinality: this "
                    + "database neither supports COUNT DISTINCT nor SELECT in "
                    + "the FROM clause.");
            }
            String sql = sqlQuery.toString();
            final SqlStatement stmt =
                RolapUtil.executeQuery(
                    dataSource, sql,
                    "RolapStar.Column.getCardinality",
                    "while counting distinct values of column '"
                    + expression.getGenericExpression(),
                    sqlQuery.getDialect());
            try {
                ResultSet resultSet = stmt.getResultSet();
                Util.assertTrue(resultSet.next());
                ++stmt.rowCount;
                return resultSet.getInt(1);
            } catch (SQLException e) {
                throw stmt.handle(e);
            } finally {
                stmt.close();
            }
        }

        /**
         * Generates a predicate that a column matches one of a list of values.
         *
         * <p>
         * Several possible outputs, depending upon whether the there are
         * nulls:<ul>
         *
         * <li>One not-null value: <code>foo.bar = 1</code>
         *
         * <li>All values not null: <code>foo.bar in (1, 2, 3)</code></li
         *
         * <li>Null and not null values:
         * <code>(foo.bar is null or foo.bar in (1, 2))</code></li>
         *
         * <li>Only null values:
         * <code>foo.bar is null</code></li>
         *
         * <li>String values: <code>foo.bar in ('a', 'b', 'c')</code></li>
         *
         * </ul>
         */
        public static String createInExpr(
            final String expr,
            StarColumnPredicate predicate,
            Dialect.Datatype datatype,
            SqlQuery sqlQuery)
        {
            // Sometimes a column predicate is created without a column. This
            // is unfortunate, and we will fix it some day. For now, create
            // a fake column with all of the information needed by the toSql
            // method, and a copy of the predicate wrapping that fake column.
            if (!Bug.BugMondrian313Fixed
                || !Bug.BugMondrian314Fixed
                && predicate.getConstrainedColumn() == null)
            {
                Column column = new Column(datatype) {
                    public String generateExprString(SqlQuery query) {
                        return expr;
                    }
                };
                predicate = predicate.cloneWithColumn(column);
            }

            StringBuilder buf = new StringBuilder(64);
            predicate.toSql(sqlQuery, buf);
            return buf.toString();
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }

        /**
         * Prints this column.
         *
         * @param pw Print writer
         * @param prefix Prefix to print first, such as spaces for indentation
         */
        public void print(PrintWriter pw, String prefix) {
            SqlQuery sqlQuery = getSqlQuery();
            pw.print(prefix);
            pw.print(getName());
            pw.print(" (");
            pw.print(getBitPosition());
            pw.print("): ");
            pw.print(generateExprString(sqlQuery));
        }

        public Dialect.Datatype getDatatype() {
            return datatype;
        }

        /**
         * Returns a string representation of the datatype of this column, in
         * the dialect specified. For example, 'DECIMAL(10, 2) NOT NULL'.
         *
         * @param dialect Dialect
         * @return String representation of column's datatype
         */
        public String getDatatypeString(Dialect dialect) {
            final SqlQuery query = new SqlQuery(dialect);
            query.addFrom(
                table.star.factTable.relation, table.star.factTable.alias,
                false);
            query.addFrom(table.relation, table.alias, false);
            query.addSelect(expression.getExpression(query));
            final String sql = query.toString();
            Connection jdbcConnection = null;
            try {
                jdbcConnection = table.star.dataSource.getConnection();
                final PreparedStatement pstmt =
                    jdbcConnection.prepareStatement(sql);
                final ResultSetMetaData resultSetMetaData =
                    pstmt.getMetaData();
                assert resultSetMetaData.getColumnCount() == 1;
                final String type = resultSetMetaData.getColumnTypeName(1);
                int precision = resultSetMetaData.getPrecision(1);
                final int scale = resultSetMetaData.getScale(1);
                if (type.equals("DOUBLE")) {
                    precision = 0;
                }
                String typeString;
                if (precision == 0) {
                    typeString = type;
                } else if (scale == 0) {
                    typeString = type + "(" + precision + ")";
                } else {
                    typeString = type + "(" + precision + ", " + scale + ")";
                }
                pstmt.close();
                jdbcConnection.close();
                jdbcConnection = null;
                return typeString;
            } catch (SQLException e) {
                throw Util.newError(
                    e,
                    "Error while deriving type of column " + toString());
            } finally {
                if (jdbcConnection != null) {
                    try {
                        jdbcConnection.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }

        public SqlStatement.Type getInternalType() {
            return internalType;
        }
    }

    /**
     * Definition of a measure in a star schema.
     *
     * <p>A measure is basically just a column; except that its
     * {@link #aggregator} defines how it is to be rolled up.
     */
    public static class Measure extends Column {
        private final String cubeName;
        private final RolapAggregator aggregator;

        public Measure(
            String name,
            String cubeName,
            RolapAggregator aggregator,
            Table table,
            MondrianDef.Expression expression,
            Dialect.Datatype datatype)
        {
            super(name, table, expression, datatype);
            this.cubeName = cubeName;
            this.aggregator = aggregator;
        }

        public RolapAggregator getAggregator() {
            return aggregator;
        }

        public boolean equals(Object o) {
            if (! (o instanceof RolapStar.Measure)) {
                return false;
            }
            RolapStar.Measure that = (RolapStar.Measure) o;
            if (!super.equals(that)) {
                return false;
            }
            // Measure names are only unique within their cube - and remember
            // that a given RolapStar can support multiple cubes if they have
            // the same fact table.
            if (!cubeName.equals(that.cubeName)) {
                return false;
            }
            // Note: both measure have to have the same aggregator
            return (that.aggregator == this.aggregator);
        }

        public int hashCode() {
            int h = super.hashCode();
            h = Util.hash(h, aggregator);
            return h;
        }

        public void print(PrintWriter pw, String prefix) {
            SqlQuery sqlQuery = getSqlQuery();
            pw.print(prefix);
            pw.print(getName());
            pw.print(" (");
            pw.print(getBitPosition());
            pw.print("): ");
            pw.print(
                aggregator.getExpression(
                    getExpression() == null
                        ? null
                        : generateExprString(sqlQuery)));
        }

        public String getCubeName() {
            return cubeName;
        }
    }

    /**
     * Definition of a table in a star schema.
     *
     * <p>A 'table' is defined by a
     * {@link mondrian.olap.MondrianDef.RelationOrJoin} so may, in fact, be a
     * view.
     *
     * <p>Every table in the star schema except the fact table has a parent
     * table, and a condition which specifies how it is joined to its parent.
     * So the star schema is, in effect, a hierarchy with the fact table at
     * its root.
     */
    public static class Table {
        private final RolapStar star;
        private final MondrianDef.Relation relation;
        private final List<Column> columnList;
        private final Table parent;
        private List<Table> children;
        private final Condition joinCondition;
        private final String alias;

        private Table(
            RolapStar star,
            MondrianDef.Relation relation,
            Table parent,
            Condition joinCondition)
        {
            this.star = star;
            this.relation = relation;
            this.alias = chooseAlias();
            this.parent = parent;
            final AliasReplacer aliasReplacer =
                    new AliasReplacer(relation.getAlias(), this.alias);
            this.joinCondition = aliasReplacer.visit(joinCondition);
            if (this.joinCondition != null) {
                this.joinCondition.table = this;
            }
            this.columnList = new ArrayList<Column>();
            this.children = Collections.emptyList();
            Util.assertTrue((parent == null) == (joinCondition == null));
        }

        /**
         * Returns the condition by which a dimension table is connected to its
         * {@link #getParentTable() parent}; or null if this is the fact table.
         */
        public Condition getJoinCondition() {
            return joinCondition;
        }

        /**
         * Returns this table's parent table, or null if this is the fact table
         * (which is at the center of the star).
         */
        public Table getParentTable() {
            return parent;
        }

        private void addColumn(Column column) {
            columnList.add(column);
        }

        /**
         * Adds to a list all columns of this table or a child table
         * which are present in a given bitKey.
         *
         * <p>Note: This method is slow, but that's acceptable because it is
         * only used for tracing. It would be more efficient to store an
         * array in the {@link RolapStar} mapping column ordinals to columns.
         */
        private void collectColumns(BitKey bitKey, List<Column> list) {
            for (Column column : getColumns()) {
                if (bitKey.get(column.getBitPosition())) {
                    list.add(column);
                }
            }
            for (Table table : getChildren()) {
                table.collectColumns(bitKey, list);
            }
        }

        /**
         * Returns an array of all columns in this star with a given name.
         */
        public Column[] lookupColumns(String columnName) {
            List<Column> l = new ArrayList<Column>();
            for (Column column : getColumns()) {
                if (column.getExpression() instanceof MondrianDef.Column) {
                    MondrianDef.Column columnExpr =
                        (MondrianDef.Column) column.getExpression();
                    if (columnExpr.name.equals(columnName)) {
                        l.add(column);
                    }
                }
            }
            return l.toArray(new Column[l.size()]);
        }

        public Column lookupColumn(String columnName) {
            for (Column column : getColumns()) {
                if (column.getExpression() instanceof MondrianDef.Column) {
                    MondrianDef.Column columnExpr =
                        (MondrianDef.Column) column.getExpression();
                    if (columnExpr.name.equals(columnName)) {
                        return column;
                    }
                } else if (column.getName().equals(columnName)) {
                    return column;
                }
            }
            return null;
        }

        /**
         * Given a MondrianDef.Expression return a column with that expression
         * or null.
         */
        public Column lookupColumnByExpression(MondrianDef.Expression xmlExpr) {
            for (Column column : getColumns()) {
                if (column instanceof Measure) {
                    continue;
                }
                if (column.getExpression().equals(xmlExpr)) {
                    return column;
                }
            }
            return null;
        }

        public boolean containsColumn(Column column) {
            return getColumns().contains(column);
        }

        /**
         * Look up a {@link Measure} by its name.
         * Returns null if not found.
         */
        public Measure lookupMeasureByName(String cubeName, String name) {
            for (Column column : getColumns()) {
                if (column instanceof Measure) {
                    Measure measure = (Measure) column;
                    if (measure.getName().equals(name)
                        && measure.getCubeName().equals(cubeName))
                    {
                        return measure;
                    }
                }
            }
            return null;
        }

        RolapStar getStar() {
            return star;
        }
        private SqlQuery getSqlQuery() {
            return getStar().getSqlQuery();
        }
        public MondrianDef.Relation getRelation() {
            return relation;
        }

        /** Chooses an alias which is unique within the star. */
        private String chooseAlias() {
            List<String> aliasList = star.getAliasList();
            for (int i = 0;; ++i) {
                String candidateAlias = relation.getAlias();
                if (i > 0) {
                    candidateAlias += "_" + i;
                }
                if (!aliasList.contains(candidateAlias)) {
                    return candidateAlias;
                }
            }
        }

        public String getAlias() {
            return alias;
        }

        /**
         * Sometimes one need to get to the "real" name when the table has
         * been given an alias.
         */
        public String getTableName() {
            if (relation instanceof MondrianDef.Table) {
                MondrianDef.Table t = (MondrianDef.Table) relation;
                return t.name;
            } else {
                return null;
            }
        }

        synchronized void makeMeasure(RolapBaseCubeMeasure measure) {
            // Remove assertion to allow cube to be recreated
            // assert lookupMeasureByName(
            //    measure.getCube().getName(), measure.getName()) == null;
            RolapStar.Measure starMeasure = new RolapStar.Measure(
                measure.getName(),
                measure.getCube().getName(),
                measure.getAggregator(),
                this,
                measure.getMondrianDefExpression(),
                measure.getDatatype());

            measure.setStarMeasure(starMeasure); // reverse mapping

            if (containsColumn(starMeasure)) {
                star.decrementColumnCount();
            } else {
                addColumn(starMeasure);
            }
        }

        /**
         * This is only called by RolapCube. If the RolapLevel has a non-null
         * name expression then two columns will be made, otherwise only one.
         * Updates the RolapLevel to RolapStar.Column mapping associated with
         * this cube.
         *
         * @param cube Cube
         * @param level Level
         * @param parentColumn Parent column
         */
        synchronized Column makeColumns(
            RolapCube cube,
            RolapCubeLevel level,
            Column parentColumn,
            String usagePrefix)
        {
            Column nameColumn = null;
            if (level.getNameExp() != null) {
                // make a column for the name expression
                nameColumn = makeColumnForLevelExpr(
                    cube,
                    level,
                    level.getName(),
                    level.getNameExp(),
                    Dialect.Datatype.String,
                    null,
                    null,
                    null,
                    null);
            }

            // select the column's name depending upon whether or not a
            // "named" column, above, has been created.
            String name = (level.getNameExp() == null)
                ? level.getName()
                : level.getName() + " (Key)";

            // If the nameColumn is not null, then it is associated with this
            // column.
            Column column = makeColumnForLevelExpr(
                cube,
                level,
                name,
                level.getKeyExp(),
                level.getDatatype(),
                level.getInternalType(),
                nameColumn,
                parentColumn,
                usagePrefix);

            if (column != null) {
                level.setStarKeyColumn(column);
            }

            return column;
        }

        private Column makeColumnForLevelExpr(
            RolapCube cube,
            RolapLevel level,
            String name,
            MondrianDef.Expression xmlExpr,
            Dialect.Datatype datatype,
            SqlStatement.Type internalType,
            Column nameColumn,
            Column parentColumn,
            String usagePrefix)
        {
            Table table = this;
            if (xmlExpr instanceof MondrianDef.Column) {
                final MondrianDef.Column xmlColumn =
                    (MondrianDef.Column) xmlExpr;

                String tableName = xmlColumn.table;
                table = findAncestor(tableName);
                if (table == null) {
                    throw Util.newError(
                        "Level '" + level.getUniqueName()
                        + "' of cube '"
                        + this
                        + "' is invalid: table '" + tableName
                        + "' is not found in current scope"
                        + Util.nl
                        + ", star:"
                        + Util.nl
                        + getStar());
                }
                RolapStar.AliasReplacer aliasReplacer =
                    new RolapStar.AliasReplacer(tableName, table.getAlias());
                xmlExpr = aliasReplacer.visit(xmlExpr);
            }
            // does the column already exist??
            Column c = lookupColumnByExpression(xmlExpr);

            RolapStar.Column column;
            // Verify Column is not null and not the same as the
            // nameColumn created previously (bug 1438285)
            if (c != null && !c.equals(nameColumn)) {
                // Yes, well just reuse it
                // You might wonder why the column need be returned if it
                // already exists. Well, it might have been created for one
                // cube, but for another cube using the same fact table, it
                // still needs to be put into the cube level to column map.
                // Trust me, return null and a junit test fails.
                column = c;
            } else {
                // Make a new column and add it
                column = new RolapStar.Column(
                    name,
                    table,
                    xmlExpr,
                    datatype,
                    internalType,
                    nameColumn,
                    parentColumn,
                    usagePrefix,
                    level.getApproxRowCount(),
                    star.nextColumnCount());
                addColumn(column);
            }
            return column;
        }

        /**
         * Extends this 'leg' of the star by adding <code>relation</code>
         * joined by <code>joinCondition</code>. If the same expression is
         * already present, does not create it again. Stores the unaliased
         * table names to RolapStar.Table mapping associated with the
         * input <code>cube</code>.
         */
        synchronized Table addJoin(
            RolapCube cube,
            MondrianDef.RelationOrJoin relationOrJoin,
            RolapStar.Condition joinCondition)
        {
            if (relationOrJoin instanceof MondrianDef.Relation) {
                final MondrianDef.Relation relation =
                    (MondrianDef.Relation) relationOrJoin;
                RolapStar.Table starTable =
                    findChild(relation, joinCondition);
                if (starTable == null) {
                    starTable = new RolapStar.Table(
                        star, relation, this, joinCondition);
                    if (this.children.isEmpty()) {
                        this.children = new ArrayList<Table>();
                    }
                    this.children.add(starTable);
                }
                return starTable;
            } else if (relationOrJoin instanceof MondrianDef.Join) {
                MondrianDef.Join join = (MondrianDef.Join) relationOrJoin;
                RolapStar.Table leftTable =
                    addJoin(cube, join.left, joinCondition);
                String leftAlias = join.leftAlias;
                if (leftAlias == null) {
                    // REVIEW: is cast to Relation valid?
                    leftAlias = ((MondrianDef.Relation) join.left).getAlias();
                    if (leftAlias == null) {
                        throw Util.newError(
                            "missing leftKeyAlias in " + relationOrJoin);
                    }
                }
                assert leftTable.findAncestor(leftAlias) == leftTable;
                // switch to uniquified alias
                leftAlias = leftTable.getAlias();

                String rightAlias = join.rightAlias;
                if (rightAlias == null) {
                    // the right relation of a join may be a join
                    // if so, we need to use the right relation join's
                    // left relation's alias.
                    if (join.right instanceof MondrianDef.Join) {
                        MondrianDef.Join joinright =
                            (MondrianDef.Join) join.right;
                        // REVIEW: is cast to Relation valid?
                        rightAlias =
                            ((MondrianDef.Relation) joinright.left)
                                .getAlias();
                    } else {
                        // REVIEW: is cast to Relation valid?
                        rightAlias =
                            ((MondrianDef.Relation) join.right)
                                .getAlias();
                    }
                    if (rightAlias == null) {
                        throw Util.newError(
                            "missing rightKeyAlias in " + relationOrJoin);
                    }
                }
                joinCondition = new RolapStar.Condition(
                    new MondrianDef.Column(leftAlias, join.leftKey),
                    new MondrianDef.Column(rightAlias, join.rightKey));
                RolapStar.Table rightTable = leftTable.addJoin(
                    cube, join.right, joinCondition);
                return rightTable;

            } else {
                throw Util.newInternal("bad relation type " + relationOrJoin);
            }
        }

        /**
         * Returns a child relation which maps onto a given relation, or null
         * if there is none.
         */
        public Table findChild(
            MondrianDef.Relation relation,
            Condition joinCondition)
        {
            for (Table child : getChildren()) {
                if (child.relation.equals(relation)) {
                    Condition condition = joinCondition;
                    if (!Util.equalName(relation.getAlias(), child.alias)) {
                        // Make the two conditions comparable, by replacing
                        // occurrence of this table's alias with occurrences
                        // of the child's alias.
                        AliasReplacer aliasReplacer = new AliasReplacer(
                            relation.getAlias(), child.alias);
                        condition = aliasReplacer.visit(joinCondition);
                    }
                    if (child.joinCondition.equals(condition)) {
                        return child;
                    }
                }
            }
            return null;
        }

        /**
         * Returns a descendant with a given alias, or null if none found.
         */
        public Table findDescendant(String seekAlias) {
            if (getAlias().equals(seekAlias)) {
                return this;
            }
            for (Table child : getChildren()) {
                Table found = child.findDescendant(seekAlias);
                if (found != null) {
                    return found;
                }
            }
            return null;
        }

        /**
         * Returns an ancestor with a given alias, or null if not found.
         */
        public Table findAncestor(String tableName) {
            for (Table t = this; t != null; t = t.parent) {
                if (t.relation.getAlias().equals(tableName)) {
                    return t;
                }
            }
            return null;
        }

        public boolean equalsTableName(String tableName) {
            if (this.relation instanceof MondrianDef.Table) {
                MondrianDef.Table mt = (MondrianDef.Table) this.relation;
                if (mt.name.equals(tableName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Adds this table to the FROM clause of a query, and also, if
         * <code>joinToParent</code>, any join condition.
         *
         * @param query Query to add to
         * @param failIfExists Pass in false if you might have already added
         *     the table before and if that happens you want to do nothing.
         * @param joinToParent Pass in true if you are constraining a cell
         *     calculation, false if you are retrieving members.
         */
        public void addToFrom(
            SqlQuery query,
            boolean failIfExists,
            boolean joinToParent)
        {
            query.addFrom(relation, alias, failIfExists);
            Util.assertTrue((parent == null) == (joinCondition == null));
            if (joinToParent) {
                if (parent != null) {
                    parent.addToFrom(query, failIfExists, joinToParent);
                }
                if (joinCondition != null) {
                    query.addWhere(joinCondition.toString(query));
                }
            }
        }

        /**
         * Returns a list of child {@link Table}s.
         */
        public List<Table> getChildren() {
            return children;
        }

        /**
         * Returns a list of this table's {@link Column}s.
         */
        public List<Column> getColumns() {
            return columnList;
        }

        /**
         * Finds the child table of the fact table with the given columnName
         * used in its left join condition. This is used by the AggTableManager
         * while characterizing the fact table columns.
         */
        public RolapStar.Table findTableWithLeftJoinCondition(
            final String columnName)
        {
            for (Table child : getChildren()) {
                Condition condition = child.joinCondition;
                if (condition != null) {
                    if (condition.left instanceof MondrianDef.Column) {
                        MondrianDef.Column mcolumn =
                            (MondrianDef.Column) condition.left;
                        if (mcolumn.name.equals(columnName)) {
                            return child;
                        }
                    }
                }
            }
            return null;
        }

        /**
         * This is used during aggregate table validation to make sure that the
         * mapping from for the aggregate join condition is valid. It returns
         * the child table with the matching left join condition.
         */
        public RolapStar.Table findTableWithLeftCondition(
            final MondrianDef.Expression left)
        {
            for (Table child : getChildren()) {
                Condition condition = child.joinCondition;
                if (condition != null) {
                    if (condition.left instanceof MondrianDef.Column) {
                        MondrianDef.Column mcolumn =
                            (MondrianDef.Column) condition.left;
                        if (mcolumn.equals(left)) {
                            return child;
                        }
                    }
                }
            }
            return null;
        }

        /**
         * Note: I do not think that this is ever true.
         */
        public boolean isFunky() {
            return (relation == null);
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Table)) {
                return false;
            }
            Table other = (Table) obj;
            return getAlias().equals(other.getAlias());
        }
        public int hashCode() {
            return getAlias().hashCode();
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }

        /**
         * Prints this table and its children.
         */
        public void print(PrintWriter pw, String prefix) {
            pw.print(prefix);
            pw.println("Table:");
            String subprefix = prefix + "  ";

            pw.print(subprefix);
            pw.print("alias=");
            pw.println(getAlias());

            if (this.relation != null) {
                pw.print(subprefix);
                pw.print("relation=");
                pw.println(relation);
            }

            pw.print(subprefix);
            pw.println("Columns:");
            String subsubprefix = subprefix + "  ";

            for (Column column : getColumns()) {
                column.print(pw, subsubprefix);
                pw.println();
            }

            if (this.joinCondition != null) {
                this.joinCondition.print(pw, subprefix);
            }
            for (Table child : getChildren()) {
                child.print(pw, subprefix);
            }
        }

        /**
         * Returns whether this table has a column with the given name.
         */
        public boolean containsColumn(String columnName) {
            if (relation instanceof MondrianDef.Relation) {
                return star.containsColumn(
                    ((MondrianDef.Relation) relation).getAlias(),
                    columnName);
            } else {
                // todo: Deal with join.
                return false;
            }
        }
    }

    public static class Condition {
        private static final Logger LOGGER = Logger.getLogger(Condition.class);

        private final MondrianDef.Expression left;
        private final MondrianDef.Expression right;
        // set in Table constructor
        Table table;

        Condition(
            MondrianDef.Expression left,
            MondrianDef.Expression right)
        {
            assert left != null;
            assert right != null;

            if (!(left instanceof MondrianDef.Column)) {
                // TODO: Will this ever print?? if not then left should be
                // of type MondrianDef.Column.
                LOGGER.debug(
                    "Condition.left NOT Column: "
                    + left.getClass().getName());
            }
            this.left = left;
            this.right = right;
        }
        public MondrianDef.Expression getLeft() {
            return left;
        }
        public String getLeft(final SqlQuery query) {
            return this.left.getExpression(query);
        }
        public MondrianDef.Expression getRight() {
            return right;
        }
        public String getRight(final SqlQuery query) {
            return this.right.getExpression(query);
        }
        public String toString(SqlQuery query) {
            return left.getExpression(query) + " = "
                + right.getExpression(query);
        }
        public int hashCode() {
            return left.hashCode() ^ right.hashCode();
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Condition)) {
                return false;
            }
            Condition that = (Condition) obj;
            return this.left.equals(that.left)
                && this.right.equals(that.right);
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }

        /**
         * Prints this table and its children.
         */
        public void print(PrintWriter pw, String prefix) {
            SqlQuery sqlQueuy = table.getSqlQuery();
            pw.print(prefix);
            pw.println("Condition:");
            String subprefix = prefix + "  ";

            pw.print(subprefix);
            pw.print("left=");
            // print the foreign key bit position if we can figure it out
            if (left instanceof MondrianDef.Column) {
                MondrianDef.Column c = (MondrianDef.Column) left;
                Column col = table.star.getFactTable().lookupColumn(c.name);
                if (col != null) {
                    pw.print(" (");
                    pw.print(col.getBitPosition());
                    pw.print(") ");
                }
             }
            pw.println(left.getExpression(sqlQueuy));

            pw.print(subprefix);
            pw.print("right=");
            pw.println(right.getExpression(sqlQueuy));
        }
    }

    /**
     * Creates a copy of an expression, everywhere replacing one alias
     * with another.
     */
    public static class AliasReplacer {
        private final String oldAlias;
        private final String newAlias;

        public AliasReplacer(String oldAlias, String newAlias) {
            this.oldAlias = oldAlias;
            this.newAlias = newAlias;
        }

        private Condition visit(Condition condition) {
            if (condition == null) {
                return null;
            }
            if (newAlias.equals(oldAlias)) {
                return condition;
            }
            return new Condition(
                    visit(condition.left),
                    visit(condition.right));
        }

        public MondrianDef.Expression visit(MondrianDef.Expression expression) {
            if (expression == null) {
                return null;
            }
            if (newAlias.equals(oldAlias)) {
                return expression;
            }
            if (expression instanceof MondrianDef.Column) {
                MondrianDef.Column column = (MondrianDef.Column) expression;
                return new MondrianDef.Column(visit(column.table), column.name);
            } else {
                throw Util.newInternal("need to implement " + expression);
            }
        }

        private String visit(String table) {
            return table.equals(oldAlias)
                ? newAlias
                : table;
        }
    }

    /**
     * Comparator to compare columns based on their name
     */
    public static class ColumnComparator implements Comparator<Column> {

        public static ColumnComparator instance = new ColumnComparator();

        private ColumnComparator() {
        }

        public int compare(Column o1, Column o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }
}

// End RolapStar.java