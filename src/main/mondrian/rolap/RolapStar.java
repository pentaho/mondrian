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
import mondrian.rolap.agg.Aggregation;
import mondrian.rolap.agg.AggregationKey;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.server.Locus;
import mondrian.spi.DataSourceChangeListener;
import mondrian.spi.Dialect;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

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

    private final RolapSchema schema;

    // not final for test purposes
    private DataSource dataSource;

    private final Table factTable;

    /** Holds all global aggregations of this star. */
    private final Map<AggregationKey, Aggregation> sharedAggregations =
        new HashMap<AggregationKey, Aggregation>();

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
    private final Map<AggregationKey, Aggregation> pendingAggregations =
        new HashMap<AggregationKey, Aggregation>();

    /**
     * Holds all requests for aggregations.
     */
    private final List<AggregationKey> aggregationRequests =
        new ArrayList<AggregationKey>();

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

    /**
     * Keeps track of the columns across all tables. Should have
     * a number of elements equal to columnCount.
     */
    private final List<Column> columnList = new ArrayList<Column>();

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

    private final Map<RolapSchema.PhysExpr, Column> map =
        new HashMap<RolapSchema.PhysExpr, Column>();

    /**
     * @see Util#deprecated(Object, boolean)
     */
    private final Map<RolapSchema.PhysRelation, Table> tableMap =
        new HashMap<RolapSchema.PhysRelation, Table>();

    /**
     * Creates a RolapStar. Please use
     * {@link RolapSchema.RolapStarRegistry#getOrCreateStar} to create a
     * {@link RolapStar}.
     */
    protected RolapStar(
        final RolapSchema schema,
        final DataSource dataSource,
        final RolapSchema.PhysRelation fact)
    {
        this.cacheAggregations = true;
        this.schema = schema;
        this.dataSource = dataSource;
        this.factTable = new RolapStar.Table(this, fact, null, null);

        clearAggStarList();

        this.sqlQueryDialect = schema.getDialect();
        this.changeListener = schema.getDataSourceChangeListener();
    }

    /**
     * Returns the column of this RolapStar that holds a given expression.
     * The same expression may be held in other stars, too, mapped in each case
     * to a column owned by the respective star. The column represents a
     * path from the root of the star (its fact table) to the expression.
     *
     * <p>TODO: Needs another parameter, PhysPath!
     * Only (PhysColumn, PhysPath) is unique within a RolapStar.
     * See {@link mondrian.rolap.RolapMeasureGroup#getRolapStarColumn(RolapCubeDimension, mondrian.rolap.RolapSchema.PhysColumn)}.
     * Maybe we need a map object that combines (RolapCubeDimension, RolapStar).
     *
     * @param expr Expression
     * @param fail Whether to fail if not found
     * @return Column representing path from root of star to expression, never
     * null
     *
     * @throws RuntimeException if expression has not previously been registered
     *     and fail is true
     */
    public Column getColumn(
        RolapSchema.PhysExpr expr,
        boolean fail)
    {
        final Column column = map.get(expr);
        if (column == null) {
            if (fail) {
                throw new IllegalArgumentException(
                    "star has no column for " + expr);
            }
        } else {
            assert column.expression.equals(expr);
        }
        return column;
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
    public void setCacheAggregations(boolean cacheAggregations) {
        // this can only change from true to false
        this.cacheAggregations = cacheAggregations;
        clearCachedAggregations(false);
    }

    /**
     * Returns whether the this RolapStar cache aggregates.
     *
     * @see #setCacheAggregations(boolean)
     */
    public boolean isCacheAggregations() {
        return this.cacheAggregations;
    }

    boolean isCacheDisabled() {
        return MondrianProperties.instance().DisableCaching.get();
    }

    /**
     * Clears the aggregate cache. This only does something if aggregate caching
     * is disabled (see {@link #setCacheAggregations(boolean)}).
     *
     * @param forced If true, clears cached aggregations regardless of any other
     *   settings.  If false, clears only cache from the current thread
     */
    public void clearCachedAggregations(boolean forced) {
        if (forced || !cacheAggregations || isCacheDisabled()) {
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
            aggregation =
                new Aggregation(
                    MondrianServer.forConnection(
                        schema.getInternalConnection()),
                    aggregationKey);

            this.localAggregations.get().put(aggregationKey, aggregation);

            // Let the change listener get the opportunity to register the
            // first time the aggregation is used
            if ((this.cacheAggregations) && (!isCacheDisabled())) {
                if (changeListener != null) {
                    Util.discard(
                        changeListener.isAggregationChanged(aggregation));
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

        if (cacheAggregations && !isCacheDisabled()) {
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
            if (cacheAggregations && !isCacheDisabled()) {
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
                            aggregation =
                                new Aggregation(
                                    MondrianServer.forConnection(
                                        schema.getInternalConnection()),
                                    aggregationKey);

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
            if (cacheAggregations && !isCacheDisabled()) {
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
        if (cacheAggregations && !isCacheDisabled()) {
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
     * Collects all columns in a given table and its children into a list.
     * If <code>joinColumn</code> is specified, only considers child tables
     * joined by the given column.
     */
    public static void collectColumns(
        Collection<Column> columnList,
        Table table,
        RolapSchema.PhysRealColumn joinColumn)
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
        Util.deprecated("not used - remove?", false);
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
            String columnExpr = column.getExpression().toSql();
            if (column instanceof Measure) {
                Measure measure = (Measure) column;
                columnExpr = measure.getAggregator().getExpression(columnExpr);
            }
            final String columnName = columnNameList.get(k);
            String alias = query.addSelect(columnExpr, null, columnName);
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
     * @see Util#deprecated(Object, boolean) Consider moving this method from
     * RolapStar to {@link mondrian.rolap.RolapMeasureGroup}
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

    // -- Inner classes --------------------------------------------------------

    /**
     * A column in a star schema.
     */
    public static class Column {
        private final Table table;
        private final RolapSchema.PhysExpr expression;
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
         *
         * @see Util#deprecated(Object, boolean) This should belong to the
         *   RolapAttribute, not the star column. Should agg-table recognition
         *   be done at the attribute level too? If so, obsolete this field.
         *   Likewise parentColumn and usagePrefix.
         */
        private final Column nameColumn;


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
            RolapSchema.PhysExpr expression,
            Dialect.Datatype datatype)
        {
            this(
                name, table, expression, datatype, null, null,
                null, null, Integer.MIN_VALUE, table.star.nextColumnCount());
        }

        private Column(
            String name,
            Table table,
            RolapSchema.PhysExpr expression,
            Dialect.Datatype datatype,
            SqlStatement.Type internalType,
            Column nameColumn,
            Column parentColumn,
            String usagePrefix,
            int approxCardinality,
            int bitPosition)
        {
            assert table != null;
            assert name != null;
            assert datatype != null;
            if (Util.deprecated(false, false)) {
                // TODO: remove datatype field
                assert datatype == expression.getDatatype();
            }
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
            if (table != null) {
                table.star.addColumn(this);
            }
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

        public RolapStar.Column getNameColumn() {
            Util.deprecated(
                "nameColumn is redundant - could obsolete it and use RolapLevel.getNameExp then indirect",
                false);
            return nameColumn;
        }

        public RolapStar.Column getParentColumn() {
            Util.deprecated(
                "parentColumn seems to be used ONLY for AggGen; remove it and represent the information outside RolapStar?",
                false);
            return parentColumn;
        }

        public String getUsagePrefix() {
            return usagePrefix;
        }

        public boolean isNameColumn() {
            return nameColumn != null;
        }

        public RolapSchema.PhysExpr getExpression() {
            return expression;
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
                    approxCardinality = card;
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
            Dialect dialect = table.star.getSqlQueryDialect();
            SqlQuery sqlQuery = new SqlQuery(dialect);
            if (dialect.allowsCountDistinct()) {
                // e.g. "select count(distinct product_id) from product"
                sqlQuery.addSelect(
                    "count(distinct " + getExpression().toSql() + ")",
                    null);

                // no need to join fact table here
                table.addToFrom(sqlQuery, true, false);
            } else if (dialect.allowsFromQuery()) {
                // Some databases (e.g. Access) don't like 'count(distinct)',
                // so use, e.g., "select count(*) from (select distinct
                // product_id from product)"
                SqlQuery inner = sqlQuery.cloneEmpty();
                inner.setDistinct(true);
                inner.addSelect(getExpression().toSql(), null);
                boolean failIfExists = true,
                    joinToParent = false;
                table.addToFrom(inner, failIfExists, joinToParent);
                sqlQuery.addSelect("count(*)", null);
                sqlQuery.addFrom(inner, "init", failIfExists);
            } else {
                throw Util.newInternal(
                    "Cannot compute cardinality: this database neither "
                    + "supports COUNT DISTINCT nor SELECT in the FROM clause.");
            }
            String sql = sqlQuery.toString();
            final SqlStatement stmt =
                RolapUtil.executeQuery(
                    dataSource,
                    sql,
                    new Locus(
                        Locus.peek().execution,
                        "RolapStar.Column.getCardinality",
                        "while counting distinct values of column '"
                        + expression.toSql()));
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
            SqlQuery sqlQuery = new SqlQuery(table.star.getSqlQueryDialect());
            pw.print(prefix);
            pw.print(getName());
            pw.print(" (");
            pw.print(getBitPosition());
            pw.print("): ");
            pw.print(getExpression().toSql());
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
            Util.deprecated("move to dialect?", false);
            final SqlQuery query = new SqlQuery(dialect);
            query.addFrom(
                table.star.factTable.relation, table.star.factTable.alias,
                false);
            query.addFrom(table.relation, table.alias, false);
            query.addSelect(expression.toSql(), null);
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
            RolapSchema.PhysExpr expression,
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
            SqlQuery sqlQuery =
                new SqlQuery(getTable().star.getSqlQueryDialect());
            pw.print(prefix);
            pw.print(getName());
            pw.print(" (");
            pw.print(getBitPosition());
            pw.print("): ");
            pw.print(
                aggregator.getExpression(
                    getExpression() == null
                        ? null
                        : getExpression().toSql()));
        }

        public String getCubeName() {
            return cubeName;
        }
    }

    /**
     * Definition of a table in a star schema.
     *
     * <p>A 'table' is defined by a
     * {@link mondrian.olap.Mondrian3Def.RelationOrJoin} so may, in fact, be a
     * view.
     *
     * <p>Every table in the star schema except the fact table has a parent
     * table, and a condition which specifies how it is joined to its parent.
     * So the star schema is, in effect, a hierarchy with the fact table at
     * its root.
     */
    public static class Table {
        private final RolapStar star;
        private final RolapSchema.PhysRelation relation;
        private final List<Column> columnList;
        private final Table parent;
        private List<Table> children;
        private final Condition joinCondition;
        private final String alias;

        private Table(
            RolapStar star,
            RolapSchema.PhysRelation relation,
            Table parent,
            Condition joinCondition)
        {
            assert star != null;
            assert relation != null;
            this.star = star;
            this.relation = relation;
            this.alias = chooseAlias();
            this.parent = parent;
            this.joinCondition = joinCondition;
            this.columnList = new ArrayList<Column>();
            this.children = Collections.emptyList();
            assert (parent == null) == (joinCondition == null);
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
            Util.deprecated("not used - remove", true);
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
         * Returns a list of all columns in this star with a given name.
         */
        public List<Column> lookupColumns(String columnName) {
            List<Column> list = new ArrayList<Column>();
            for (Column column : getColumns()) {
                if (matches(columnName, column)) {
                    list.add(column);
                }
            }
            return list;
        }

        private boolean matches(String columnName, Column column) {
            final RolapSchema.PhysExpr expr = column.getExpression();
            if (expr instanceof RolapSchema.PhysRealColumn) {
                RolapSchema.PhysRealColumn columnExpr =
                    (RolapSchema.PhysRealColumn) expr;
                return columnExpr.name.equals(columnName);
            } else if (expr instanceof RolapSchema.PhysCalcColumn) {
                RolapSchema.PhysCalcColumn columnExpr =
                    (RolapSchema.PhysCalcColumn) expr;
                return columnExpr.toSql().equals(columnName);
            }
            return false;
        }

        public Column lookupColumn(String columnName) {
            for (Column column : getColumns()) {
                if (matches(columnName, column)) {
                    return column;
                }
            }
            return null;
        }

        /**
         * Given an expression, returns a column with that expression
         * or null.
         *
         * @param name Name of level (for descriptive purposes)
         * @param property Property of level (for descriptive purposes, may be
         *     null)
         */
        public Column lookupColumnByExpression(
            RolapSchema.PhysExpr expr,
            boolean create,
            String name,
            String property)
        {
            for (Column column : getColumns()) {
                if (column instanceof Measure) {
                    continue;
                }
                if (column.getExpression().equals(expr)) {
                    return column;
                }
            }
            if (create) {
                if (name == null && expr instanceof RolapSchema.PhysColumn) {
                    name = ((RolapSchema.PhysColumn) expr).name;
                }
                Column column =
                    new RolapStar.Column(
                        property == null
                            ? name
                            : name + " (" + property + ")",
                        this,
                        expr,
                        expr.getDatatype() == null
                            ? Dialect.Datatype.Numeric
                            : expr.getDatatype(),
                        null,
                        // TODO: obsolete nameColumn
                        null,
                        // TODO: obsolete parentColumn
                        null,
                        // TODO: pass in usagePrefix (from DimensionUsage)
                        null,
                        -1,
                        star.nextColumnCount());
                addColumn(column);
                return column;
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

        public RolapSchema.PhysRelation getRelation() {
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
            if (relation instanceof RolapSchema.PhysTable) {
                RolapSchema.PhysTable t = (RolapSchema.PhysTable) relation;
                return t.name;
            } else {
                return null;
            }
        }

        synchronized void makeMeasure(RolapBaseCubeMeasure measure) {
            // Remove assertion to allow cube to be recreated
            // assert lookupMeasureByName(
            //    measure.getCube().getName(), measure.getName()) == null;
            RolapStar.Measure starMeasure =
                new RolapStar.Measure(
                    measure.getName(),
                    measure.getCube().getName(),
                    measure.getAggregator(),
                    this,
                    measure.getExpr(),
                    measure.getDatatype());

            measure.setStarMeasure(starMeasure); // reverse mapping

            if (containsColumn(starMeasure)) {
                star.decrementColumnCount();
            } else {
                addColumn(starMeasure);
            }
        }

        /**
         * Returns a child relation which maps onto a given relation, or null
         * if there is none.
         *
         * @param relation Relation to join to
         * @param joinCondition Join condition
         * @param add Whether to add a child if not found
         *
         * @return Child, or null if not found and add is false
         */
        public Table findChild(
            RolapSchema.PhysRelation relation,
            Condition joinCondition,
            boolean add)
        {
            for (Table child : getChildren()) {
                if (child.relation.equals(relation)) {
                    if (child.joinCondition.equals(joinCondition)) {
                        return child;
                    }
                }
            }
            if (add) {
                Table child =
                    new RolapStar.Table(
                        star, relation, this, joinCondition);
                if (this.children.isEmpty()) {
                    this.children = new ArrayList<Table>();
                }
                this.children.add(child);
                return child;
            } else {
                return null;
            }
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
        public final void addToFrom(
            SqlQuery query,
            boolean failIfExists,
            boolean joinToParent)
        {
            Util.deprecated("use PhysPath.addToFrom", false);
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
                    if (condition.left instanceof RolapSchema.PhysRealColumn) {
                        RolapSchema.PhysRealColumn mcolumn =
                            (RolapSchema.PhysRealColumn) condition.left;
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
            final RolapSchema.PhysExpr left)
        {
            for (Table child : getChildren()) {
                Condition condition = child.joinCondition;
                if (condition != null) {
                    if (condition.left instanceof RolapSchema.PhysRealColumn) {
                        RolapSchema.PhysRealColumn mcolumn =
                            (RolapSchema.PhysRealColumn) condition.left;
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
            return relation.getColumn(columnName, false) != null;
        }
    }

    public static class Condition {
        private static final Logger LOGGER = Logger.getLogger(Condition.class);

        private final RolapSchema.PhysExpr left;
        private final RolapSchema.PhysExpr right;

        Condition(
            RolapSchema.PhysExpr left,
            RolapSchema.PhysExpr right)
        {
            assert left != null;
            assert right != null;

            if (!(left instanceof RolapSchema.PhysRealColumn)) {
                // TODO: Will this ever print?? if not then left should be
                // of type MondrianDef.Column.
                LOGGER.debug(
                    "Condition.left NOT Column: "
                    + left.getClass().getName());
            }
            this.left = left;
            this.right = right;
        }

        Condition(
            RolapSchema.PhysLink link)
        {
            this(
                link.sourceKey.columnList.get(0),
                link.columnList.get(0));
            assert link.sourceKey.columnList.size() == 1
                : "TODO: implement compound keys by obsoleting Condition and"
                  + "using PhysLink";
        }

        public RolapSchema.PhysExpr getLeft() {
            return left;
        }

        public RolapSchema.PhysExpr getRight() {
            return right;
        }

        public String toString(SqlQuery query) {
            Util.deprecated("obsolete query param", false);
            return left.toSql() + " = " + right.toSql();
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
            pw.print(prefix);
            pw.println("Condition:");
            String subprefix = prefix + "  ";

            pw.print(subprefix);
            pw.print("left=");
            pw.println(left.toSql());

            pw.print(subprefix);
            pw.print("right=");
            pw.println(right.toSql());
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
