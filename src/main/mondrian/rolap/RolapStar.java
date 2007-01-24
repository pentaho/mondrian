/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.DataSourceChangeListener;

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
      * This static variable controls the aggregate data cache for all
      * RolapStars. An administrator or tester might selectively enable or
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
                            RolapSchema.flushAllRolapStarCachedAggregations();
                        }
                    }
                }
        );
    }


    private final RolapSchema schema;

    // not final for test purposes
    private DataSource dataSource;

    private final Table factTable;

    /**
     * Maps {@link RolapCube} to a {@link HashMap} which maps
     * {@link RolapLevel} to {@link Column}. The double indirection is
     * necessary because in different cubes, a shared hierarchy might be joined
     * onto the fact table at different levels.
     */
    private final Map<RolapCube, Map<RolapLevel, Column>> mapCubeToMapLevelToColumn;

    /** holds all global aggregations of this star */
    private Map<RolapStarAggregationKey,Aggregation> aggregations;
    
    /** holds all thread local aggregations of this star */
    private ThreadLocal<Map<RolapStarAggregationKey,Aggregation>> 
        localAggregations = 
            new ThreadLocal<Map<RolapStarAggregationKey,Aggregation>>() {
        
            protected Map<RolapStarAggregationKey,Aggregation> initialValue() {
                return new HashMap<RolapStarAggregationKey, Aggregation>();
            }            
        };
        
    /** holds all pending aggregations of this star that are waiting to
     * be pushed into the global cache.  They cannot be pushed yet, because
     * the aggregates in question are currently in use by other threads */
    private Map<RolapStarAggregationKey,Aggregation> pendingAggregations;        
    
    /** holds all requests of aggregations */
    private List<RolapStarAggregationKey> aggregationRequests;
    
    /** holds all requests of aggregations per thread*/
    private ThreadLocal<List<RolapStarAggregationKey>> localAggregationRequests =
        new ThreadLocal<List<RolapStarAggregationKey>>() {
            protected List<RolapStarAggregationKey> initialValue() {
                return new ArrayList<RolapStarAggregationKey>();
            }                                
        };

    /** how many columns (column and columnName) there are */
    private int columnCount;

    private final SqlQuery.Dialect sqlQueryDialect;

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

    /**
     * Creates a RolapStar. Please use
     * {@link RolapSchema.RolapStarRegistry#getOrCreateStar} to create a
     * {@link RolapStar}.
     */
    RolapStar(
            final RolapSchema schema,
            final DataSource dataSource,
            final MondrianDef.Relation fact) {
        this.cacheAggregations = true;
        this.schema = schema;
        this.dataSource = dataSource;
        this.factTable = new RolapStar.Table(this, fact, null, null);

        this.mapCubeToMapLevelToColumn =
            new HashMap<RolapCube, Map<RolapLevel, Column>>();
        this.aggregations = new HashMap<RolapStarAggregationKey, Aggregation>();
        this.pendingAggregations = new HashMap<RolapStarAggregationKey, Aggregation>();
        
        this.aggregationRequests = new ArrayList<RolapStarAggregationKey>();
        
        clearAggStarList();

        sqlQueryDialect = schema.getDialect();        

        changeListener = schema.getDataSourceChangeListener();
    }

    /**
     * The the RolapStar's column count. After a star has been created with all
     * of its columns, this is the number of columns in the star.
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

    public Table getFactTable() {
        return factTable;
    }

    /**
     * Clone an existing SqlQuery to create a new one (this cloning creates one
     * with an empty sql query).
     */
    public SqlQuery getSqlQuery() {
        return new SqlQuery(getSqlQueryDialect());
    }

    /**
     * Get this RolapStar's SQL dialect.
     */
    public SqlQuery.Dialect getSqlQueryDialect() {
        return sqlQueryDialect;
    }

    /**
     * This maps a cube to a Map of level to colunms. Now the only reason
     * the star needs to map via a cube is that more than one cube can
     * share the same star.
     *
     * @param cube Cube
     */
    Map<RolapLevel, Column> getMapLevelToColumn(RolapCube cube) {
        Map<RolapLevel, Column> mapLevelToColumn =
            this.mapCubeToMapLevelToColumn.get(cube);
        if (mapLevelToColumn == null) {
            mapLevelToColumn = new HashMap<RolapLevel, Column>();
            this.mapCubeToMapLevelToColumn.put(cube, mapLevelToColumn);
        }
        return mapLevelToColumn;
    }


    /**
     * This is called only by the RolapCube and is only called if caching is to
     * be turned off. Note that the same RolapStar can be associated with more
     * than on RolapCube. If any one of those cubes has caching turned off, then
     * caching is turned off for all of them.
     *
     * @param b
     */
    void setCacheAggregations(boolean b) {
        // this can only change from true to false
        this.cacheAggregations = b;
        clearCachedAggregations(false);
    }

    /**
     * Does the RolapStar cache aggregates.
     */
    boolean isCacheAggregations() {
        return this.cacheAggregations;
    }

    /**
     * Clear the aggregate cache. This only does something if this star has
     * caching set to off.
     *
     * @param forced if true, then the cached aggregations are cleared
     * regardless of any other settings.  When set to false, only cache
     * from the current thread context is cleared.
     */
    void clearCachedAggregations(boolean forced) {        
        if (forced || (! this.cacheAggregations) || RolapStar.disableCaching) {

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("RolapStar.clearCachedAggregations: schema=");
                buf.append(schema.getName());
                buf.append(", star=");
                buf.append(getFactTable().getAlias());
                LOGGER.debug(buf.toString());
            }
            
            if (forced) { 
                synchronized(aggregations) {
                    aggregations.clear();
                }
                localAggregations.get().clear(); 
            }
            else {
                /* Only clear aggregation cache for the currect thread context */
                localAggregations.get().clear();
            }
        }
    }

    /**
     * Looks up an aggregation or creates one if it does not exist in an
     * atomic (synchronized) operation.
     * 
     * When a new aggregation is created, it is marked as thread local.
     * 
     * @param bitKey this is the contrained column bitkey
     */
    public Aggregation lookupOrCreateAggregation(
            final BitKey bitKey) {
    
        Aggregation aggregation = lookupAggregation(bitKey);
                
        if (aggregation == null) {
            aggregation = new Aggregation(this, bitKey);
            RolapStarAggregationKey aggregationKey = 
                new RolapStarAggregationKey(bitKey);
                
            this.localAggregations.get().put(aggregationKey, aggregation);          
            
            // Let the change listener get the opportunity to register the 
            // first time the aggregation is used
            if ((this.cacheAggregations) && (!RolapStar.disableCaching)) {
                if (changeListener != null) {
                    Util.discard(changeListener.isAggregationChanged(aggregation));
                }
            }
        }
        return aggregation;        
    }
    

    /**
     * Looks for an existing aggregation over a given set of columns, or
     * returns <code>null</code> if there is none.
     * 
     * Thread local cache is taken first.
     *
     * <p>Must be called from synchronized context.
     */
    public Aggregation lookupAggregation(BitKey bitKey) {
        
        Aggregation aggregation = null;
        
        // First try thread local cache 
        RolapStarAggregationKey aggregationKey = 
            new RolapStarAggregationKey(bitKey);
        aggregation = localAggregations.get().get(aggregationKey);
        if (aggregation != null) {
            return aggregation;
        }

        if ((this.cacheAggregations) && (!RolapStar.disableCaching)) {            
            // Look in global cache
            synchronized(aggregations) {
                aggregation = aggregations.get(aggregationKey);
                if (aggregation != null) {
                    // Keep track of global aggregates that a query is using
                    recordAggregationRequest(bitKey);
                }
            }            
        }
        
        return aggregation;
    }
    
    /**
     * Checks whether an aggregation has changed since the last the time
     * loaded.
     * 
     * If so, a new thread local aggregation will be made and added after
     * the query has finished.
     * 
     * This function should be called before a query is executed and afterwards
     * the function pushAggregateModificationsToGlobalCache() should
     * be called.
     * 
     */
    public void checkAggregateModifications() {                    
        if (changeListener != null) {
            if ((this.cacheAggregations) && (!RolapStar.disableCaching)) {
                synchronized(aggregations) {
                                        
                    Iterator<Map.Entry<RolapStarAggregationKey, Aggregation>> 
                            it = aggregations.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<RolapStarAggregationKey, Aggregation> e = 
                            it.next();
                        RolapStarAggregationKey aggregationKey = e.getKey();
                        
                        Aggregation aggregation = e.getValue(); 
                        if (changeListener.isAggregationChanged(aggregation)) {
                            // Create new thread local aggregation
                            // This thread will renew aggregations
                            // And these will be checked in if all queries
                            // that are currently using these aggregates
                            // are finished
                            aggregation = new Aggregation(this, 
                                            aggregationKey.getBitKey());
                            RolapStarAggregationKey localAggregationKey = 
                                new RolapStarAggregationKey(
                                        aggregationKey.getBitKey());
                                                    
                            localAggregations.get().put(localAggregationKey, 
                                    aggregation);                                                               
                        }                                                    
                    }
                }
            }    
        }            
    }
    
    /**
     * 
     * Checks if changed modifications may be pushed into global cache.
     * 
     * It will check whether there are other running queries that are
     * using the requested modifications.  If this is the case, modifications
     * are not pushed yet.
     * 
     */
    public void pushAggregateModificationsToGlobalCache() {
        // Need synchronized access to both aggregationRequests as to
        // aggregations, synchronize this instead
        synchronized(this) {        
            if ((this.cacheAggregations) && (!RolapStar.disableCaching)) {
                
                // Push pending modifications other thread could not push
                // to global cache, because it was in use
                Iterator<Map.Entry<RolapStarAggregationKey, Aggregation>> 
                    it = pendingAggregations.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<RolapStarAggregationKey, Aggregation> e = 
                        it.next();
                    RolapStarAggregationKey aggregationKey = e.getKey();                    
                    Aggregation aggregation = e.getValue();
                    // In case this aggregation is not requested by anyone
                    // this aggregation may be pushed into global cache
                    // otherwise put it in pending cache, that will be pushed
                    // when another query finishes
                    if (!isAggregationRequested(aggregationKey.getBitKey())) {
                        pushAggregateModification(aggregationKey, aggregation,
                                aggregations);
                        it.remove();
                    }                                           
                }
                // Push thread local modifications                 
                it = localAggregations.get().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<RolapStarAggregationKey, Aggregation> e = 
                        it.next();
                    RolapStarAggregationKey aggregationKey = e.getKey();                    
                    Aggregation aggregation = e.getValue();
                    // In case this aggregation is not requested by anyone
                    // this aggregation may be pushed into global cache
                    // otherwise put it in pending cache, that will be pushed
                    // when another query finishes
                    if (!isAggregationRequested(aggregationKey.getBitKey())) {
                        pushAggregateModification(aggregationKey, aggregation,
                                aggregations);                        
                    } 
                    else {
                        pushAggregateModification(aggregationKey, aggregation,
                                pendingAggregations);
                    }                                            
                }
                localAggregations.get().clear();
            }
            // Clear own aggregation requests
            clearAggregationRequests();
        }                    
    }    
    /* Push aggregations in destination aggregations, replacing older
     * entries.
     */
    private void pushAggregateModification(
            RolapStarAggregationKey localAggregationKey, 
            Aggregation aggregation,
            Map<RolapStarAggregationKey,Aggregation> destAggregations) {                    
        if ((this.cacheAggregations) && (!RolapStar.disableCaching)) {
            synchronized(destAggregations) {
                
                boolean found = false;
                Iterator<Map.Entry<RolapStarAggregationKey, Aggregation>> 
                        it = destAggregations.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<RolapStarAggregationKey, Aggregation> e = 
                        it.next();
                    RolapStarAggregationKey aggregationKey = e.getKey();
                    
                    if (localAggregationKey.getBitKey()
                                .equals(aggregationKey.getBitKey())) {
                        
                        if (localAggregationKey.getTimeStamp()
                                .after(aggregationKey.getTimeStamp())) {    
                            
                            it.remove();
                        } else {
                            // Entry is newer, do not replace
                            found = true;
                        }
                        break;
                    }                                                    
                }
                if (!found) {
                    destAggregations.put(localAggregationKey, aggregation);
                }
            }
        }                
    }      
    
    /* Record global cache requests per thread */
    private void recordAggregationRequest(BitKey bitKey) {
        RolapStarAggregationKey aggregationKey = 
            new RolapStarAggregationKey(bitKey);        
        synchronized(aggregationRequests) {
            aggregationRequests.add(aggregationKey);
        }     
        // Store own request for cleanup afterwards
        localAggregationRequests.get().add(aggregationKey);
    }
    
    /* Checks whether an aggregation is requested by another thread */
    private boolean isAggregationRequested(BitKey bitKey) {
        
        synchronized(aggregationRequests) {
            RolapStarAggregationKey aggregationKey = 
                new RolapStarAggregationKey(bitKey);        
            
            return aggregationRequests.contains(aggregationKey);      
        }
    }    
    
    /**
     * Clear the aggregation requests from the current thread
     *
     */
    private void clearAggregationRequests() {
        synchronized(aggregationRequests) {
            
            for (RolapStarAggregationKey aggregationKey : 
                localAggregationRequests.get()) {
                
                // Remove first occurence
                aggregationRequests.remove(aggregationKey);
            }            
            localAggregationRequests.get().clear();
        }                
    }
        
    /**
     * Allocates a connection to the underlying RDBMS.
     *
     * <p>The client MUST close connection returned by this method; use the
     * <code>try ... finally</code> idiom to be sure of this.
     */
    public Connection getJdbcConnection() {
        Connection jdbcConnection;
        try {
            jdbcConnection = dataSource.getConnection();
        } catch (SQLException e) {
            throw Util.newInternal(
                e, "Error while creating connection from data source");
        }
        return jdbcConnection;
    }

    /** For testing purposes only.  */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

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

    public Column[] lookupColumns(BitKey bitKey) {
        List<Column> list = new ArrayList<Column>();
        factTable.collectColumns(bitKey, list);
        return (Column[]) list.toArray(new Column[0]);
    }

    /**
     * This is used by TestAggregationManager only.
     */
    public Column lookupColumn(String tableAlias, String columnName) {
        final Table table = factTable.findDescendant(tableAlias);
        return (table == null) ? null : table.lookupColumn(columnName);
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
            MondrianDef.Column joinColumn) {
        if (joinColumn == null) {
            columnList.addAll(table.columnList);
        }
        for (Table child : table.children) {
            if (joinColumn == null ||
                child.getJoinCondition().left.equals(joinColumn)) {
                collectColumns(columnList, child, null);
            }
        }
    }

    /**
     * Reads a cell of <code>measure</code>, where <code>columns</code> are
     * constrained to <code>values</code>.  <code>values</code> must be the
     * same length as <code>columns</code>; null values are left unconstrained.
     */
    Object getCell(CellRequest request) {
        Connection jdbcConnection = getJdbcConnection();
        try {
            return getCell(request, jdbcConnection);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }

    private Object getCell(CellRequest request, Connection jdbcConnection) {
        Measure measure = request.getMeasure();
        Column[] columns = request.getConstrainedColumns();
        Object[] values = request.getSingleValues();
        Util.assertTrue(columns.length == values.length);
        SqlQuery sqlQuery = getSqlQuery();
        // add measure
        Util.assertTrue(measure.getTable() == factTable);
        factTable.addToFrom(sqlQuery, true, true);
        sqlQuery.addSelect(
            measure.aggregator.getExpression(measure.generateExprString(sqlQuery)));
        // add constraining dimensions
        for (int i = 0; i < columns.length; i++) {
            Object value = values[i];
            if (value == null) {
                continue; // not constrained
            }
            Column column = columns[i];
            Table table = column.getTable();
            if (table.isFunky()) {
                // this is a funky dimension -- ignore for now
                continue;
            }
            table.addToFrom(sqlQuery, true, true);
        }
        String sql = sqlQuery.toString();
        ResultSet resultSet = null;
        try {
            resultSet = RolapUtil.executeQuery(
                    jdbcConnection, sql, "RolapStar.getCell");
            Object o = null;
            if (resultSet.next()) {
                o = resultSet.getObject(1);
            }
            if (o == null) {
                o = Util.nullValue; // convert to placeholder
            }
            return o;
        } catch (SQLException e) {
            throw Util.newInternal(e,
                    "while computing single cell; sql=[" + sql + "]");
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.getStatement().close();
                    resultSet.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private boolean containsColumn(String tableName, String columnName) {
        final Connection jdbcConnection = getJdbcConnection();
        try {
            final DatabaseMetaData metaData = jdbcConnection.getMetaData();
            final ResultSet columns =
                metaData.getColumns(null, null, tableName, columnName);
            final boolean hasNext = columns.next();
            return hasNext;
        } catch (SQLException e) {
            throw Util.newInternal("Error while retrieving metadata for table '" +
                            tableName + "', column '" + columnName + "'");
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
    }

    /**
     * A column in a star schema.
     */
    public static class Column {
        private final Table table;
        private final MondrianDef.Expression expression;
        private final SqlQuery.Datatype datatype;
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

        private int cardinality = -1;

        private Column(
            String name,
            Table table,
            MondrianDef.Expression expression,
            SqlQuery.Datatype datatype)
        {
            this(name, table, expression, datatype, null, null, null);
        }

        private Column(
            String name,
            Table table,
            MondrianDef.Expression expression,
            SqlQuery.Datatype datatype,
            Column nameColumn,
            Column parentColumn,
            String usagePrefix)
        {
            this.name = name;
            this.table = table;
            this.expression = expression;
            this.datatype = datatype;
            this.bitPosition = table.star.nextColumnCount();
            this.nameColumn = nameColumn;
            this.parentColumn = parentColumn;
            this.usagePrefix = usagePrefix;
            if (nameColumn != null) {
                nameColumn.isNameColumn = true;
            }
        }

        public boolean equals(Object obj) {
            if (! (obj instanceof RolapStar.Column)) {
                return false;
            }
            RolapStar.Column other = (RolapStar.Column) obj;
            // Note: both columns have to be from the same table
            return (other.table == this.table) &&
                   other.expression.equals(this.expression) &&
                   (other.datatype == this.datatype) &&
                   other.name.equals(this.name);
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

        public int getCardinality() {
            if (cardinality == -1) {
                Connection jdbcConnection = getStar().getJdbcConnection();
                try {
                    cardinality = getCardinality(jdbcConnection);
                } finally {
                    try {
                        jdbcConnection.close();
                    } catch (SQLException e) {
                        //ignore
                    }
                }
            }
            return cardinality;
        }

        private int getCardinality(Connection jdbcConnection) {
            SqlQuery sqlQuery = getSqlQuery();
            if (sqlQuery.getDialect().allowsCountDistinct()) {
                // e.g. "select count(distinct product_id) from product"
                sqlQuery.addSelect("count(distinct "
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
                throw Util.newInternal("Cannot compute cardinality: this " +
                    "database neither supports COUNT DISTINCT nor SELECT in " +
                    "the FROM clause.");
            }
            String sql = sqlQuery.toString();
            ResultSet resultSet = null;
            try {
                resultSet = RolapUtil.executeQuery(
                        jdbcConnection, sql,
                        "RolapStar.Column.getCardinality");
                Util.assertTrue(resultSet.next());
                return resultSet.getInt(1);
            } catch (SQLException e) {
                throw Util.newInternal(e,
                        "while counting distinct values of column '" +
                        expression.getGenericExpression() +
                        "'; sql=[" + sql + "]");
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.getStatement().close();
                        resultSet.close();
                    }
                } catch (SQLException e) {
                    // ignore
                }
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
            String expr,
            ColumnConstraint[] constraints,
            SqlQuery.Datatype datatype,
            SqlQuery.Dialect dialect)
        {
            if (constraints.length == 1) {
                final ColumnConstraint constraint = constraints[0];
                Object key = constraint.getValue();
                if (key != RolapUtil.sqlNullValue) {
                    StringBuilder buf = new StringBuilder(64);
                    buf.append(expr);
                    buf.append(" = ");
                    dialect.quote(buf, key, datatype);
                    return buf.toString();
                }
            }
            int notNullCount = 0;
            StringBuilder buf = new StringBuilder(expr);
            buf.append(" in (");
            Object lastNotNull = null;
            for (int i = 0; i < constraints.length; i++) {
                final ColumnConstraint constraint = constraints[i];
                Object key = constraint.getValue();
                if (key == RolapUtil.sqlNullValue) {
                    continue;
                }
                lastNotNull = key;
                if (notNullCount > 0) {
                    buf.append(", ");
                }
                ++notNullCount;
                dialect.quote(buf, key, datatype);
            }
            buf.append(')');
            if (notNullCount < constraints.length) {
                // There was at least one null.
                switch (notNullCount) {
                case 0:
                    // Special case -- there were no values besides null.
                    // Return, for example, "x is null".
                    return expr + " is null";
                case 1:
                    // Special case -- one not-null value, and null, for
                    // example "(x = 1 or x is null)".
                    buf.setLength(0);
                    buf.append('(');
                    buf.append(expr);
                    buf.append(" = ");
                    dialect.quote(buf, lastNotNull, datatype);
                    buf.append(" or ");
                    buf.append(expr);
                    buf.append(" is null)");
                    return buf.toString();
                default:
                    // Nulls and values, for example,
                    // "(x in (1, 2) or x IS NULL)".
                    final String str = buf.toString();
                    buf.setLength(0);
                    buf.append('(');
                    buf.append(str);
                    buf.append(" or ");
                    buf.append(expr);
                    buf.append(" is null)");
                    return buf.toString();
                }
            } else {
                // No nulls. Return, for example, "x in (1, 2, 3)".
                return buf.toString();
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
         * Prints this table and its children.
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

        public SqlQuery.Datatype getDatatype() {
            return datatype;
        }
    }

    /**
     * Definition of a measure in a star schema.
     *
     * <p>A measure is basically just a column; except that its
     * {@link #aggregator} defines how it is to be rolled up.
     */
    public static class Measure extends Column {
        private final RolapAggregator aggregator;

        private Measure(
                String name,
                RolapAggregator aggregator,
                Table table,
                MondrianDef.Expression expression,
                SqlQuery.Datatype datatype) {
            super(name, table, expression, datatype);
            this.aggregator = aggregator;
        }

        public RolapAggregator getAggregator() {
            return aggregator;
        }

        public boolean equals(Object obj) {
            if (! super.equals(obj)) {
                return false;
            }
            if (! (obj instanceof RolapStar.Measure)) {
                return false;
            }
            RolapStar.Measure other = (RolapStar.Measure) obj;
            // Note: both measure have to have the same aggregator
            return (other.aggregator == this.aggregator);
        }

        /**
         * Prints this table and its children.
         */
        public void print(PrintWriter pw, String prefix) {
            SqlQuery sqlQuery = getSqlQuery();
            pw.print(prefix);
            pw.print(getName());
            pw.print(" (");
            pw.print(getBitPosition());
            pw.print("): ");
            pw.print(aggregator.getExpression(generateExprString(sqlQuery)));
        }
    }

    /**
     * Definition of a table in a star schema.
     *
     * <p>A 'table' is defined by a
     * {@link mondrian.olap.MondrianDef.Relation} so may, in fact, be a view.
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
                Condition joinCondition) {
            this.star = star;
            this.relation = relation;
            Util.assertTrue(
                    relation instanceof MondrianDef.Table ||
                    relation instanceof MondrianDef.View,
                    "todo: allow dimension which is not a Table or View, [" +
                    relation + "]");
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
            for (Iterator<Column> it = getColumns().iterator(); it.hasNext(); ) {
                Column column = it.next();
                if (column.getExpression() instanceof MondrianDef.Column) {
                    MondrianDef.Column columnExpr =
                        (MondrianDef.Column) column.getExpression();
                    if (columnExpr.name.equals(columnName)) {
                        l.add(column);
                    }
                }
            }
            return (Column[]) l.toArray(new Column[0]);
        }

        public Column lookupColumn(String columnName) {
            for (Iterator<Column> it = getColumns().iterator(); it.hasNext(); ) {
                Column column = it.next();
                if (column.getExpression() instanceof MondrianDef.Column) {
                    MondrianDef.Column columnExpr =
                        (MondrianDef.Column) column.getExpression();
                    if (columnExpr.name.equals(columnName)) {
                        return column;
                    }
                }
            }
            return null;
        }

        /**
         * Given a MondrianDef.Expression return a column with that expression
         * or null.
         */
        public Column lookupColumnByExpression(MondrianDef.Expression xmlExpr) {
            for (Iterator<Column> it = getColumns().iterator(); it.hasNext(); ) {
                Column column = it.next();
                if (column instanceof RolapStar.Measure) {
                    continue;
                }
                if (column.getExpression().equals(xmlExpr)) {
                    return column;
                }
            }
            return null;
        }

        public boolean containsColumn(Column column) {
            for (Iterator<Column> it = getColumns().iterator(); it.hasNext(); ) {
                Column other = it.next();
                if (column.equals(other)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Look up a {@link Measure} by its name.
         * Returns null if not found.
         */
        public Measure lookupMeasureByName(String name) {
            for (Iterator<Column> it = getColumns().iterator(); it.hasNext(); ) {
                Column column = it.next();
                if (column instanceof Measure) {
                    Measure measure = (Measure) column;
                    if (measure.getName().equals(name)) {
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
            RolapStar.Measure starMeasure = new RolapStar.Measure(
                measure.getName(),
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
         *
         * @param cube
         * @param level
         * @param parentColumn
         */
        synchronized Column makeColumns(
                RolapCube cube,
                RolapLevel level,
                Column parentColumn,
                String usagePrefix) {

            Column nameColumn = null;
            if (level.getNameExp() != null) {
                // make a column for the name expression
                nameColumn = makeColumnForLevelExpr(
                    cube,
                    level,
                    level.getName(),
                    level.getNameExp(),
                    SqlQuery.Datatype.String,
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
                nameColumn,
                parentColumn,
                usagePrefix);

            if (column != null) {
                Map<RolapLevel, Column> map = star.getMapLevelToColumn(cube);
                map.put(level, column);
            }

            return column;
        }

        private Column makeColumnForLevelExpr(
                RolapCube cube,
                RolapLevel level,
                String name,
                MondrianDef.Expression xmlExpr,
                SqlQuery.Datatype datatype,
                Column nameColumn,
                Column parentColumn,
                String usagePrefix) {
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

            RolapStar.Column column = null;
            if (c != null) {
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
                    nameColumn,
                    parentColumn,
                    usagePrefix);
                addColumn(column);
            }
            return column;
        }


        /**
         * Extends this 'leg' of the star by adding <code>relation</code>
         * joined by <code>joinCondition</code>. If the same expression is
         * already present, does not create it again.
         */
        synchronized Table addJoin(MondrianDef.Relation relation,
                                   RolapStar.Condition joinCondition) {
            if (relation instanceof MondrianDef.Table ||
                    relation instanceof MondrianDef.View) {
                RolapStar.Table starTable = findChild(relation, joinCondition);
                if (starTable == null) {
                    starTable = new RolapStar.Table(star, relation, this,
                        joinCondition);
                    if (this.children.isEmpty()) {
                        this.children = new ArrayList<Table>();
                    }
                    this.children.add(starTable);
                }
                return starTable;

            } else if (relation instanceof MondrianDef.Join) {
                MondrianDef.Join join = (MondrianDef.Join) relation;
                RolapStar.Table leftTable = addJoin(join.left, joinCondition);
                String leftAlias = join.leftAlias;
                if (leftAlias == null) {
                    leftAlias = join.left.getAlias();
                    if (leftAlias == null) {
                        throw Util.newError(
                                "missing leftKeyAlias in " + relation);
                    }
                }
                assert leftTable.findAncestor(leftAlias) == leftTable;
                // switch to uniquified alias
                leftAlias = leftTable.getAlias();

                String rightAlias = join.rightAlias;
                if (rightAlias == null) {
                    rightAlias = join.right.getAlias();
                    if (rightAlias == null) {
                        throw Util.newError(
                                "missing rightKeyAlias in " + relation);
                    }
                }
                joinCondition = new RolapStar.Condition(
                        new MondrianDef.Column(leftAlias, join.leftKey),
                        new MondrianDef.Column(rightAlias, join.rightKey));
                RolapStar.Table rightTable = leftTable.addJoin(
                        join.right, joinCondition);
                return rightTable;

            } else {
                throw Util.newInternal("bad relation type " + relation);
            }
        }

        /**
         * Returns a child relation which maps onto a given relation, or null if
         * there is none.
         */
        public Table findChild(
            MondrianDef.Relation relation,
            Condition joinCondition) {
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
            boolean joinToParent) {
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
                final String columnName) {
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
                final MondrianDef.Expression left) {
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

            for (Iterator<Column> it = getColumns().iterator(); it.hasNext(); ) {
                Column column = it.next();
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
            if (relation instanceof MondrianDef.Table) {
                return star.containsColumn(((MondrianDef.Table) relation).name,
                    columnName);
            } else {
                // todo: Deal with join and view.
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

        Condition(MondrianDef.Expression left,
                  MondrianDef.Expression right) {
            assert left != null;
            assert right != null;

            if (!(left instanceof MondrianDef.Column)) {
                // TODO: Will this ever print?? if not then left should be
                // of type MondrianDef.Column.
                LOGGER.debug("Condition.left NOT Column: "
                    + left.getClass().getName());
            }
            this.left = left;
            this.right = right;
        }
        public MondrianDef.Expression getLeft() {
            return left;
        }
        public MondrianDef.Expression getRight() {
            return right;
        }
        public String toString(SqlQuery query) {
            return left.getExpression(query) + " = " + right.getExpression(query);
        }
        public int hashCode() {
            return left.hashCode() ^ right.hashCode();
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Condition)) {
                return false;
            }
            Condition that = (Condition) obj;
            return (this.left.equals(that.left) &&
                    this.right.equals(that.right));
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
     * @return Returns the Data source change listener.
     */
    public DataSourceChangeListener getChangeListener() {
        return changeListener;
    }

    /**
     * @param changeListener The Data source change listener to set.
     */
    public void setChangeListener(DataSourceChangeListener changeListener) {
        this.changeListener = changeListener;
    }
}
/* 
 * Class to create an aggregation key for the cache that is thread based.
 */
class RolapStarAggregationKey {
    private BitKey bitKey;
    private Timestamp timeStamp;
    /*
     * Creates an aggregation key, specify whether the cache is local 
     * to the thread
     */
    RolapStarAggregationKey(final BitKey bitKey) {
        this.bitKey = bitKey.copy(); 
        this.timeStamp = new Timestamp(System.currentTimeMillis());
    }
    public int hashCode() {    
        int c = 1;
        if (bitKey != null) {
            c = bitKey.hashCode();
        }
        return c;        
    }    
    public boolean equals(Object other) {
        if (other instanceof RolapStarAggregationKey) {
            RolapStarAggregationKey aggregationKey = 
                (RolapStarAggregationKey) other;
            return bitKey.equals(aggregationKey.bitKey);
        } else {
            return false;
        }
    }
    public String toString() {
        return bitKey.toString() + " time_stamp = " + timeStamp.toString();
    }    
    /**
     * @return Returns the bitKey.
     */
    public BitKey getBitKey() {
        return bitKey;
    }
    /**
     * @param bitKey The bitKey to set.
     */
    public void setBitKey(BitKey bitKey) {
        this.bitKey = bitKey;
    }
    /**
     * @return Returns the time stamp the aggregation is created.
     */
    public Timestamp getTimeStamp() {
        return timeStamp;
    }
    /**
     * @param timeStamp the time stamp the aggregation is created.
     */
    public void setTimeStamp(Timestamp timeStamp) {
        this.timeStamp = timeStamp;
    }    
};
// End RolapStar.java
