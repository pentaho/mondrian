/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
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
 **/
public class RolapStar {

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
                            RolapSchema.flushAllRolapStarCaches();
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
    private final Map mapCubeToMapLevelToColumn;

    /** holds all aggregations of this star */
    private Map aggregations;

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
    private List aggStars;

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

        this.mapCubeToMapLevelToColumn = new HashMap();
        this.aggregations = new HashMap();

        clearAggStarList();

        sqlQueryDialect = makeSqlQueryDialect();
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
        aggStars = Collections.EMPTY_LIST;
    }

    /**
     * Internally the AggStars are added in sort order, smallest row count
     * to biggest where ties do not matter.
     */
    public void addAggStar(AggStar aggStar) {
        if (aggStars == Collections.EMPTY_LIST) {
            // if this is NOT a LinkedList, then the insertion time is longer.
            aggStars = new LinkedList();
            aggStars.add(aggStar);

        } else {
            // size
            int size = aggStar.getSize();
            ListIterator lit = aggStars.listIterator();
            while (lit.hasNext()) {
                AggStar as = (AggStar) lit.next();
                if (as.getSize() >= size) {
                    break;
                }
            }
            lit.previous();
            lit.add(aggStar);
        }
    }

    /**
     * Set the agg star list to empty.
     */
    void clearAggStarList() {
        aggStars = Collections.EMPTY_LIST;
    }

    /**
     * Reorder the list of aggregate stars. This should be called if the
     * algorithm used to order the AggStars has been changed.
     */
    public void reOrderAggStarList() {
        // the order of these two lines is important
        List l = aggStars;
        clearAggStarList();

        for (Iterator it = l.iterator(); it.hasNext(); ) {
            AggStar aggStar = (AggStar) it.next();
            addAggStar(aggStar);
        }
    }

    /**
     * Returns this RolapStar's aggregate table AggStars, ordered in ascending
     * order of size.
     */
    public List getAggStars() {
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
     * Get this RolapStar's RolapSchema's Sql Dialect.
     */
    public SqlQuery.Dialect getSqlQueryDialect() {
        return sqlQueryDialect;
    }

    /**
     * Make an SqlQuery from a jdbc connection.
     */
    private SqlQuery.Dialect makeSqlQueryDialect() {
        Connection conn = getJdbcConnection();
        try {
            return SqlQuery.Dialect.create(conn.getMetaData());
        } catch (SQLException e) {
            throw Util.newInternal(
                e, "Error while creating SqlQuery from connection");
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }

    /**
     * This maps a cube to a Map of level to colunms. Now the only reason
     * the star needs to map via a cube is that more than one cube can
     * share the same star.
     *
     * @param cube
     */
    Map getMapLevelToColumn(RolapCube cube) {
        Map mapLevelToColumn = (Map) this.mapCubeToMapLevelToColumn.get(cube);
        if (mapLevelToColumn == null) {
            mapLevelToColumn = new HashMap();
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
        clearCache();
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
     */
    void clearCache() {
        if (! this.cacheAggregations || RolapStar.disableCaching) {
            aggregations.clear();
        }
    }

    /**
     * Looks up an aggregation or creates one if it does not exist in an
     * atomic (synchronized) operation.
     */
    public Aggregation lookupOrCreateAggregation(final BitKey bitKey) {
        synchronized (aggregations) {
            Aggregation aggregation = lookupAggregation(bitKey);
            if (aggregation == null) {
                aggregation = new Aggregation(this, bitKey);
                this.aggregations.put(bitKey, aggregation);
            }
            return aggregation;
        }
    }

    /**
     * Looks for an existing aggregation over a given set of columns, or
     * returns <code>null</code> if there is none.
     *
     * <p>Must be called from synchronized context.
     */
    public Aggregation lookupAggregation(BitKey bitKey) {
        synchronized (aggregations) {
            return (Aggregation) aggregations.get(bitKey);
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

    /** For testing purposes only.  **/
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
        List list = new ArrayList();
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
    public List getAliasList() {
        List aliasList = new ArrayList();
        if (factTable != null) {
            collectAliases(aliasList, factTable);
        }
        return aliasList;
    }

    /**
     * Finds all of the table aliases in a table and its children.
     */
    private static void collectAliases(List aliasList, Table table) {
        aliasList.add(table.getAlias());
        for (int i = 0; i < table.children.size(); i++) {
            Table child = (Table) table.children.get(i);
            collectAliases(aliasList, child);
        }
    }

    /**
     * Collects all columns in this table and its children.
     * If <code>joinColumn</code> is specified, only considers child tables
     * joined by the given column.
     */
    public static void collectColumns(
            Collection columnList,
            Table table,
            MondrianDef.Column joinColumn) {
        if (joinColumn == null) {
            columnList.addAll(table.columnList);
        }
        for (int i = 0; i < table.children.size(); i++) {
            Table child = (Table) table.children.get(i);
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
     **/
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
        Column[] columns = request.getColumns();
        Object[] values = request.getSingleValues();
        Util.assertTrue(columns.length == values.length);
        SqlQuery sqlQuery = getSqlQuery();
        // add measure
        Util.assertTrue(measure.getTable() == factTable);
        factTable.addToFrom(sqlQuery, true, true);
        sqlQuery.addSelect(
            measure.aggregator.getExpression(measure.getExpression(sqlQuery)));
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
        print(pw, "");
        pw.flush();
        return sw.toString();
    }

    public void print(PrintWriter pw, String prefix) {
        pw.print(prefix);
        pw.println("RolapStar:");
        String subprefix = prefix + "  ";
        factTable.print(pw, subprefix);

        for (Iterator it = getAggStars().iterator(); it.hasNext(); ) {
            AggStar aggStar = (AggStar) it.next();
            aggStar.print(pw, subprefix);
        }
    }

    /**
     * A column in a star schema.
     */
    public static class Column {
        private final Table table;
        private final MondrianDef.Expression expression;
        private final boolean isNumeric;
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

        private Column(String name,
                       Table table,
                       MondrianDef.Expression expression,
                       boolean isNumeric) {
            this(name, table, expression, isNumeric, null, null, null);
        }
        private Column(String name,
                       Table table,
                       MondrianDef.Expression expression,
                       boolean isNumeric,
                       Column nameColumn,
                       Column parentColumn,
                       String usagePrefix
                       ) {
            this.name = name;
            this.table = table;
            this.expression = expression;
            this.isNumeric = isNumeric;
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
                   (other.isNumeric == this.isNumeric) &&
                   other.name.equals(this.name);
        }

        public String getName() {
            return name;
        }
        public boolean isNumeric() {
            return isNumeric;
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

        public String getExpression(SqlQuery query) {
            return expression.getExpression(query);
        }

        private static void quoteValue(
                Object o,
                StringBuffer buf,
                boolean isNumeric) {
            String s = o.toString();
            if (isNumeric) {
                buf.append(s);
            } else {
                if (s == null) {
                    buf.append("NULL");
                } else {
                    Util.singleQuoteString(s, buf);
                }
            }
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
                    + getExpression(sqlQuery) + ")");

                // no need to join fact table here
                table.addToFrom(sqlQuery, true, false);
            } else if (sqlQuery.getDialect().allowsFromQuery()) {
                // Some databases (e.g. Access) don't like 'count(distinct)',
                // so use, e.g., "select count(*) from (select distinct
                // product_id from product)"
                SqlQuery inner = sqlQuery.cloneEmpty();
                inner.setDistinct(true);
                inner.addSelect(getExpression(inner));
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
         * <li>String values: <code>foo.bar in ('a', 'b', 'c')</code></li></ul>
         */
        public static String createInExpr(
                String expr,
                ColumnConstraint[] constraints,
                boolean isNumeric) {
            if (constraints.length == 1) {
                final ColumnConstraint constraint = constraints[0];
                Object key = constraint.getValue();
                if (key != RolapUtil.sqlNullValue) {
                    StringBuffer buf = new StringBuffer(64);
                    buf.append(expr);
                    buf.append(" = ");
                    quoteValue(key, buf, isNumeric);
                    return buf.toString();
                }
            }
            int notNullCount = 0;
            StringBuffer sb = new StringBuffer(expr);
            sb.append(" in (");
            for (int i = 0; i < constraints.length; i++) {
                final ColumnConstraint constraint = constraints[i];
                Object key = constraint.getValue();
                if (key == RolapUtil.sqlNullValue) {
                    continue;
                }
                if (notNullCount > 0) {
                    sb.append(", ");
                }
                ++notNullCount;
                quoteValue(key, sb, isNumeric);
            }
            sb.append(')');
            if (notNullCount < constraints.length) {
                // There was at least one null.
                StringBuffer buf;
                switch (notNullCount) {
                case 0:
                    // Special case -- there were no values besides null.
                    // Return, for example, "x is null".
                    return expr + " is null";
                case 1:
                    // Special case -- one not-null value, and null, for
                    // example "(x = 1 or x is null)".
                    buf = new StringBuffer(64);
                    buf.append('(');
                    buf.append(expr);
                    buf.append(" = ");
                    quoteValue(constraints[0].getValue(), buf, isNumeric);
                    buf.append(" or ");
                    buf.append(expr);
                    buf.append(" is null)");
                    return buf.toString();
                default:
                    // Nulls and values, for example,
                    // "(x in (1, 2) or x IS NULL)".
                    buf = new StringBuffer(64);
                    buf.append('(');
                    buf.append(sb.toString());
                    buf.append(" or ");
                    buf.append(expr);
                    buf.append(" is null)");
                    return buf.toString();
                }
            } else {
                // No nulls. Return, for example, "x in (1, 2, 3)".
                return sb.toString();
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
            pw.print(getExpression(sqlQuery));
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
                boolean isNumeric) {
            super(name, table, expression, isNumeric);
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
            pw.print(aggregator.getExpression(getExpression(sqlQuery)));
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
        private final List columnList;
        private final Table parent;
        private List children;
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
            this.columnList = new ArrayList();
            this.children = Collections.EMPTY_LIST;
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
        private void collectColumns(BitKey bitKey, List list) {
            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column column = (Column) it.next();
                if (bitKey.get(column.getBitPosition())) {
                    list.add(column);
                }
            }
            for (Iterator it = getChildren().iterator(); it.hasNext(); ) {
                Table child = (Table) it.next();
                child.collectColumns(bitKey, list);
            }
        }

        /**
         * Returns an array of all columns in this star with a given name.
         */
        public Column[] lookupColumns(String columnName) {
            List l = new ArrayList();
            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column column = (Column) it.next();
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
            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column column = (Column) it.next();
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
            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column column = (Column) it.next();
                if (column.getExpression().equals(xmlExpr)) {
                    return column;
                }
            }
            return null;
        }

        public boolean containsColumn(Column column) {
            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column other = (Column) it.next();
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
            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column column = (Column) it.next();
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
            List aliasList = star.getAliasList();
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
        synchronized void makeMeasure(RolapStoredMeasure storedMeasure) {
            RolapStar.Measure measure = new RolapStar.Measure(
                storedMeasure.getName(),
                storedMeasure.getAggregator(),
                this,
                storedMeasure.getMondrianDefExpression(),
                true);

            storedMeasure.setStarMeasure(measure); // reverse mapping

            if (containsColumn(measure)) {
                star.decrementColumnCount();
            } else {
                addColumn(measure);
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
                    false,
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
                (level.getFlags() & RolapLevel.NUMERIC) != 0,
                nameColumn,
                parentColumn,
                usagePrefix);

            if (column != null) {
                Map map = star.getMapLevelToColumn(cube);
                map.put(level, column);
            }

            return column;
        }

        private Column makeColumnForLevelExpr(
                RolapCube cube,
                RolapLevel level,
                String name,
                MondrianDef.Expression xmlExpr,
                boolean isNumeric,
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
                column = new RolapStar.Column(name,
                                              table,
                                              xmlExpr,
                                              isNumeric,
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
                    if (this.children == Collections.EMPTY_LIST) {
                        this.children = new ArrayList();
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
        public Table findChild(MondrianDef.Relation relation,
                               Condition joinCondition) {
            for (Iterator it = getChildren().iterator(); it.hasNext(); ) {
                Table child = (Table) it.next();
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
            for (Iterator it = getChildren().iterator(); it.hasNext(); ) {
                Table child = (Table) it.next();
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
         * Adds this table to the from clause of a query.
         *
         * @param query Query to add to
         * @param failIfExists Pass in false if you might have already added
         *     the table before and if that happens you want to do nothing.
         * @param joinToParent Pass in true if you are constraining a cell
         *     calculcation, false if you are retrieving members.
         */
        public void addToFrom(SqlQuery query,
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
        public List getChildren() {
            return children;
        }

        /**
         * Returns a list of this table's {@link Column}s.
         */
        public List getColumns() {
            return columnList;
        }

        /**
         * Finds the child table of the fact table with the given columnName
         * used in its left join condition. This is used by the AggTableManager
         * while characterizing the fact table columns.
         */
        public RolapStar.Table findTableWithLeftJoinCondition(
                final String columnName) {
            for (Iterator it = getChildren().iterator(); it.hasNext(); ) {
                Table child = (Table) it.next();
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
            for (Iterator it = getChildren().iterator(); it.hasNext(); ) {
                Table child = (Table) it.next();
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

            for (Iterator it = getColumns().iterator(); it.hasNext(); ) {
                Column column = (Column) it.next();
                column.print(pw, subsubprefix);
                pw.println();
            }

            if (this.joinCondition != null) {
                this.joinCondition.print(pw, subprefix);
            }
            for (Iterator it = getChildren().iterator(); it.hasNext(); ) {
                Table child = (Table) it.next();
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
            Util.assertPrecondition(left != null);
            Util.assertPrecondition(right != null);

            if (!(left instanceof MondrianDef.Column)) {
                // TODO: Will this ever print?? if not then left should be
                // of type MondrianDef.Column.
                LOGGER.debug("Condition.left NOT Column: "
                    +left.getClass().getName());
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
}

// End RolapStar.java
