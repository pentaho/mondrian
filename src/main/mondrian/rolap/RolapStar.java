/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.olap.MondrianResource;
import mondrian.rolap.agg.Aggregation;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.ColumnConstraint;
import mondrian.rolap.sql.SqlQuery;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

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
    /**
     * As {@link #mapCubeToMapLevelToColumn}, but holds name columns.
     */
    private final Map mapCubeToMapLevelToNameColumn;

    /**
     * Maps {@link Column} to {@link String} for each column which is a key
     * to a level.
     */
    private final Map mapColumnToName;

    /** holds all aggregations of this star */
    private List aggregations;

    /**
     * Creates a RolapStar. Please use
     * {@link RolapSchema.RolapStarRegistry#getOrCreateStar} to create a
     * {@link RolapStar}.
     */
    RolapStar(RolapSchema schema, 
              DataSource dataSource, 
              MondrianDef.Relation fact) {
        this.schema = schema;
        this.dataSource = dataSource;
        this.factTable = new Table(this, fact, null, null);

        this.mapCubeToMapLevelToColumn = new HashMap();
        this.mapCubeToMapLevelToNameColumn = new HashMap();
        this.mapColumnToName = new HashMap();
        this.aggregations = new ArrayList();
    }

    public Table getFactTable() {
        return factTable;
    }

    void addAggregation(Aggregation agg) {
        synchronized(aggregations) {
            aggregations.add(agg);
        }
    }

    Map getMapLevelToColumn(RolapCube cube) {
        Map mapLevelToColumn = (Map) this.mapCubeToMapLevelToColumn.get(cube);
        if (mapLevelToColumn == null) {
            mapLevelToColumn = new HashMap();
            this.mapCubeToMapLevelToColumn.put(cube, mapLevelToColumn);
        }
        return mapLevelToColumn;
    }
    Map getMapLevelToNameColumn(RolapCube cube) {
        Map mapLevelToNameColumn = (HashMap)
            this.mapCubeToMapLevelToNameColumn.get(cube);
        if (mapLevelToNameColumn == null) {
            mapLevelToNameColumn = new HashMap();
            this.mapCubeToMapLevelToNameColumn.put(cube, mapLevelToNameColumn);
        }
        return mapLevelToNameColumn;
    }


    /**
     * Looks up an aggregation or creates one if it does not exist in an
     * atomic (synchronized) operation
     */
    public Aggregation lookupOrCreateAggregation(RolapStar.Column[] columns) {
        synchronized(aggregations) {
            Aggregation aggregation = lookupAggregation(columns);
            if (aggregation == null) {
                aggregation = new Aggregation(this, columns);
                this.aggregations.add(aggregation);
            }
            return aggregation;
        }
    }

    /**
     * Looks for an existing aggregation over a given set of columns, or
     * returns <code>null</code> if there is none.
     *
     * <p>Must be called from synchronized context.
     **/
    public Aggregation lookupAggregation(RolapStar.Column[] columns) {
        synchronized(aggregations) {
            for (int i = 0, count = aggregations.size(); i < count; i++) {
                Aggregation aggregation = (Aggregation) aggregations.get(i);
                Util.assertTrue(aggregation.getStar() == this);
                if (equals(aggregation.getColumns(), columns)) {
                    return aggregation;
                }
            }
            return null;
        }
    }

    /**
     * Returns whether two arrays of columns are identical.
     **/
    private static boolean equals(RolapStar.Column[] columns1, 
                                  RolapStar.Column[] columns2) {
        int count = columns1.length;
        if (count != columns2.length) {
            return false;
        }
        for (int j = 0; j < count; j++) {
            if (columns1[j] != columns2[j]) {
                return false;
            }
        }
        return true;
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
    public Column lookupColumn(String tableAlias, String columnName) {
        final Table table = factTable.findDescendant(tableAlias);
        if (table != null) {
            for (int i = 0; i < table.columns.size(); i++) {
                Column column = (Column) table.columns.get(i);
                if (column.getExpression() instanceof MondrianDef.Column) {
                    MondrianDef.Column columnExpr = 
                        (MondrianDef.Column) column.getExpression();
                    if (columnExpr.name.equals(columnName)) {
                        return column;
                    }
                }
            }
        }
        return null;
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
        SqlQuery sqlQuery;
        try {
            sqlQuery = new SqlQuery(jdbcConnection.getMetaData());
        } catch (SQLException e) {
            throw Util.getRes().newInternal("while computing single cell", e);
        }
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
            throw Util.getRes().newInternal(
                    "while computing single cell; sql=[" + sql + "]", e);
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
     * Returns a printable name for a column, generally the name of the level
     * mapped into it; or null if no such mapping exists. The mapping is
     * approximate.
     */
    public String getColumnName(Column column) {
        return (String) mapColumnToName.get(column);
    }

    void addColumnToName(Column column, String name) {
        this.mapColumnToName.put(column, name);
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
            throw MondrianResource.instance().newInternal(
                "Error while retrieving metadata for table '" +
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

    /**
     * A column in a star schema.
     */
    public static class Column {
        private final Table table;
        private final MondrianDef.Expression expression;
        private final boolean isNumeric;
        private int cardinality = -1;

        Column(Table table, 
               MondrianDef.Expression expression, 
               boolean isNumeric) {
            this.table = table;
            this.expression = expression;
            this.isNumeric = isNumeric;
        }
        public RolapStar.Table getTable() {
            return table;
        }
        public MondrianDef.Expression getExpression() {
            return expression;
        }

        public RolapStar getStar() {
            return table.star;
        }

        public String getExpression(SqlQuery query) {
            return expression.getExpression(query);
        }

        private void quoteValue(Object o, StringBuffer buf) {
            String s = o.toString();
            if (isNumeric) {
                buf.append(s);
            } else {
                RolapUtil.singleQuoteForSql(s, buf);
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
            SqlQuery sqlQuery;
            try {
                sqlQuery = new SqlQuery(jdbcConnection.getMetaData());
            } catch (SQLException e) {
                throw Util.getRes().newInternal(
                        "while counting distinct values of column '" +
                        expression.getGenericExpression() + "'", e);
            }
            if (sqlQuery.allowsCountDistinct()) {
                // e.g. "select count(distinct product_id) from product"
                sqlQuery.addSelect("count(distinct " 
                    + getExpression(sqlQuery) + ")");

                // no need to join fact table here
                table.addToFrom(sqlQuery, true, false);
            } else if (sqlQuery.allowsFromQuery()) {
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
                throw Util.getRes().newInternal(
                        "while counting distinct values of column '" 
                        + expression.getGenericExpression() 
                        + "'; sql=[" 
                        + sql 
                        + "]",
                        e);
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
        public String createInExpr(String expr, 
                                   ColumnConstraint[] constraints) {
            if (constraints.length == 1) {
                final ColumnConstraint constraint = constraints[0];
                Object key = constraint.getValue();
                if (key != RolapUtil.sqlNullValue) {
                    StringBuffer buf = new StringBuffer(64);
                    buf.append(expr);
                    buf.append(" = ");
                    quoteValue(key, buf);
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
                quoteValue(key, sb);
            }
            sb.append(')');
            if (notNullCount < constraints.length) {
                // There was at least one null.
                switch (notNullCount) {
                case 0:
                    // Special case -- there were no values besides null.
                    // Return, for example, "x is null".
                    return expr + " is null";
                case 1: {
                    // Special case -- one not-null value, and null, for
                    // example "(x is null or x = 1)".
                    StringBuffer buf = new StringBuffer(64);
                    buf.append('(');
                    buf.append(expr);
                    buf.append(" = ");
                    quoteValue(constraints[0].getValue(), buf);
                    buf.append(" or ");
                    buf.append(expr);
                    buf.append(" is null");
                    return buf.toString();
                }
                default: {
                    // Nulls and values, for example,
                    // "(x in (1, 2) or x IS NULL)".
                    StringBuffer buf = new StringBuffer(64);
                    buf.append('(');
                    buf.append(sb.toString());
                    buf.append(" or ");
                    buf.append(expr);
                    buf.append(" is null");
                    return buf.toString();
                }
                }
            } else {
                // No nulls. Return, for example, "x in (1, 2, 3)".
                return sb.toString();
            }
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

        Measure(RolapAggregator aggregator,
                Table table, 
                MondrianDef.Expression expression,
                boolean isNumeric) {
            super(table, expression, isNumeric);
            this.aggregator = aggregator;
        }
        public RolapAggregator getAggregator() {
            return aggregator;
        }
    };

    /**
     * Definition of a table in a star schema.
     *
     * <p>A 'table' is defined by a {@link MondrianDef.Relation} so may, in
     * fact, be a view.
     *
     * <p>Every table in the star schema except the fact table has a parent
     * table, and a condition which specifies how it is joined to its parent.
     * So the star schema is, in effect, a hierarchy with the fact table at
     * its root.
     */
    public static class Table {
        private final RolapStar star;
        private final MondrianDef.Relation relation;
        private final List columns;
        private final Table parent;
        private final List children;
        /** Condition with which it is connected to its parent. **/
        private final Condition joinCondition;
        private final String alias;

        Table(RolapStar star, 
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
            this.columns = new ArrayList();
            this.children = new ArrayList();
            Util.assertTrue((parent == null) == (joinCondition == null));
        }
        public Table getParent() {
            return parent;
        }
        public void addColumn(Column column) {
            this.columns.add(column);
        }
        RolapStar getStar() {
            return star;
        }
        MondrianDef.Relation getRelation() {
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
            for (int i = 0; i < children.size(); i++) {
                Table child = (Table) children.get(i);
                if (child.relation.equals(relation)) {
                    Condition condition = joinCondition;
                    if (!Util.equals(relation.getAlias(), child.alias)) {
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
            for (int i = 0, n = children.size(); i < n; i++) {
                Table child = (Table) children.get(i);
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

        public boolean isFunky() {
            return (relation == null);
        }
        /**
         * Prints this table and its children.
         */
        void print(PrintWriter pw, String prefix) {
            pw.print(prefix);
            pw.print("Table: alias=[" + this.getAlias());
            if (this.relation != null) {
                pw.print("] relation=[" + relation);
            }
            pw.print("] columns=[");
            for (int i = 0, n = columns.size(); i < n; i++) {
                Column column = (Column) columns.get(i);
                if (i > 0) {
                    pw.print(',');
                }
                pw.print(column.getExpression().getGenericExpression());
            }
            pw.println(']');
            for (int i = 0; i < children.size(); i++) {
                Table child = (Table) children.get(i);
                child.print(pw, prefix + "  ");
            }
        }

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
        private final MondrianDef.Expression left;
        private final MondrianDef.Expression right;

        Condition(MondrianDef.Expression left, 
                  MondrianDef.Expression right) {
            Util.assertPrecondition(left != null);
            Util.assertPrecondition(right != null);

            this.left = left;
            this.right = right;
        }
        String toString(SqlQuery query) {
            return left.getExpression(query) + " = " + right.getExpression(query);
        }

/*
RME - this can not be what was wanted
        public int hashCode() {
            int h = Util.hash(0, left.toString());
            h = Util.hash(h, right.toString());
            return h;
        }
*/
        public int hashCode() {
            int h = Util.hash(0, left.hashCode());
            h = Util.hash(h, right.hashCode());
            return h;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Condition)) {
                return false;
            }
            Condition that = (Condition) obj;
            return Util.equals(this.left.toString(), that.left.toString()) &&
                Util.equals(this.right.toString(), that.right.toString());
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
                return new MondrianDef.Column(visit(column.table),
                        column.name);
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
