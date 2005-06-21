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

package mondrian.rolap.aggmatcher;

import mondrian.olap.*;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.*;
import java.util.*;

/**
 * This is an aggregate table version of a RolapStar for a fact table.
 * <p>
 * There is the following class structure:
 * <pre>
 * AggStar
 *   Table
 *     JoinCondition
 *     Column
 *     Level extends Column
 *   FactTable extends Table
 *     Measure extends Table.Column
 *   DimTable extends Table
 * <pre>
 * Each inner class is non-static meaning that instances have implied references
 * to the enclosing object.
 *
 * @author <a>Richard M. Emberson</a>
 * @version
 */
public class AggStar {
    private static final Logger LOGGER = Logger.getLogger(AggStar.class);

    static Logger getLogger() {
        return LOGGER;
    }

    private static final MondrianResource mres = MondrianResource.instance();

    /**
     * Creates an AggStar and all of its {@link Table}, {@link Table.Column}s,
     * etc.
     *
     * @param star
     * @param dbTable
     * @param msgRecorder
     * @return
     */
    public static AggStar makeAggStar(final RolapStar star,
                                      final JdbcSchema.Table dbTable,
                                      final MessageRecorder msgRecorder) {

        AggStar aggStar = new AggStar(star, dbTable);
        AggStar.FactTable aggStarFactTable = aggStar.getFactTable();

        //////////////////////////////////////////////////////////////////////
        //
        // load fact count
        for (Iterator it =
            dbTable.getColumnUsages(JdbcSchema.FACT_COUNT_COLUMN_TYPE);
                    it.hasNext();) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            aggStarFactTable.loadFactCount(usage);
        }
        //
        //////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////
        //
        // load measures
        for (Iterator it =
                dbTable.getColumnUsages(JdbcSchema.MEASURE_COLUMN_TYPE);
                it.hasNext();) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            aggStarFactTable.loadMeasure(usage);
        }
        //
        //////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////
        //
        // load foreign keys
        for (Iterator it =
                dbTable.getColumnUsages(JdbcSchema.FOREIGN_KEY_COLUMN_TYPE);
                it.hasNext();) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            aggStarFactTable.loadForeignKey(usage);
        }
        //
        //////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////
        //
        // load levels
        for (Iterator it =
                dbTable.getColumnUsages(JdbcSchema.LEVEL_COLUMN_TYPE);
                it.hasNext();) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            aggStarFactTable.loadLevel(usage);
        }
        //
        //////////////////////////////////////////////////////////////////////

        return aggStar;
    }

    private final RolapStar star;
    private final AggStar.FactTable aggTable;
    private final BitKey bitKey;
    private final AggStar.Table.Column[] columns;

    AggStar(final RolapStar star, final JdbcSchema.Table aggTable) {
        this.star = star;
        this.bitKey = BitKey.Factory.makeBitKey(star.getColumnCount());
        this.aggTable = new AggStar.FactTable(aggTable);
        this.columns = new AggStar.Table.Column[star.getColumnCount()];
    }

    public AggStar.FactTable getFactTable() {
        return aggTable;
    }

    /**
     * This is a measure of the IO cost of querying this table. It can be
     * either the row count or the row count times the size of a row.
     * If the property MondrianProperties.ChooseAggregateByVolume
     * is true, then volume is returned, otherwise row count.
     *
     * @return
     */
    public int getSize() {
        return (MondrianProperties.instance().getChooseAggregateByVolume())
            ? getFactTable().getVolume()
            : getFactTable().getNumberOfRows();
    }

    /**
     * Returns true if every bit set to 1 in the bitKey parameter is also
     * set to 1 in this AggStar's bitKey.
     *
     * @param bitKey
     * @param exact If true, bits must match exactly; if false, AggStar's
     *   bitKey can be a superset
     * @return
     */
    public boolean matches(final BitKey bitKey, boolean exact) {
        return exact ?
                getBitKey().equals(bitKey) :
                getBitKey().isSuperSetOf(bitKey);
    }

    /**
     * Get this AggStar's RolapStar.
     *
     * @return
     */
    public RolapStar getStar() {
        return star;
    }

    /**
     * Get the BitKey.
     *
     * @return
     */
    public BitKey getBitKey() {
        return bitKey;
    }

    /**
     * Get an SqlQuery instance.
     *
     * @return
     */
    private SqlQuery getSqlQuery() {
        return getStar().getSqlQuery();
    }

    /**
     * Get a java.sql.Connection.
     *
     * @return
     */
    public Connection getJdbcConnection() {
        return getStar().getJdbcConnection();
    }

    /**
     * Get the Measure at the given bit position or return null.
     * Note that there is no check that the bit position is within the range of
     * the array of columns.
     * Nor is there a check that the column type at that position is a Measure.
     *
     * @param bitPos
     * @return A Measure or null.
     */
    public AggStar.FactTable.Measure lookupMeasure(final int bitPos) {
        return (AggStar.FactTable.Measure) columns[bitPos];
    }
    /**
     * Get the Level at the given bit position or return null.
     * Note that there is no check that the bit position is within the range of
     * the array of columns.
     * Nor is there a check that the column type at that position is a Level.
     *
     * @param bitPos
     * @return A Level or null.
     */
    public AggStar.Table.Level lookupLevel(final int bitPos) {
        return (AggStar.Table.Level) columns[bitPos];
    }

    /**
     * Get the Column at the bit position.
     * Note that there is no check that the bit position is within the range of
     * the array of columns.
     *
     * @param bitPos
     * @return
     */
    public AggStar.Table.Column lookupColumn(final int bitPos) {
        return columns[bitPos];
    }

    /**
     * This is called by within the Column constructor.
     *
     * @param column
     */
    private void addColumn(final AggStar.Table.Column column) {
        columns[column.getBitPosition()] = column;
    }

    /**
     * Base Table class for the FactTable and DimTable classes.
     * This class parallels the RolapStar.Table class.
     *
     */
    public abstract class Table {

        /**
         * The query join condition between a base table and this table (the
         * table that owns the join condition).
         */
        public class JoinCondition {
        	private final Logger LOGGER = Logger.getLogger(JoinCondition.class);
            // I think this is always a MondrianDef.Column
            private final MondrianDef.Expression left;
            private final MondrianDef.Expression right;

            private JoinCondition(final MondrianDef.Expression left,
                                  final MondrianDef.Expression right) {
                if (!(left instanceof MondrianDef.Column)) {
                    LOGGER.debug("JoinCondition.left NOT Column: "
                        +left.getClass().getName());
                }
                this.left = left;
                this.right = right;
            }

            /**
             * Get the enclosing AggStar.Table.
             *
             * @return
             */
            public Table getTable() {
                return AggStar.Table.this;
            }

            /**
             * This is used to create part of a SQL where clause.
             *
             * @param query
             * @return
             */
            String toString(final SqlQuery query) {
                StringBuffer buf = new StringBuffer(64);
                buf.append(left.getExpression(query));
                buf.append(" = ");
                buf.append(right.getExpression(query));
                return buf.toString();
            }
            public String toString() {
                StringWriter sw = new StringWriter(128);
                PrintWriter pw = new PrintWriter(sw);
                print(pw, "");
                pw.flush();
                return sw.toString();
            }

            /**
             * Prints this table and its children.
             */
            public void print(final PrintWriter pw, final String prefix) {
                SqlQuery sqlQueuy = getTable().getSqlQuery();
                pw.print(prefix);
                pw.println("JoinCondition:");
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
         * Base class for Level and Measure classes
         */
        public class Column {

            private final String name;
            private final MondrianDef.Expression expression;
            private final boolean isNumeric;
            /**
             * This is only used in RolapAggregationManager and adds
             * non-constraining columns making the drill-through queries
             * easier for humans to understand.
             */
            private final Column nameColumn;

            /** this has a unique value per star */
            private final int bitPosition;

            protected Column(final String name,
                             final MondrianDef.Expression expression,
                             final boolean isNumeric,
                             final int bitPosition) {
                this.name = name;
                this.expression = expression;
                this.isNumeric = isNumeric;
                this.bitPosition = bitPosition;

                this.nameColumn = null;

                // do not count the fact_count column
                if (bitPosition >= 0) {
                    AggStar.this.bitKey.setByPos(bitPosition);
                    AggStar.this.addColumn(this);
                }
            }

            /**
             * Get the name of the column (this is the name in the database).
             *
             * @return
             */
            public String getName() {
                return name;
            }

            /**
             * Get the enclosing AggStar.Table.
             *
             * @return
             */
            public AggStar.Table getTable() {
                return AggStar.Table.this;
            }

            /**
             * Get the bit possition associted with this column. This has the
             * same value as this column's RolapStar.Column.
             *
             * @return
             */
            public int getBitPosition() {
                return bitPosition;
            }

            /**
             * Return true if this is a numeric column.
             *
             * @return
             */
            public boolean isNumeric() {
                return isNumeric;
            }
            public SqlQuery getSqlQuery() {
                return getTable().getAggStar().getSqlQuery();
            }
            public MondrianDef.Expression getExpression() {
                return expression;
            }

            /**
             * This is used to create, generally, an SQL query segment of the
             * form: tablename.columnname.
             *
             * @param query
             * @return
             */
            public String getExpression(final SqlQuery query) {
                return getExpression().getExpression(query);
            }
            public String toString() {
                StringWriter sw = new StringWriter(256);
                PrintWriter pw = new PrintWriter(sw);
                print(pw, "");
                pw.flush();
                return sw.toString();
            }
            public void print(final PrintWriter pw, final String prefix) {
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
         * This class is used for holding dimension level information.
         * Both DimTables and FactTables can have Level columns.
         */
        final class Level extends Column {

            Level(final String name,
                  final MondrianDef.Expression expression,
                  final boolean isNumeric,
                  final int bitPosition) {
                super(name, expression, isNumeric, bitPosition);
            }
        }

        /** The name of the table in the database. */
        private final String name;
        private final MondrianDef.Relation relation;
        protected final List levels;
        protected List children;

        Table(final String name, final MondrianDef.Relation relation) {
            this.name = name;
            this.relation = relation;
            this.levels = new ArrayList();
            this.children = Collections.EMPTY_LIST;
        }

        /**
         * Return the name of the table in the database.
         *
         * @return
         */
        public String getName() {
            return name;
        }

        /**
         * Return true if this table has a parent table (FactTable instances
         * do not have parent tables, all other do).
         *
         * @return
         */
        public abstract boolean hasParent();

        /**
         * Get the parent table (returns null if this table is a FactTable).
         *
         * @return
         */
        public abstract Table getParent();

        /**
         * Return true if this table has a join condition (only DimTables have
         * join conditions, FactTable instances do not).
         *
         * @return
         */
        public abstract boolean hasJoinCondition();
        public abstract Table.JoinCondition getJoinCondition();

        public MondrianDef.Relation getRelation() {
            return relation;
        }

        /**
         * Get this table's enclosing AggStar.
         *
         * @return
         */
        protected AggStar getAggStar() {
            return AggStar.this;
        }

        /**
         * Get a SqlQuery object.
         *
         * @return
         */
        protected SqlQuery getSqlQuery() {
            return getAggStar().getSqlQuery();
        }

        /**
         * Get a java.sql.Connection.
         *
         * @return
         */
        public Connection getJdbcConnection() {
            return getAggStar().getJdbcConnection();
        }

        /**
         * Add a Level column.
         *
         * @param level
         */
        protected void addLevel(final AggStar.Table.Level level) {
            this.levels.add(level);
        }

        /**
         * Get all Level columns.
         *
         * @return
         */
        public Iterator getLevels() {
            return levels.iterator();
        }

        /**
         * Add a child DimTable table.
         *
         * @param child
         */
        protected void addTable(final DimTable child) {
            if (children == Collections.EMPTY_LIST) {
                children = new ArrayList();
            }
            children.add(child);
        }

        /**
         * Get all child tables.
         *
         * @return
         */
        public Iterator getChildren() {
            return children.iterator();
        }

        /**
         * Convert a RolapStar.Table into a AggStar.DimTable as well as
         * converting all columns and child tables. If the
         * rightJoinConditionColumnName parameter is null, then the table's namd
         * and the rTable parameter's condition left condition's column name
         * are used to form the join condition's left expression.
         *
         * @param rTable
         * @param rightJoinConditionColumnName
         * @return
         */
        protected AggStar.DimTable convertTable(final RolapStar.Table rTable,
                                  final String rightJoinConditionColumnName) {
            String tableName = rTable.getAlias();
            MondrianDef.Relation relation = rTable.getRelation();
            RolapStar.Condition rjoinCondition = rTable.getCondition();
            MondrianDef.Expression rleft = rjoinCondition.getLeft();
            MondrianDef.Expression rright = rjoinCondition.getRight();

            MondrianDef.Expression left = null;
            if (rightJoinConditionColumnName != null) {
                left = new MondrianDef.Column(getName(),
                                              rightJoinConditionColumnName);
            } else {
                if (rleft instanceof MondrianDef.Column) {
                    MondrianDef.Column rcolumn = (MondrianDef.Column) rleft;
                    left = new MondrianDef.Column(getName(), rcolumn.name);
                } else {

                    // RME TODO can we catch this during validation
                    String msg = mres.getBadRolapStarLeftJoinCondition(
                        "AggStar.Table",
                        rleft.getClass().getName(), left.toString());
                    getLogger().warn(msg);
                }
            }
            JoinCondition joinCondition = new JoinCondition(left, rright);
            DimTable dimTable =
                new DimTable(this, tableName, relation, joinCondition);

            dimTable.convertColumns(rTable);
            dimTable.convertChildren(rTable);

            return dimTable;
        }

        /**
         * Convert a RolapStar.Table table's columns into
         * AggStar.Table.Level columns.
         *
         * @param rTable
         */
        protected void convertColumns(final RolapStar.Table rTable) {
            // add level columns
            for (Iterator it = rTable.getColumns(); it.hasNext(); ) {
                RolapStar.Column column = (RolapStar.Column) it.next();

                String name = column.getName();
                MondrianDef.Expression expression = column.getExpression();
                boolean isNumeric = column.isNumeric();
                int bitPosition = column.getBitPosition();

                Level level = new Level(name,
                                        expression,
                                        isNumeric,
                                        bitPosition);
                addLevel(level);
            }
        }

        /**
         * Convert the child tables of a RolapStar.Table into
         * child AggStar.DimTable tables.
         *
         * @param rTable
         */
        protected void convertChildren(final RolapStar.Table rTable) {
            // add children tables
            for (Iterator it = rTable.getChildren(); it.hasNext(); ) {
                RolapStar.Table rTableChild = (RolapStar.Table) it.next();

                AggStar.DimTable dimChild = convertTable(rTableChild, null);

                addTable(dimChild);
            }
        }

        /**
         * This is a copy of the code found in RolapStar used to generate an SQL
         * query.
         *
         * @param query
         * @param failIfExists
         * @param joinToParent
         */
        public void addToFrom(final SqlQuery query,
                              final boolean failIfExists,
                              final boolean joinToParent) {
            query.addFrom(relation, name, failIfExists);
            if (joinToParent) {
                if (hasParent()) {
                    getParent().addToFrom(query, failIfExists, joinToParent);
                }
                if (hasJoinCondition()) {
                    query.addWhere(getJoinCondition().toString(query));
                }
            }
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }
        public abstract void print(final PrintWriter pw, final String prefix);
    }

    /**
     * This is an aggregate fact table.
     */
    public class FactTable extends Table {

        /**
         * This is a Column that is a Measure (contains an aggregator).
         */
        public class Measure extends Table.Column {
            private final RolapAggregator aggregator;

            Measure(final String name,
                    final MondrianDef.Expression expression,
                    final boolean isNumeric,
                    final int bitPosition,
                    final RolapAggregator aggregator) {
                super(name, expression, isNumeric, bitPosition);
                this.aggregator = aggregator;
            }

            /**
             * Get this Measure's RolapAggregator.
             *
             * @return
             */
            public RolapAggregator getAggregator() {
                return aggregator;
            }

            /**
             * Get this Measure's sql expression which is an aggregator.
             *
             * @param query
             * @return
             */
            public String getExpression(final SqlQuery query) {
                String expr = getExpression().getExpression(query);
                return getAggregator().getExpression(expr);
            }
        }

        private Column factCountColumn;
        private final List measures;
        private final int totalColumnSize;
        private int numberOfRows;

        FactTable(final JdbcSchema.Table aggTable) {
            this(aggTable.getName(),
                 aggTable.table,
                 aggTable.getTotalColumnSize());
        }
        FactTable(final String name,
                  final MondrianDef.Relation relation,
                  final int totalColumnSize) {
            super(name, relation);
            this.totalColumnSize = totalColumnSize;
            this.measures = new ArrayList();
            this.numberOfRows = -1;
        }
        public Table getParent() {
            return null;
        }
        public boolean hasParent() {
            return false;
        }
        public boolean hasJoinCondition() {
            return false;
        }
        public Table.JoinCondition getJoinCondition() {
            return null;
        }

        /**
         * Get the volume of the table (now of rows * size of a row).
         *
         * @return
         */
        public int getVolume() {
            return getTotalColumnSize() * getNumberOfRows();
        }

        /**
         * Get the total size of all columns in a row.
         *
         * @return
         */
        public int getTotalColumnSize() {
            return totalColumnSize;
        }

        /**
         * Get the number of rows in this aggregate table.
         *
         * @return
         */
        public int getNumberOfRows() {
            if (numberOfRows == -1) {
                makeNumberOfRows();
            }
            return numberOfRows;
        }

        /**
         * Get all Measures.
         *
         * @return Iterator over measures,
         */
        public Iterator getMeasures() {
            return measures.iterator();
        }

        /**
         * Get all columns.
         *
         * @return Iterator over columns,
         */
        public Iterator getColumns() {
            List list = new ArrayList();
            list.addAll(measures);
            list.addAll(levels);
            for (Iterator it = getChildren(); it.hasNext(); ) {
                DimTable dimTable = (DimTable) it.next();
                dimTable.addColumnsToList(list);
            }

            return list.iterator();
        }

        /**
         * For a foreign key usage create a child DimTable table.
         *
         * @param usage
         */
        private void loadForeignKey(final JdbcSchema.Table.Column.Usage usage) {
            DimTable child = convertTable(usage.rTable,
                                          usage.rightJoinConditionColumnName);
            addTable(child);
        }
        /**
         * Given a usage of type measure, create a Measure column.
         *
         * @param usage
         */
        private void loadMeasure(final JdbcSchema.Table.Column.Usage usage) {
            String name = usage.getColumn().getName();
            String symbolicName = usage.getSymbolicName();
            if (symbolicName == null) {
                symbolicName = name;
            }
            boolean isNumeric = usage.getColumn().isNumeric();
            RolapAggregator aggregator = usage.getAggregator();

            MondrianDef.Expression expression = null;
            if (usage.getColumn().hasUsage(JdbcSchema.FOREIGN_KEY_COLUMN_TYPE) &&
                    ! aggregator.isDistinct()) {
                expression = factCountColumn.getExpression();
            } else {
                expression = new MondrianDef.Column(getName(), name);
            }

            int bitPosition = usage.measure.getBitPosition();

            Measure aggMeasure = new Measure(symbolicName,
                                             expression,
                                             isNumeric,
                                             bitPosition,
                                             aggregator);

            measures.add(aggMeasure);
        }

        /**
         * Create a fact_count column for a usage of type fact count.
         *
         * @param usage
         */
        private void loadFactCount(final JdbcSchema.Table.Column.Usage usage) {
            String name = usage.getColumn().getName();
            String symbolicName = usage.getSymbolicName();
            if (symbolicName == null) {
                symbolicName = name;
            }

            MondrianDef.Expression expression =
                new MondrianDef.Column(getName(), name);
            boolean isNumeric = usage.getColumn().isNumeric();
            int bitPosition = -1;

            Column aggColumn = new Column(symbolicName,
                                          expression,
                                          isNumeric,
                                          bitPosition);

            factCountColumn = aggColumn;
        }

        /**
         * Given a usage of type level, create a Level column.
         *
         * @param usage
         */
        private void loadLevel(final JdbcSchema.Table.Column.Usage usage) {
            String name = usage.getSymbolicName();
            MondrianDef.Expression expression =
                new MondrianDef.Column(getName(), usage.levelColumnName);
            boolean isNumeric = usage.getColumn().isNumeric();
            int bitPosition = usage.column.getBitPosition();

            Level level = new Level(name,
                                    expression,
                                    isNumeric,
                                    bitPosition);
            addLevel(level);
        }
        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("Table:");
            String subprefix = prefix + "  ";
            String subsubprefix = subprefix + "  ";

            pw.print(subprefix);
            pw.print("name=");
            pw.println(getName());

            if (getRelation() != null) {
                pw.print(subprefix);
                pw.print("relation=");
                pw.println(getRelation());
            }

            pw.print(subprefix);
            pw.print("numberofrows=");
            pw.println(getNumberOfRows());

            pw.print(subprefix);
            pw.println("FactCount:");
            factCountColumn.print(pw, subsubprefix);
            pw.println();

            pw.print(subprefix);
            pw.println("Measures:");
            for (Iterator it = getMeasures(); it.hasNext(); ) {
                Column column = (Column) it.next();
                column.print(pw, subsubprefix);
                pw.println();
            }

            pw.print(subprefix);
            pw.println("Levels:");
            for (Iterator it = getLevels(); it.hasNext(); ) {
                Level level = (Level) it.next();
                level.print(pw, subsubprefix);
                pw.println();
            }

            for (Iterator it = getChildren(); it.hasNext(); ) {
                DimTable child = (DimTable) it.next();
                child.print(pw, subprefix);
            }
        }
        private void makeNumberOfRows() {
            SqlQuery query = getSqlQuery();
            query.addSelect("count(*)");
            query.addFrom(getRelation(), getName(), false);
            Connection conn = getJdbcConnection();
            try {
                ResultSet rs = null;
                try {
                    rs = RolapUtil.executeQuery(conn, query.toString(),
                                         "AggStar.FactTable.makeNumberOfRows");

                    if (rs.next()) {
                        numberOfRows = rs.getInt(1);
                    } else {
                        String msg = mres.getSqlQueryFailed(
                                "AggStar.FactTable.makeNumberOfRows",
                                query.toString());
                        getLogger().warn(msg);

                        // set to large number so that this table is never used
                        numberOfRows = Integer.MAX_VALUE/getTotalColumnSize();
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            } catch (SQLException ex) {
                // ignore
                getLogger().error(ex);
            } finally {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }
        }

    }

    /**
     * This class represents a dimension table.
     */
    public class DimTable extends Table {
        private final Table parent;
        private final JoinCondition joinCondition;

        DimTable(final Table parent,
                 final String name,
                 final MondrianDef.Relation relation,
                 final JoinCondition joinCondition) {
            super(name, relation);
            this.parent = parent;
            this.joinCondition = joinCondition;
        }
        public Table getParent() {
            return parent;
        }
        public boolean hasParent() {
            return true;
        }
        public boolean hasJoinCondition() {
            return true;
        }
        public Table.JoinCondition getJoinCondition() {
            return joinCondition;
        }


        /**
         * Add all of this Table's columns to the list parameter and then add
         * all child table columns.
         *
         * @param list
         */
        public void addColumnsToList(final List list) {
            list.addAll(levels);
            for (Iterator it = getChildren(); it.hasNext(); ) {
                DimTable dimTable = (DimTable) it.next();
                dimTable.addColumnsToList(list);
            }
        }
        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("Table:");
            String subprefix = prefix + "  ";
            String subsubprefix = subprefix + "  ";

            pw.print(subprefix);
            pw.print("name=");
            pw.println(getName());

            if (getRelation() != null) {
                pw.print(subprefix);
                pw.print("relation=");
                pw.println(getRelation());
            }

            pw.print(subprefix);
            pw.println("Levels:");

            for (Iterator it = getLevels(); it.hasNext(); ) {
                Level level = (Level) it.next();
                level.print(pw, subsubprefix);
                pw.println();
            }

            joinCondition.print(pw, subprefix);

            for (Iterator it = getChildren(); it.hasNext(); ) {
                DimTable child = (DimTable) it.next();
                child.print(pw, subprefix);
            }
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
     * Print this AggStar.
     *
     * @param pw
     * @param prefix
     */
    public void print(final PrintWriter pw, final String prefix) {
        pw.print(prefix);
        pw.println("AggStar:");
        String subprefix = prefix + "  ";
        pw.print(subprefix);
        pw.println(bitKey);
        aggTable.print(pw, subprefix);
    }
}
