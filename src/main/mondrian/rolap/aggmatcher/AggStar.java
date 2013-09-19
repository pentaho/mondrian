/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.*;
import mondrian.olap.MondrianDef.AggLevel;
import mondrian.recorder.MessageRecorder;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.JdbcSchema.Table.Column.Usage;
import mondrian.rolap.aggmatcher.JdbcSchema.UsageType;
import mondrian.rolap.sql.SqlQuery;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.Dialect;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

/**
 * Aggregate table version of a RolapStar for a fact table.
 *
 * <p>There is the following class structure:
 * <pre>
 * AggStar
 *   Table
 *     JoinCondition
 *     Column
 *     Level extends Column
 *   FactTable extends Table
 *     Measure extends Table.Column
 *   DimTable extends Table
 * </pre>
 *
 * <p>Each inner class is non-static meaning that instances have implied
 * references to the enclosing object.
 *
 * @author Richard M. Emberson
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
     */
    public static AggStar makeAggStar(
        final RolapStar star,
        final JdbcSchema.Table dbTable,
        final MessageRecorder msgRecorder,
        final int approxRowCount)
    {
        AggStar aggStar = new AggStar(star, dbTable, approxRowCount);
        AggStar.FactTable aggStarFactTable = aggStar.getFactTable();

        // 1. load fact count
        for (Iterator<JdbcSchema.Table.Column.Usage> it =
                 dbTable.getColumnUsages(JdbcSchema.UsageType.FACT_COUNT);
             it.hasNext();)
        {
            JdbcSchema.Table.Column.Usage usage = it.next();
            aggStarFactTable.loadFactCount(usage);
        }

        // 2. load measures
        for (Iterator<JdbcSchema.Table.Column.Usage> it =
                dbTable.getColumnUsages(JdbcSchema.UsageType.MEASURE);
             it.hasNext();)
        {
            JdbcSchema.Table.Column.Usage usage = it.next();
            aggStarFactTable.loadMeasure(usage);
        }

        // 3. load foreign keys
        for (Iterator<JdbcSchema.Table.Column.Usage> it =
                dbTable.getColumnUsages(JdbcSchema.UsageType.FOREIGN_KEY);
             it.hasNext();)
        {
            JdbcSchema.Table.Column.Usage usage = it.next();
            aggStarFactTable.loadForeignKey(usage);
        }

        // 4. load levels
        for (Iterator<JdbcSchema.Table.Column.Usage> it =
                dbTable.getColumnUsages(JdbcSchema.UsageType.LEVEL);
             it.hasNext();)
        {
            JdbcSchema.Table.Column.Usage usage = it.next();
            aggStarFactTable.loadLevel(usage);
        }

        // 5. for each distinct-count measure, populate a list of the levels
        //    which it is OK to roll up
        for (FactTable.Measure measure : aggStarFactTable.measures) {
            if (measure.aggregator.isDistinct()
                && measure.argument instanceof MondrianDef.Column)
            {
                setLevelBits(
                    measure.rollableLevelBitKey,
                    aggStarFactTable,
                    (MondrianDef.Column) measure.argument,
                    star.getFactTable());
            }
        }

        return aggStar;
    }

    /**
     * Sets bits in the bitmap for all levels reachable from a given table.
     * This allows us to compute which levels can be safely aggregated away
     * when rolling up a distinct-count measure.
     *
     * <p>For example, when rolling up a measure based on
     * 'COUNT(DISTINCT customer_id)', all levels in the Customers table
     * and the Regions table reached via the Customers table can be rolled up.
     * So method sets the bit for all of these levels.
     *
     * @param bitKey Bit key of levels which can be rolled up
     * @param aggTable Fact or dimension table which is the start point for
     *   the navigation
     * @param column Foreign-key column which constraints which dimension
     */
    private static void setLevelBits(
        final BitKey bitKey,
        Table aggTable,
        MondrianDef.Column column,
        RolapStar.Table table)
    {
        final Set<RolapStar.Column> columns = new HashSet<RolapStar.Column>();
        RolapStar.collectColumns(columns, table, column);

        final List<Table.Level> levelList = new ArrayList<Table.Level>();
        collectLevels(levelList, aggTable, null);

        for (Table.Level level : levelList) {
            if (columns.contains(level.starColumn)) {
                bitKey.set(level.getBitPosition());
            }
        }
    }

    private static void collectLevels(
        List<Table.Level> levelList,
        Table table,
        MondrianDef.Column joinColumn)
    {
        if (joinColumn == null) {
            levelList.addAll(table.levels);
        }
        for (Table dimTable : table.children) {
            if (joinColumn != null
                && !dimTable.getJoinCondition().left.equals(joinColumn))
            {
                continue;
            }
            collectLevels(levelList, dimTable, null);
        }
    }

    private final RolapStar star;
    private final AggStar.FactTable aggTable;

    /**
     * This BitKey is for all of the columns in the AggStar (levels and
     * measures).
     */
    private final BitKey bitKey;

    /**
     * BitKey of the levels (levels and foreign keys) of this AggStar.
     */
    private final BitKey levelBitKey;

    /**
     * BitKey of the measures of this AggStar.
     */
    private final BitKey measureBitKey;

    /**
     * BitKey of the foreign keys of this AggStar.
     */
    private final BitKey foreignKeyBitKey;

    /**
     * BitKey of those measures of this AggStar that are distinct count
     * aggregates.
     */
    private final BitKey distinctMeasureBitKey;
    private final AggStar.Table.Column[] columns;
    /**
     * A map of bit positions to columns which need to
     * be joined and are not collapsed. If the aggregate table
     * includes an {@link AggLevel} element which is not
     * collapsed, it will appear in that list. We use this
     * list later on to create the join paths in AggQuerySpec.
     */
    private final Map<Integer, AggStar.Table.Column> levelColumnsToJoin;

    /**
     * An approximate number of rows present in this aggregate table.
     */
    private final int approxRowCount;

    AggStar(
        final RolapStar star,
        final JdbcSchema.Table aggTable,
        final int approxRowCount)
    {
        this.star = star;
        this.approxRowCount = approxRowCount;
        this.bitKey = BitKey.Factory.makeBitKey(star.getColumnCount());
        this.levelBitKey = bitKey.emptyCopy();
        this.measureBitKey = bitKey.emptyCopy();
        this.foreignKeyBitKey = bitKey.emptyCopy();
        this.distinctMeasureBitKey = bitKey.emptyCopy();
        this.aggTable = new AggStar.FactTable(aggTable);
        this.columns = new AggStar.Table.Column[star.getColumnCount()];
        this.levelColumnsToJoin = new HashMap<Integer, AggStar.Table.Column>();
    }

    /**
     * Get the fact table.
     *
     * @return the fact table
     */
    public AggStar.FactTable getFactTable() {
        return aggTable;
    }

    /**
     * Find a table by name (alias) that is a descendant of the base
     * fact table.
     *
     * @param name the table to find
     * @return the table or null
     */
    public Table findTable(String name) {
        AggStar.FactTable table = getFactTable();
        return table.findDescendant(name);
    }


    /**
     * Returns a measure of the IO cost of querying this table. It can be
     * either the row count or the row count times the size of a row.
     * If the property {@link MondrianProperties#ChooseAggregateByVolume}
     * is true, then volume is returned, otherwise row count.
     */
    public int getSize() {
        return MondrianProperties.instance().ChooseAggregateByVolume.get()
            ? getFactTable().getVolume()
            : getFactTable().getNumberOfRows();
    }

    void setForeignKey(int index) {
        this.foreignKeyBitKey.set(index);
    }
    public BitKey getForeignKeyBitKey() {
        return this.foreignKeyBitKey;
    }

    /**
     * Is this AggStar's BitKey a super set (proper or not) of the BitKey
     * parameter.
     *
     * @return true if it is a super set
     */
    public boolean superSetMatch(final BitKey bitKey) {
        return getBitKey().isSuperSetOf(bitKey);
    }

    /**
     * Return true if this AggStar's level BitKey equals the
     * <code>levelBitKey</code> parameter
     * and if this AggStar's measure BitKey is a super set
     * (proper or not) of the <code>measureBitKey</code> parameter.
     */
    public boolean select(
        final BitKey levelBitKey,
        final BitKey coreLevelBitKey,
        final BitKey measureBitKey)
    {
        if (!getMeasureBitKey().isSuperSetOf(measureBitKey)) {
            return false;
        }
        if (getLevelBitKey().equals(levelBitKey)) {
            return true;
        } else if (getLevelBitKey().isSuperSetOf(levelBitKey)
            && getLevelBitKey().andNot(coreLevelBitKey).equals(
                levelBitKey.andNot(coreLevelBitKey)))
        {
            // It's OK to roll up levels which are orthogonal to the distinct
            // measure.
            return true;
        } else {
            return false;
        }
    }

    /**
     * Get this AggStar's RolapStar.
     */
    public RolapStar getStar() {
        return star;
    }

    /**
     * Return true if AggStar has measures
     */
    public boolean hasMeasures() {
        return getFactTable().hasMeasures();
    }

    /**
     * Return true if AggStar has levels
     */
    public boolean hasLevels() {
        return getFactTable().hasLevels();
    }

    /**
     * Returns whether this AggStar has foreign keys.
     */
    public boolean hasForeignKeys() {
        return getFactTable().hasChildren();
    }

    /**
     * Returns the BitKey.
     */
    public BitKey getBitKey() {
        return bitKey;
    }

    /**
     * Get the foreign-key/level BitKey.
     */
    public BitKey getLevelBitKey() {
        return levelBitKey;
    }

    /**
     * Returns a BitKey of all measures.
     */
    public BitKey getMeasureBitKey() {
        return measureBitKey;
    }

    /**
     * Returns a BitKey containing only distinct measures.
     */
    public BitKey getDistinctMeasureBitKey() {
        return distinctMeasureBitKey;
    }

    /**
     * Get an SqlQuery instance.
     */
    private SqlQuery getSqlQuery() {
        return getStar().getSqlQuery();
    }

    /**
     * Get the Measure at the given bit position or return null.
     * Note that there is no check that the bit position is within the range of
     * the array of columns.
     * Nor is there a check that the column type at that position is a Measure.
     *
     * @return A Measure or null.
     */
    public AggStar.FactTable.Measure lookupMeasure(final int bitPos) {
        AggStar.Table.Column column = lookupColumn(bitPos);
        return (column instanceof AggStar.FactTable.Measure)
            ?  (AggStar.FactTable.Measure) column
            : null;
    }
    /**
     * Get the Level at the given bit position or return null.
     * Note that there is no check that the bit position is within the range of
     * the array of columns.
     * Nor is there a check that the column type at that position is a Level.
     *
     * @return A Level or null.
     */
    public AggStar.Table.Level lookupLevel(final int bitPos) {
        AggStar.Table.Column column = lookupColumn(bitPos);
        return (column instanceof AggStar.FactTable.Level)
            ?  (AggStar.FactTable.Level) column
            : null;
    }

    /**
     * Get the Column at the bit position.
     * Note that there is no check that the bit position is within the range of
     * the array of columns.
     */
    public AggStar.Table.Column lookupColumn(final int bitPos) {
        if (columns[bitPos] != null) {
            return columns[bitPos];
        }
        return levelColumnsToJoin.get(bitPos);
    }

    /**
     * Returns true if every level column in the AggStar  is collapsed.
     */
    public boolean isFullyCollapsed() {
        BitSet bitSet = this.getLevelBitKey().toBitSet();
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1))
        {
            if (this.lookupLevel(i) == null
                || !(this.lookupLevel(i)).isCollapsed())
            {
                return false;
            }
        }
        return true;
    }

    /**
     * This is called by within the Column constructor.
     */
    private void addColumn(final AggStar.Table.Column column) {
        columns[column.getBitPosition()] = column;
    }

    private static final Logger JOIN_CONDITION_LOGGER =
            Logger.getLogger(AggStar.Table.JoinCondition.class);

    /**
     * Base Table class for the FactTable and DimTable classes.
     * This class parallels the RolapStar.Table class.
     */
    public abstract class Table {

        /**
         * The query join condition between a base table and this table (the
         * table that owns the join condition).
         */
        public class JoinCondition {
            // I think this is always a MondrianDef.Column
            private final MondrianDef.Expression left;
            private final MondrianDef.Expression right;

            private JoinCondition(
                final MondrianDef.Expression left,
                final MondrianDef.Expression right)
            {
                if (!(left instanceof MondrianDef.Column)) {
                    JOIN_CONDITION_LOGGER.debug(
                        "JoinCondition.left NOT Column: "
                        + left.getClass().getName());
                }
                this.left = left;
                this.right = right;
            }

            /**
             * Get the enclosing AggStar.Table.
             */
            public Table getTable() {
                return AggStar.Table.this;
            }

            /**
             * Return the left join expression.
             */
            public MondrianDef.Expression getLeft() {
                return this.left;
            }

            /**
             * Return the left join expression as string.
             */
            public String getLeft(final SqlQuery query) {
                return this.left.getExpression(query);
            }

            /**
             * Return the right join expression.
             */
            public MondrianDef.Expression getRight() {
                return this.right;
            }

            /**
             * This is used to create part of a SQL where clause.
             */
            String toString(final SqlQuery query) {
                StringBuilder buf = new StringBuilder(64);
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
                if (left instanceof MondrianDef.Column) {
                    MondrianDef.Column c = (MondrianDef.Column) left;
                    mondrian.rolap.RolapStar.Column col =
                        getTable().getAggStar().getStar().getFactTable()
                        .lookupColumn(c.name);
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
         * Base class for Level and Measure classes
         */
        public class Column {

            private final String name;
            private final MondrianDef.Expression expression;
            private final Dialect.Datatype datatype;
            /**
             * This is only used in RolapAggregationManager and adds
             * non-constraining columns making the drill-through queries
             * easier for humans to understand.
             */
            private final Column nameColumn;

            /** this has a unique value per star */
            private final int bitPosition;

            protected Column(
                final String name,
                final MondrianDef.Expression expression,
                final Dialect.Datatype datatype,
                final int bitPosition)
            {
                this.name = name;
                this.expression = expression;
                this.datatype = datatype;
                this.bitPosition = bitPosition;

                this.nameColumn = null;

                // do not count the fact_count column
                if (bitPosition >= 0) {
                    AggStar.this.bitKey.set(bitPosition);
                    AggStar.this.addColumn(this);
                }
            }

            /**
             * Get the name of the column (this is the name in the database).
             */
            public String getName() {
                return name;
            }

            /**
             * Get the enclosing AggStar.Table.
             */
            public AggStar.Table getTable() {
                return AggStar.Table.this;
            }

            /**
             * Get the bit possition associted with this column. This has the
             * same value as this column's RolapStar.Column.
             */
            public int getBitPosition() {
                return bitPosition;
            }

            /**
             * Returns the datatype of this column.
             */
            public Dialect.Datatype getDatatype() {
                return datatype;
            }

            public SqlQuery getSqlQuery() {
                return getTable().getAggStar().getSqlQuery();
            }

            public MondrianDef.Expression getExpression() {
                return expression;
            }

            /**
             * Generates a SQL expression, which typically this looks like
             * this: <code><i>tableName</i>.<i>columnName</i></code>.
             */
            public String generateExprString(final SqlQuery query) {
                String usagePrefix = getUsagePrefix();
                String exprString;

                if (usagePrefix == null) {
                    exprString = getExpression().getExpression(query);
                } else {
                    MondrianDef.Expression expression = getExpression();
                    assert expression instanceof MondrianDef.Column;
                    MondrianDef.Column columnExpr =
                        (MondrianDef.Column)expression;
                    String prefixedName = usagePrefix
                        + columnExpr.getColumnName();
                    String tableName = columnExpr.getTableAlias();
                    exprString = query.getDialect().quoteIdentifier(
                        tableName, prefixedName);
                }
                return exprString;
            }

            private String getUsagePrefix() {
                String usagePrefix = null;
                if (this.getBitPosition() != -1) {
                    usagePrefix = getStar().getColumn(this.getBitPosition())
                        .getUsagePrefix();
                }
                return usagePrefix;
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
                pw.print(generateExprString(sqlQuery));
            }

            public SqlStatement.Type getInternalType() {
                return null;
            }
        }

        /**
         * This class is used for holding foreign key columns.
         * Both DimTables and FactTables can have Level columns.
         */
        final class ForeignKey extends Column {

            private ForeignKey(
                final String name,
                final MondrianDef.Expression expression,
                final Dialect.Datatype datatype,
                final int bitPosition)
            {
                super(name, expression, datatype, bitPosition);
                AggStar.this.levelBitKey.set(bitPosition);
            }
        }

        /**
         * This class is used for holding dimension level information.
         * Both DimTables and FactTables can have Level columns.
         */
        final class Level extends Column {
            private final RolapStar.Column starColumn;
            private final boolean collapsed;

            private Level(
                final String name,
                final MondrianDef.Expression expression,
                final int bitPosition,
                RolapStar.Column starColumn,
                boolean collapsed)
            {
                super(name, expression, starColumn.getDatatype(), bitPosition);
                this.starColumn = starColumn;
                this.collapsed = collapsed;
                AggStar.this.levelBitKey.set(bitPosition);
            }

            @Override
            public SqlStatement.Type getInternalType() {
                return starColumn.getInternalType();
            }

            public boolean isCollapsed() {
                return collapsed;
            }
        }

        /** The name of the table in the database. */
        private final String name;
        private final MondrianDef.Relation relation;
        protected final List<Level> levels = new ArrayList<Level>();
        protected List<DimTable> children;

        Table(final String name, final MondrianDef.Relation relation) {
            this.name = name;
            this.relation = relation;
            this.children = Collections.emptyList();
        }

        /**
         * Return the name of the table in the database.
         */
        public String getName() {
            return name;
        }

        /**
         * Return true if this table has a parent table (FactTable instances
         * do not have parent tables, all other do).
         */
        public abstract boolean hasParent();

        /**
         * Get the parent table (returns null if this table is a FactTable).
         */
        public abstract Table getParent();

        /**
         * Return true if this table has a join condition (only DimTables have
         * join conditions, FactTable instances do not).
         */
        public abstract boolean hasJoinCondition();
        public abstract Table.JoinCondition getJoinCondition();

        public MondrianDef.Relation getRelation() {
            return relation;
        }

        /**
         * Get this table's enclosing AggStar.
         */
        protected AggStar getAggStar() {
            return AggStar.this;
        }

        /**
         * Get a SqlQuery object.
         */
        protected SqlQuery getSqlQuery() {
            return getAggStar().getSqlQuery();
        }

        /**
         * Add a Level column.
         */
        protected void addLevel(final AggStar.Table.Level level) {
            this.levels.add(level);
        }

        /**
         * Returns all level columns.
         */
        public List<Level> getLevels() {
            return levels;
        }

        /**
         * Return true if table has levels.
         */
        public boolean hasLevels() {
            return ! levels.isEmpty();
        }

        /**
         * Add a child DimTable table.
         */
        protected void addTable(final DimTable child) {
            if (children == Collections.EMPTY_LIST) {
                children = new ArrayList<DimTable>();
            }
            children.add(child);
        }

        /**
         * Returns a list of child {@link Table} objects.
         */
        public List<DimTable> getChildTables() {
            return children;
        }

        /**
         * Find descendant of fact table with given name or return null.
         *
         * @param name the child table name (alias).
         * @return the child table or null.
         */
        public Table findDescendant(String name) {
            if (getName().equals(name)) {
                return this;
            }

            for (Table child : getChildTables()) {
                Table found = child.findDescendant(name);
                if (found != null) {
                    return found;
                }
            }
            return null;
        }

        /**
         * Return true if this table has one or more child tables.
         */
        public boolean hasChildren() {
            return ! children.isEmpty();
        }

        /**
         * Converts a {@link mondrian.rolap.RolapStar.Table} into a
         * {@link AggStar.DimTable} as well as converting all columns and
         * child tables. If the rightJoinConditionColumnName parameter
         * is null, then the table's namd and the rTable parameter's
         * condition left condition's column name are used to form the
         * join condition's left expression.
         */
        protected AggStar.DimTable convertTable(
            final RolapStar.Table rTable,
            final Usage usage)
        {
            String tableName = rTable.getAlias();
            MondrianDef.Relation relation = rTable.getRelation();
            RolapStar.Condition rjoinCondition = rTable.getJoinCondition();
            MondrianDef.Expression rleft = rjoinCondition.getLeft();
            final MondrianDef.Expression rright;
            if (usage == null
                || usage.getUsageType() != UsageType.LEVEL)
            {
                rright = rjoinCondition.getRight();
            } else {
                rright = usage.level.getKeyExp();
            }

            MondrianDef.Column left = null;
            if (usage != null
                && usage.rightJoinConditionColumnName != null)
            {
                left = new MondrianDef.Column(
                    getName(),
                    usage.rightJoinConditionColumnName);
            } else {
                if (rleft instanceof MondrianDef.Column) {
                    MondrianDef.Column lcolumn = (MondrianDef.Column) rleft;
                    left = new MondrianDef.Column(getName(), lcolumn.name);
                } else {
                    throw Util.newInternal("not implemented: rleft=" + rleft);
/*
                    // RME TODO can we catch this during validation
                    String msg = mres.BadRolapStarLeftJoinCondition.str(
                        "AggStar.Table",
                        rleft.getClass().getName(), left.toString());
                    getLogger().warn(msg);
*/
                }
            }
            // Explicitly set which columns are foreign keys in the
            // AggStar. This lets us later determine if a measure is
            // based upon a foreign key (see AggregationManager findAgg
            // method).
            mondrian.rolap.RolapStar.Column col =
                getAggStar().getStar().getFactTable().lookupColumn(left.name);
            if (col != null) {
                getAggStar().setForeignKey(col.getBitPosition());
            }
            JoinCondition joinCondition = new JoinCondition(left, rright);
            DimTable dimTable =
                new DimTable(this, tableName, relation, joinCondition);

            if (usage == null
                || usage.getUsageType() != UsageType.LEVEL
                || (usage.getUsageType() == UsageType.LEVEL
                    && usage.collapsed))
            {
                // Only set the bits for all levels if we are not
                // dealing with a non-collapsed AggLevel because we will
                // set the bits manually in AggStar.FactTable.loadLevel.
                dimTable.convertColumns(rTable);
            }

            dimTable.convertChildren(rTable);

            return dimTable;
        }

        /**
         * Convert a RolapStar.Table table's columns into
         * AggStar.Table.Level columns.
         */
        protected void convertColumns(final RolapStar.Table rTable) {
            // add level columns
            for (RolapStar.Column column : rTable.getColumns()) {
                String name = column.getName();
                MondrianDef.Expression expression = column.getExpression();
                int bitPosition = column.getBitPosition();

                Level level = new Level(
                    name,
                    expression,
                    bitPosition,
                    column,
                    false);
                addLevel(level);
            }
        }

        /**
         * Convert the child tables of a RolapStar.Table into
         * child AggStar.DimTable tables.
         */
        protected void convertChildren(final RolapStar.Table rTable) {
            // add children tables
            for (RolapStar.Table rTableChild : rTable.getChildren()) {
                DimTable dimChild = convertTable(rTableChild, null);

                addTable(dimChild);
            }
        }

        /**
         * This is a copy of the code found in RolapStar used to generate an SQL
         * query.
         */
        public void addToFrom(
            final SqlQuery query,
            final boolean failIfExists,
            final boolean joinToParent)
        {
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
            /**
             * The fact table column which is being aggregated.
             */
            private final MondrianDef.Expression argument;
            /**
             * For distinct-count measures, contains a bitKey of levels which
             * it is OK to roll up. For regular measures, this is empty, since
             * all levels can be rolled up.
             */
            private final BitKey rollableLevelBitKey;

            private Measure(
                final String name,
                final MondrianDef.Expression expression,
                final Dialect.Datatype datatype,
                final int bitPosition,
                final RolapAggregator aggregator,
                final MondrianDef.Expression argument)
            {
                super(name, expression, datatype, bitPosition);
                this.aggregator = aggregator;
                this.argument = argument;
                assert (argument != null) == aggregator.isDistinct();
                this.rollableLevelBitKey =
                    BitKey.Factory.makeBitKey(star.getColumnCount());

                AggStar.this.measureBitKey.set(bitPosition);
            }

            public boolean isDistinct() {
                return aggregator.isDistinct();
            }

            /**
             * Get this Measure's RolapAggregator.
             */
            public RolapAggregator getAggregator() {
                return aggregator;
            }

            /**
             * Returns a <code>BitKey</code> of the levels which can be
             * safely rolled up. (For distinct-count measures, most can't.)
             */
            public BitKey getRollableLevelBitKey() {
                return rollableLevelBitKey;
            }

            public String generateRollupString(SqlQuery query) {
                String expr = generateExprString(query);
                Aggregator rollup = null;

                BitKey fkbk = AggStar.this.getForeignKeyBitKey();
                // When rolling up and the aggregator is distinct and
                // the measure is based upon a foreign key, then
                // one must use "count" rather than "sum"
                if (fkbk.get(getBitPosition())) {
                    rollup = (getAggregator().isDistinct())
                        ? getAggregator().getNonDistinctAggregator()
                        : getAggregator().getRollup();
                } else {
                    rollup = getAggregator().getRollup();
                }

                String s = ((RolapAggregator) rollup).getExpression(expr);
                return s;
            }
            public void print(final PrintWriter pw, final String prefix) {
                SqlQuery sqlQuery = getSqlQuery();
                pw.print(prefix);
                pw.print(getName());
                pw.print(" (");
                pw.print(getBitPosition());
                pw.print("): ");
                pw.print(generateRollupString(sqlQuery));
            }
        }

        private Column factCountColumn;
        private final List<Measure> measures;
        private final int totalColumnSize;
        private int numberOfRows;

        FactTable(final JdbcSchema.Table aggTable) {
            this(
                aggTable.getName(),
                aggTable.table,
                aggTable.getTotalColumnSize(),
                aggTable.getNumberOfRows());
        }

        FactTable(
            final String name,
            final MondrianDef.Relation relation,
            final int totalColumnSize,
            final int numberOfRows)
        {
            super(name, relation);
            this.totalColumnSize = totalColumnSize;
            this.measures = new ArrayList<Measure>();
            this.numberOfRows = numberOfRows;
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
         */
        public int getVolume() {
            return getTotalColumnSize() * getNumberOfRows();
        }

        /**
         * Get the total size of all columns in a row.
         */
        public int getTotalColumnSize() {
            return totalColumnSize;
        }

        /**
         * Get the number of rows in this aggregate table.
         */
        public int getNumberOfRows() {
            if (numberOfRows < 0) {
                numberOfRows =
                    star.getStatisticsCache().getRelationCardinality(
                        getRelation(), getName(), approxRowCount);
                makeNumberOfRows();
            }
            return numberOfRows;
        }

        /**
         * This is for testing ONLY.
         */
        void setNumberOfRows(int numberOfRows) {
            this.numberOfRows = numberOfRows;
        }

        /**
         * Returns a list of all measures.
         */
        public List<Measure> getMeasures() {
            return measures;
        }

        /**
         * Return true it table has measures
         */
        public boolean hasMeasures() {
            return ! measures.isEmpty();
        }

        /**
         * Returns a list of the columns in this table.
         */
        public List<Column> getColumns() {
            List<Column> list = new ArrayList<Column>();
            list.addAll(measures);
            list.addAll(levels);
            for (DimTable dimTable : getChildTables()) {
                dimTable.addColumnsToList(list);
            }
            return list;
        }

        /**
         * For a foreign key usage create a child DimTable table.
         */
        private void loadForeignKey(final JdbcSchema.Table.Column.Usage usage) {
            if (usage.rTable != null) {
                DimTable child = convertTable(
                    usage.rTable,
                    usage);
                addTable(child);
            } else {
                // it a column thats not a measure or foreign key - it must be
                // a non-shared dimension
                // See: AggTableManager.java
                JdbcSchema.Table.Column column = usage.getColumn();
                String name = column.getName();
                String symbolicName = usage.getSymbolicName();
                if (symbolicName == null) {
                    symbolicName = name;
                }

                MondrianDef.Expression expression =
                    new MondrianDef.Column(getName(), name);
                Dialect.Datatype datatype = column.getDatatype();
                RolapStar.Column rColumn = usage.rColumn;
                if (rColumn == null) {
                    getLogger().warn(
                        "loadForeignKey: for column "
                        + name
                        + ", rColumn == null");
                } else {
                    int bitPosition = rColumn.getBitPosition();
                    ForeignKey c =
                        new ForeignKey(
                            symbolicName, expression, datatype, bitPosition);
                    getAggStar().setForeignKey(c.getBitPosition());
                }
            }
        }

        /**
         * Given a usage of type measure, create a Measure column.
         */
        private void loadMeasure(final JdbcSchema.Table.Column.Usage usage) {
            final JdbcSchema.Table.Column column = usage.getColumn();
            String name = column.getName();
            String symbolicName = usage.getSymbolicName();
            if (symbolicName == null) {
                symbolicName = name;
            }
            Dialect.Datatype datatype = column.getDatatype();
            RolapAggregator aggregator = usage.getAggregator();

            MondrianDef.Expression expression;
            if (column.hasUsage(JdbcSchema.UsageType.FOREIGN_KEY)
                && !aggregator.isDistinct())
            {
                expression = factCountColumn.getExpression();
            } else {
                expression = new MondrianDef.Column(getName(), name);
            }

            MondrianDef.Expression argument;
            if (aggregator.isDistinct()) {
                argument = usage.rMeasure.getExpression();
            } else {
                argument = null;
            }

            int bitPosition = usage.rMeasure.getBitPosition();

            Measure aggMeasure =
                new Measure(
                    symbolicName,
                    expression,
                    datatype,
                    bitPosition,
                    aggregator,
                    argument);

            measures.add(aggMeasure);

            if (aggMeasure.aggregator.isDistinct()) {
                distinctMeasureBitKey.set(bitPosition);
            }
        }

        /**
         * Create a fact_count column for a usage of type fact count.
         */
        private void loadFactCount(final JdbcSchema.Table.Column.Usage usage) {
            String name = usage.getColumn().getName();
            String symbolicName = usage.getSymbolicName();
            if (symbolicName == null) {
                symbolicName = name;
            }

            MondrianDef.Expression expression =
                new MondrianDef.Column(getName(), name);
            Dialect.Datatype datatype = usage.getColumn().getDatatype();
            int bitPosition = -1;

            Column aggColumn =
                new Column(
                    symbolicName,
                    expression,
                    datatype,
                    bitPosition);

            factCountColumn = aggColumn;
        }

        /**
         * Given a usage of type level, create a Level column.
         */
        private void loadLevel(final JdbcSchema.Table.Column.Usage usage) {
            String name = usage.getSymbolicName();
            MondrianDef.Expression expression =
                new MondrianDef.Column(getName(), usage.levelColumnName);
            int bitPosition = usage.rColumn.getBitPosition();
            Level level =
                new Level(
                    name,
                    expression,
                    bitPosition,
                    usage.rColumn,
                    usage.collapsed);
            addLevel(level);

            // If we are dealing with a non-collapsed level, we have to
            // modify the bit key of the AggStar and create a column
            // object for each parent level so that the AggQuerySpec
            // can correctly link up to the other tables.
            if (!usage.collapsed) {
                // We must also update the bit key with
                // the parent levels of any non-collapsed level.
                RolapLevel parentLevel =
                    (RolapLevel)usage.level.getParentLevel();
                while (parentLevel != null && !parentLevel.isAll()) {
                    // Find the bit for this AggStar's bit key for each parent
                    // level. There is no need to modify the AggStar's bit key
                    // directly here, because the constructor of Column
                    // will do that for us later on.
                    final BitKey bk = AggStar.this.star.getBitKey(
                        new String[] {
                            parentLevel.getKeyExp().getTableAlias()},
                        new String[] {
                            ((MondrianDef.Column)parentLevel.getKeyExp())
                                .getColumnName()});
                    final int bitPos = bk.nextSetBit(0);
                    if (bitPos == -1) {
                        throw new MondrianException(
                            "Failed to match non-collapsed aggregate level with a column from the RolapStar.");
                    }
                    // Now we will create the Column object to return to the
                    // AggQuerySpec. We will use the convertTable() method
                    // because it is convenient and it is capable to convert
                    // our base table into a series of parent-child tables
                    // with their join paths figured out.
                    DimTable columnTable =
                        convertTable(
                            AggStar.this.star.getColumn(bitPosition)
                                .getTable(),
                            usage);
                    // Make sure to return the last child table, since
                    // AggQuerySpec will take care of going up the
                    // parent-child hierarchy and do all the work for us.
                    while (columnTable.getChildTables().size() > 0) {
                        columnTable = columnTable.getChildTables().get(0);
                    }
                    final DimTable finalColumnTable = columnTable;
                    levelColumnsToJoin.put(
                        bitPos,
                        new Column(
                            ((MondrianDef.Column)parentLevel.getKeyExp())
                                .getColumnName(),
                            parentLevel.getKeyExp(),
                            AggStar.this.star.getColumn(bitPos).getDatatype(),
                            bitPos)
                        {
                            public Table getTable() {
                                return finalColumnTable;
                            }
                        });
                    // Do the next parent level.
                    parentLevel = (RolapLevel) parentLevel.getParentLevel();
                }
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
            pw.print("numberofrows=");
            pw.println(getNumberOfRows());

            pw.print(subprefix);
            pw.println("FactCount:");
            factCountColumn.print(pw, subsubprefix);
            pw.println();

            pw.print(subprefix);
            pw.println("Measures:");
            for (Measure column : getMeasures()) {
                column.print(pw, subsubprefix);
                pw.println();
            }

            pw.print(subprefix);
            pw.println("Levels:");
            for (Level level : getLevels()) {
                level.print(pw, subsubprefix);
                pw.println();
            }

            for (DimTable child : getChildTables()) {
                child.print(pw, subprefix);
            }
        }

        private void makeNumberOfRows() {
            if (approxRowCount >= 0) {
                numberOfRows = approxRowCount;
                return;
            }
            SqlQuery query = getSqlQuery();
            query.addSelect("count(*)", null);
            query.addFrom(getRelation(), getName(), false);
            DataSource dataSource = getAggStar().getStar().getDataSource();
            SqlStatement stmt =
                RolapUtil.executeQuery(
                    dataSource,
                    query.toString(),
                    new Locus(
                        new Execution(
                            star.getSchema().getInternalConnection()
                                .getInternalStatement(),
                            0),
                        "AggStar.FactTable.makeNumberOfRows",
                        "Counting rows in aggregate table"));
            try {
                ResultSet resultSet = stmt.getResultSet();
                if (resultSet.next()) {
                    ++stmt.rowCount;
                    numberOfRows = resultSet.getInt(1);
                } else {
                    getLogger().warn(
                        mres.SqlQueryFailed.str(
                            "AggStar.FactTable.makeNumberOfRows",
                            query.toString()));

                    // set to large number so that this table is never used
                    numberOfRows = Integer.MAX_VALUE / getTotalColumnSize();
                }
            } catch (SQLException e) {
                throw stmt.handle(e);
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * This class represents a dimension table.
     */
    public class DimTable extends Table {
        private final Table parent;
        private final JoinCondition joinCondition;

        DimTable(
            final Table parent,
            final String name,
            final MondrianDef.Relation relation,
            final JoinCondition joinCondition)
        {
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
         */
        public void addColumnsToList(final List<Column> list) {
            list.addAll(levels);
            for (DimTable dimTable : getChildTables()) {
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

            for (Level level : getLevels()) {
                level.print(pw, subsubprefix);
                pw.println();
            }

            joinCondition.print(pw, subprefix);

            for (DimTable child : getChildTables()) {
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
     */
    public void print(final PrintWriter pw, final String prefix) {
        pw.print(prefix);
        pw.println("AggStar:" + getFactTable().getName());
        String subprefix = prefix + "  ";

        pw.print(subprefix);
        pw.print(" bk=");
        pw.println(bitKey);

        pw.print(subprefix);
        pw.print("fbk=");
        pw.println(levelBitKey);

        pw.print(subprefix);
        pw.print("mbk=");
        pw.println(measureBitKey);

        pw.print(subprefix);
        pw.print("has foreign key=");
        pw.println(aggTable.hasChildren());

        for (AggStar.Table.Column column
                : getFactTable().getColumns())
        {
            pw.print("    ");
            pw.println(column);
        }

        aggTable.print(pw, subprefix);
    }
}

// End AggStar.java
