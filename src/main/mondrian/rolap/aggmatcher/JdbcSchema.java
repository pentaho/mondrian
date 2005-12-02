/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapStar;
import mondrian.resource.MondrianResource;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;

/**
 * This class is used to scrap a database and store information about its
 * tables and columns.
 * A database has tables. A table has columns. A column has one or more usages.
 * A usage might be a column being used as a foreign key or as part of a
 * measure.
 * <p>
 * Tables are created when calling code requests the set of available
 * tables. This call <code>getTables()</code> causes all tables to be loaded.
 * But a table's columns are not loaded until, on a table-by-table basis,
 * a request is made to get the set of columns associated with the table.
 * Since, the AggTableManager first attempts table name matches (recognition)
 * most tables do not match, so why load their columns.
 * Of course, as a result, there are a host of methods that can throw an
 * SQLException, rats.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 */
public class JdbcSchema {
    private static final Logger LOGGER =
        Logger.getLogger(JdbcSchema.class);

    private static final MondrianResource mres = MondrianResource.instance();

    /**
     * Get the Logger.
     *
     * @return
     */
    public Logger getLogger() {
        return LOGGER;
    }

    public interface Factory {
        JdbcSchema makeDB(DataSource dataSource);
        void clearDB(DataSource dataSource);
    }

    public static class StdFactory implements Factory {
        private final WeakHashMap dbMap = new WeakHashMap();
        StdFactory() {
        }
        public JdbcSchema makeDB(DataSource dataSource) {
            JdbcSchema db = (JdbcSchema) dbMap.get(dataSource);
            if (db == null) {
                db = new JdbcSchema(dataSource);
                dbMap.put(dataSource, db);
            }
            return db;
        }
        public void clearDB(DataSource dataSource) {
            JdbcSchema db = (JdbcSchema) dbMap.get(dataSource);
            if (db != null) {
                db.clear();
            }
        }
    }

    public static final String FACTORY_CLASS =
                        "mondrian.rolap.aggregates.jdbcFactoryClass";
    private static Factory factory;

    private static void makeFactory() {
        if (factory == null) {
            String classname = System.getProperty(FACTORY_CLASS);
            if (classname == null) {
                factory = new StdFactory();
            } else {
                try {
                    Class clz = Class.forName(classname);
                    factory = (Factory) clz.newInstance();
                } catch (ClassNotFoundException ex) {
                    throw mres.BadJdbcFactoryClassName.ex(classname);
                } catch (InstantiationException ex) {
                    throw mres.BadJdbcFactoryInstantiation.ex(classname);
                } catch (IllegalAccessException ex) {
                    throw mres.BadJdbcFactoryAccess.ex(classname);
                }
            }
        }
    }
    public static synchronized void clearDB(DataSource dataSource) {
        makeFactory();
        factory.clearDB(dataSource);
    }

    public static synchronized JdbcSchema makeDB(DataSource dataSource) {
        makeFactory();
        return factory.makeDB(dataSource);
    }

    //
    // Types of column usages.
    //
    public static final int UNKNOWN_COLUMN_USAGE         = 0x0001;
    public static final int FOREIGN_KEY_COLUMN_USAGE     = 0x0002;
    public static final int MEASURE_COLUMN_USAGE         = 0x0004;
    public static final int LEVEL_COLUMN_USAGE           = 0x0008;
    public static final int FACT_COUNT_COLUMN_USAGE      = 0x0010;
    public static final int IGNORE_COLUMN_USAGE          = 0x0020;

    public static final String UNKNOWN_COLUMN_NAME         = "UNKNOWN";
    public static final String FOREIGN_KEY_COLUMN_NAME     = "FOREIGN_KEY";
    public static final String MEASURE_COLUMN_NAME         = "MEASURE";
    public static final String LEVEL_COLUMN_NAME           = "LEVEL";
    public static final String FACT_COUNT_COLUMN_NAME      = "FACT_COUNT";
    public static final String IGNORE_COLUMN_NAME          = "IGNORE";

    /**
     * Determine if the parameter represents a single column type, i.e., the
     * column only has one usage.
     *
     * @param columnType
     * @return true if column has only one usage.
     */
    public static boolean isUniqueColumnType(int columnType) {
        switch (columnType) {
        case UNKNOWN_COLUMN_USAGE :
            return true;
        case FOREIGN_KEY_COLUMN_USAGE :
            return true;
        case MEASURE_COLUMN_USAGE :
            return true;
        case LEVEL_COLUMN_USAGE :
            return true;
        case FACT_COUNT_COLUMN_USAGE :
            return true;
        case IGNORE_COLUMN_USAGE :
            return true;
        default :
            return false;
        }
    }

    /**
     * Map from column type enum to column type name or list of names if the
     * parameter represents more than on usage.
     *
     * @param columnType
     * @return
     */
    public static String convertColumnTypeToName(int columnType) {
        switch (columnType) {
        case UNKNOWN_COLUMN_USAGE :
            return UNKNOWN_COLUMN_NAME;
        case FOREIGN_KEY_COLUMN_USAGE :
            return FOREIGN_KEY_COLUMN_NAME;
        case MEASURE_COLUMN_USAGE :
            return MEASURE_COLUMN_NAME;
        case LEVEL_COLUMN_USAGE :
            return LEVEL_COLUMN_NAME;
        case FACT_COUNT_COLUMN_USAGE :
            return FACT_COUNT_COLUMN_NAME;
        case IGNORE_COLUMN_USAGE :
            return IGNORE_COLUMN_NAME;
        default :
            // its a multi-purpose column
            StringBuffer buf = new StringBuffer();
            if ((columnType & UNKNOWN_COLUMN_USAGE) != 0) {
                buf.append(UNKNOWN_COLUMN_NAME);
            }
            if ((columnType & FOREIGN_KEY_COLUMN_USAGE) != 0) {
                if (buf.length() != 0) {
                    buf.append('|');
                }
                buf.append(FOREIGN_KEY_COLUMN_NAME);
            }
            if ((columnType & MEASURE_COLUMN_USAGE) != 0) {
                if (buf.length() != 0) {
                    buf.append('|');
                }
                buf.append(MEASURE_COLUMN_NAME);
            }
            if ((columnType & LEVEL_COLUMN_USAGE) != 0) {
                if (buf.length() != 0) {
                    buf.append('|');
                }
                buf.append(LEVEL_COLUMN_NAME);
            }
            if ((columnType & FACT_COUNT_COLUMN_USAGE) != 0) {
                if (buf.length() != 0) {
                    buf.append('|');
                }
                buf.append(FACT_COUNT_COLUMN_NAME);
            }
            if ((columnType & IGNORE_COLUMN_USAGE) != 0) {
                if (buf.length() != 0) {
                    buf.append('|');
                }
                buf.append(IGNORE_COLUMN_NAME);
            }
            return buf.toString();
        }
    }

    /**
     * Returns true if the parameter is a java.sql.Type numeric type.
     *
     * @param javaType
     * @return
     */
    public static boolean isNumeric(int javaType) {
        switch (javaType) {
        case Types.TINYINT :
        case Types.SMALLINT :
        case Types.INTEGER :
        case Types.BIGINT :
        case Types.FLOAT :
        case Types.REAL :
        case Types.DOUBLE :
        case Types.NUMERIC :
        case Types.DECIMAL :
            return true;
        default :
            return false;
        }
    }
    /**
     * Returns true if the parameter is a java.sql.Type text type.
     *
     * @param javaType
     * @return
     */
    public static boolean isText(int javaType) {
        switch (javaType) {
        case Types.CHAR :
        case Types.VARCHAR :
        case Types.LONGVARCHAR :
            return true;
        default :
            return false;
        }
    }

    //
    // Usages of tables.
    //
    public static final int UNKNOWN_TABLE_USAGE         = 10;
    public static final int FACT_TABLE_USAGE            = 11;
    public static final int AGG_TABLE_USAGE             = 12;

    public static final String UNKNOWN_TABLE_USAGE_NAME = "UNKNOWN";
    public static final String FACT_TABLE_USAGE_NAME    = "FACT";
    public static final String AGG_TABLE_USAGE_NAME     = "AGG";

    /**
     * Convert from table usage enum to table usage name.
     *
     * @param tableUsage
     * @return
     */
    public static String convertTableUsageToName(int tableUsage) {
        switch (tableUsage) {
        case UNKNOWN_TABLE_USAGE :
            return UNKNOWN_TABLE_USAGE_NAME;
        case FACT_TABLE_USAGE :
            return FACT_TABLE_USAGE_NAME;
        case AGG_TABLE_USAGE :
            return AGG_TABLE_USAGE_NAME;
        default :
            return UNKNOWN_TABLE_USAGE_NAME;
        }
    }

    //
    // Types of tables.
    //
    public static final String UNKNOWN_TABLE_TYPE       = "UNKNOWN";
    public static final String TABLE_TABLE_TYPE         = "TABLE";
    public static final String VIEW_TYPE                = "VIEW";
    public static final String SYSTEM_TABLE_TABLE_TYPE  = "SYSTEM TABLE";
    public static final String GLOBAL_TEMP_TABLE_TYPE   = "GLOBAL TEMPORARY";
    public static final String LOCAL_TEMP_TABLE_TYPE    = "LOCAL TEMPORARY";
    public static final String ALIAS_TABLE_TYPE         = "ALIAS";
    public static final String SYNONYM_TABLE_TYPE       = "SYNONYM";

    /**
     * A table in a database.
     */
    public class Table {

        /**
         * A column in a table.
         */
        public class Column {

            /**
             * A usage of a column.
             */
            public class Usage {
                private final int columnType;
                private String symbolicName;
                private RolapAggregator aggregator;

                ////////////////////////////////////////////////////
                //
                // These instance variables are used to hold
                // stuff which is determines at one place and
                // then used somewhere else. Generally, a usage
                // is created before all of its "stuff" can be
                // determined, hence, usage is not a set of classes,
                // rather its one class with a bunch of instance
                // variables which may or may not be used.
                //

                // measure stuff
                public RolapStar.Measure rMeasure;

                // hierarchy stuff
                public MondrianDef.Relation relation;
                public MondrianDef.Expression joinExp;
                public String levelColumnName;

                // level
                public RolapStar.Column rColumn;

                // for subtables
                public RolapStar.Table rTable;
                public String rightJoinConditionColumnName;

                // It is used to hold the (possible null) prefix to
                // use during aggregate table generation (See AggGen).
                public String usagePrefix;
                //
                ////////////////////////////////////////////////////

                Usage(final int columnType) {
                    this.columnType = columnType;
                }

                /**
                 * This is the column with which this usage is associated.
                 *
                 * @return the usage's column.
                 */
                public Column getColumn() {
                    return JdbcSchema.Table.Column.this;
                }

                /**
                 * The column usage type.
                 *
                 * @return
                 */
                public int getColumnType() {
                    return columnType;
                }

                /**
                 * Is this usage of this type.
                 *
                 * @param columnType
                 * @return
                 */
                public boolean isColumnType(final int columnType) {
                    return ((this.columnType & columnType) != 0);
                }

                /**
                 * Set the symbolic (logical) name associated with this usage.
                 * For example, this might be the measure's name.
                 *
                 * @param symbolicName
                 */
                public void setSymbolicName(final String symbolicName) {
                    this.symbolicName = symbolicName;
                }

                /**
                 * Get usage's symbolic name.
                 *
                 * @return
                 */
                public String getSymbolicName() {
                    return symbolicName;
                }

                /**
                 * Set the aggregator associated with this usage (if its a
                 * measure usage).
                 *
                 * @param aggregator
                 */
                public void setAggregator(final RolapAggregator aggregator) {
                    this.aggregator = aggregator;
                }
                /**
                 * Get the aggregator associated with this usage (if its a
                 * measure usage, otherwise null).
                 *
                 * @return
                 */
                public RolapAggregator getAggregator() {
                    return aggregator;
                }

                public String toString() {
                    StringWriter sw = new StringWriter(64);
                    PrintWriter pw = new PrintWriter(sw);
                    print(pw, "");
                    pw.flush();
                    return sw.toString();
                }
                public void print(final PrintWriter pw, final String prefix) {
                    if (getSymbolicName() != null) {
                        pw.print("symbolicName=");
                        pw.print(getSymbolicName());
                    }
                    if (getAggregator() != null) {
                        pw.print(", aggregator=");
                        pw.print(getAggregator().getName());
                    }
                    pw.print(", columnType=");
                    pw.print(convertColumnTypeToName(getColumnType()));
                }

            }

            /** This is the name of the column. */
            private final String name;

            /** This is the java.sql.Type enum of the column in the database. */
            private int type;
            /**
             * This is the java.sql.Type name of the column in the database.
             */
            private String typeName;

            /** This is the size of the column in the database. */
            private int columnSize;

            /** The number of fractional digits. */
            private int decimalDigits;

            /** Radix (typically either 10 or 2). */
            private int numPrecRadix;

            /** For char types the maximum number of bytes in the column. */
            private int charOctetLength;

            /**
             * False means the column definitely does not allow NULL values.
             */
            private boolean isNullable;

            public final MondrianDef.Column column;

            private final List usages;

            /**
             * This contains the enums of all of the column's usages.
             */
            private int columnType;

            private Column(final String name) {
                this.name = name;
                this.column = new MondrianDef.Column(
                                        JdbcSchema.Table.this.getName(),
                                        name);
                this.usages = new ArrayList();
            }
            /**
             * For testing ONLY
            JdbcSchema.Table.Column copy() {
                Column column = new Column(name);
                column.type = type;
                column.typeName = typeName;
                column.columnSize = columnSize;
                column.decimalDigits = decimalDigits;
                column.numPrecRadix = numPrecRadix;
                column.charOctetLength = charOctetLength;
                column.isNullable = isNullable;

                return column;
            }
             */
            /**
             * For testing ONLY
            void clearUsages() {
                // empty
            }
             */


            /**
             * This is the column's name in the database, not a symbolic name.
             *
             * @return
             */
            public String getName() {
                return name;
            }

            /**
             * Set the columns java.sql.Type enun of the column.
             *
             * @param type
             */
            private void setType(final int type) {
                this.type = type;
            }

            /**
             * Get the columns java.sql.Type enun of the column.
             *
             * @return
             */
            public int getType() {
                return type;
            }

            /**
             * Set the columns java.sql.Type name.
             *
             * @param typeName
             */
            private void setTypeName(final String typeName) {
                this.typeName = typeName;
            }

            /**
             * Get the columns java.sql.Type name.
             *
             * @return
             */
            public String getTypeName() {
                return typeName;
            }

            /**
             * Get this column's table.
             *
             * @return
             */
            public Table getTable() {
                return JdbcSchema.Table.this;
            }

            /**
             * Return true if this column is numeric.
             *
             * @return
             */
            public boolean isNumeric() {
                return JdbcSchema.isNumeric(getType());
            }

            /**
             * Set the size in bytes of the column in the database.
             *
             * @param columnSize
             */
            private void setColumnSize(final int columnSize) {
                this.columnSize = columnSize;
            }

            /**
             * Get the size in bytes of the column in the database.
             *
             *
             * @return
             */
            public int getColumnSize() {
                return columnSize;
            }

            /**
             * Set number of fractional digits.
             *
             * @param decimalDigits
             */
            private void setDecimalDigits(final int decimalDigits) {
                this.decimalDigits = decimalDigits;
            }

            /**
             * Get number of fractional digits.
             *
             * @return
             */
            public int getDecimalDigits() {
                return decimalDigits;
            }

            /**
             * Set Radix (typically either 10 or 2).
             *
             * @param numPrecRadix
             */
            private void setNumPrecRadix(final int numPrecRadix) {
                this.numPrecRadix = numPrecRadix;
            }

            /**
             * Get Radix (typically either 10 or 2).
             *
             * @return
             */
            public int getNumPrecRadix() {
                return numPrecRadix;
            }

            /**
             * For char types the maximum number of bytes in the column.
             *
             * @param charOctetLength
             */
            private void setCharOctetLength(final int charOctetLength) {
                this.charOctetLength = charOctetLength;
            }

            /**
             * For char types the maximum number of bytes in the column.
             *
             * @return
             */
            public int getCharOctetLength() {
                return charOctetLength;
            }

            /**
             * False means the column definitely does not allow NULL values.
             *
             * @param isNullable
             */
            private void setIsNullable(final boolean isNullable) {
                this.isNullable = isNullable;
            }

            /**
             * False means the column definitely does not allow NULL values.
             *
             * @return
             */
            public boolean isNullable() {
                return isNullable;
            }

            /**
             * How many usages does this column have. A column has
             * between 0 and N usages. It has no usages if it is some
             * administrative column. It has one usage if, for example, its
             * the fact_count column or a level column (for a collapsed
             * dimension aggregate). It might have 2 usages if its a foreign key
             * that is also used as a measure. If its a column used in N
             * measures, then it will have N usages.
             *
             * @return
             */
            public int numberOfUsages() {
                return usages.size();
            }

            /**
             * Return true if the column has at least one usage.
             *
             * @return
             */
            public boolean hasUsage() {
                return (usages.size() != 0);
            }

            /**
             * Return true if the column has at least one usage of the given
             * column type.
             *
             * @param columnType
             * @return
             */
            public boolean hasUsage(final int columnType) {
                return ((this.columnType & columnType) != 0);
            }

            /**
             * Get an iterator over all usages.
             *
             * @return
             */
            public Iterator getUsages() {
                return usages.iterator();
            }

            /**
             * Get an iterator over all usages of the given column type.
             *
             * @param columnType
             * @return
             */
            public Iterator getUsages(final int columnType) {

                // Yes, this is legal.
                class ColumnTypeIterator implements Iterator {
                    private final Iterator it;
                    private final int columnType;
                    private Object nextObject;

                    ColumnTypeIterator(final Iterator it,
                                       final int columnType) {
                        this.it = it;
                        this.columnType = columnType;
                    }
                    public boolean hasNext() {
                        while (it.hasNext()) {
                            Object o = it.next();
                            if (isColumnType(o, columnType)) {
                                nextObject = o;
                                return true;
                            }

                        }
                        nextObject = null;
                        return false;
                    }
                    protected boolean isColumnType(Object o, int columnType) {
                        return ((Usage)o).isColumnType(columnType);
                    }

                    public Object next() {
                        return nextObject;
                    }
                    public void remove() {
                        it.remove();
                    }
                }

                return new ColumnTypeIterator(getUsages(), columnType);
            }

            /**
             * Create a new usage of a given column type.
             *
             * @param columnType
             * @return
             */
            public Usage newUsage(int columnType) {
                this.columnType |= columnType;

                Usage usage = new Usage(columnType);
                usages.add(usage);
                return usage;
            }

            public String toString() {
                StringWriter sw = new StringWriter(256);
                PrintWriter pw = new PrintWriter(sw);
                print(pw, "");
                pw.flush();
                return sw.toString();
            }

            public void print(final PrintWriter pw, final String prefix) {
                pw.print(prefix);
                pw.print("name=");
                pw.print(getName());
                pw.print(", typename=");
                pw.print(getTypeName());
                pw.print(", size=");
                pw.print(getColumnSize());

                switch (getType()) {
                case Types.TINYINT :
                case Types.SMALLINT :
                case Types.INTEGER :
                case Types.BIGINT :
                case Types.FLOAT :
                case Types.REAL :
                case Types.DOUBLE :
                    break;
                case Types.NUMERIC :
                case Types.DECIMAL :
                    pw.print(", decimalDigits=");
                    pw.print(getDecimalDigits());
                    pw.print(", numPrecRadix=");
                    pw.print(getNumPrecRadix());
                    break;
                case Types.CHAR :
                case Types.VARCHAR :
                    pw.print(", charOctetLength=");
                    pw.print(getCharOctetLength());
                    break;
                case Types.LONGVARCHAR :
                case Types.DATE :
                case Types.TIME :
                case Types.TIMESTAMP :
                case Types.BINARY :
                case Types.VARBINARY :
                case Types.LONGVARBINARY :
                default:
                    break;
                }
                pw.print(", isNullable=");
                pw.print(isNullable());

                if (hasUsage()) {
                    pw.print(" Usages [");
                    for (Iterator it = getUsages(); it.hasNext(); ) {
                        Usage u = (Usage) it.next();
                        pw.print('(');
                        u.print(pw, prefix);
                        pw.print(')');
                    }
                    pw.println("]");
                }
            }
        }

        /** Name of table. */
        private final String name;
        /** Map from column name to column. */
        private Map columnMap;
        /** Sum of all of the table's column's column sizes. */
        private int totalColumnSize;
        /**
         * Is the table a fact, aggregate or other table type.
         * Note: this assumes that a table has only ONE usage.
         */
        private int tableUsage;

        /**
         * Typical table types are: "TABLE", "VIEW", "SYSTEM TABLE",
         * "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
         */
        private String tableType;

        // mondriandef stuff
        public MondrianDef.Table table;

        private boolean allColumnsLoaded;

        private Table(final String name) {
            this.name = name;
            this.tableUsage = UNKNOWN_TABLE_USAGE;
            this.tableType = UNKNOWN_TABLE_TYPE;
        }

        public void load() throws SQLException {
            loadColumns();
        }

        /**
         * For testing ONLY
        JdbcSchema.Table copy() {
            Table table = new Table(name);
            table.totalColumnSize = totalColumnSize;
            table.tableUsage = tableUsage;
            table.tableType = tableType;

            Map m = table.getColumnMap();
            for (Iterator it = getColumns(); it.hasNext(); ) {
                Column column = (Column) it.next();
                m.put(column.getName(), column.copy());
            }

            return table;
        }
         */
        /**
         * For testing ONLY
        void clearUsages() {
            this.tableUsage = UNKNOWN_TABLE_USAGE;
            for (Iterator it = getColumns(); it.hasNext(); ) {
                Column column = (Column) it.next();
                column.clearUsages();
            }
        }
         */

        /**
         * Get the name of the table.
         *
         * @return
         */
        public String getName() {
            return name;
        }

        /**
         * Get the total size of a row (sum of the column sizes).
         *
         * @return
         */
        public int getTotalColumnSize() {
            return totalColumnSize;
        }

        /**
         * Get the number of rows in the table.
         *
         * @return
         */
        public int getNumberOfRows() {
            return -1;
        }

        /**
         * Iterate of the table's columns.
         *
         * @return
         */
        public Iterator getColumns() {
            return getColumnMap().values().iterator();
        }

        /**
         * Iterate over all all column usages of a give column type.
         *
         * @param columnType
         * @return
         */
        public Iterator getColumnUsages(final int columnType) {

            class CTIterator implements Iterator {
                private final Iterator columns;
                private final int columnType;
                private Iterator it;
                private Object nextObject;

                CTIterator(final Iterator columns, final int columnType) {
                    this.columns = columns;
                    this.columnType = columnType;
                }
                public boolean hasNext() {
                    while (true) {
                        while ((it == null) || ! it.hasNext()) {
                            if (! columns.hasNext()) {
                                nextObject = null;
                                return false;
                            }
                            Column c = (Column) columns.next();
                            it = c.getUsages();
                        }
                        JdbcSchema.Table.Column.Usage usage =
                            (JdbcSchema.Table.Column.Usage) it.next();
                        if (usage.isColumnType(columnType)) {
                            nextObject = usage;
                            return true;
                        }
                    }
                }
                public Object next() {
                    return nextObject;
                }
                public void remove() {
                    it.remove();
                }
            }
            return new CTIterator(getColumns(), columnType);
        }

        /**
         * Get a column by its name.
         *
         * @param columnName
         * @return
         */
        public Column getColumn(final String columnName) {
            return (Column) getColumnMap().get(columnName);
        }

        /**
         * Return true if this table contains a column with the given name.
         *
         * @param columnName
         * @return
         */
        public boolean constainsColumn(final String columnName) {
            return getColumnMap().containsKey(columnName);
        }

        /**
         * Set the table usage (fact, aggregate or other).
         *
         * @param tableUsage
         */
        public void setTableUsage(final int tableUsage) {
            // if it has already been set, then it can NOT be reset
            if ((this.tableUsage != UNKNOWN_TABLE_USAGE) &&
                    (this.tableUsage != tableUsage)) {

                throw mres.AttemptToChangeTableUsage.ex(
                    getName(),
                    convertTableUsageToName(this.tableUsage),
                    convertTableUsageToName(tableUsage));
            }
            this.tableUsage = tableUsage;
        }

        /**
         * Get the table's usage.
         *
         * @return
         */
        public int getTableUsage() {
            return tableUsage;
        }

        /**
         * Set the table type
         *
         * @param tableType
         */
        public void setTableType(final String tableType) {
            // if it has already been set, then it can NOT be reset
            if ((this.tableType != UNKNOWN_TABLE_TYPE) &&
                    (! this.tableType.equals(tableType))) {

                throw mres.AttemptToChangeTableType.ex(
                    getName(),
                    this.tableType,
                    tableType);
            }
            this.tableType = tableType;
        }
        /**
         * Get the table's type.
         *
         * @return
         */
        public String getTableType() {
            return tableType;
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }
        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("Table:");
            String subprefix = prefix + "  ";
            String subsubprefix = subprefix + "  ";

            pw.print(subprefix);
            pw.print("name=");
            pw.print(getName());
            pw.print(", type=");
            pw.print(getTableType());
            pw.print(", usage=");
            pw.println(convertTableUsageToName(getTableUsage()));

            pw.print(subprefix);
            pw.print("totalColumnSize=");
            pw.println(getTotalColumnSize());

            pw.print(subprefix);
            pw.println("Columns: [");
            Iterator it = getColumnMap().values().iterator();
            while (it.hasNext()) {
                Column column = (Column) it.next();
                column.print(pw, subsubprefix);
                pw.println();
            }
            pw.print(subprefix);
            pw.println("]");
        }

        /**
         * Get all of the columns associated with a table and create Column
         * objects with the column's name, type, type name and column size.
         *
         * @throws SQLException
         */
        private void loadColumns() throws SQLException {
            if (! allColumnsLoaded) {
                Connection conn = JdbcSchema.this.getConnection();
                try {
                    DatabaseMetaData dmd = conn.getMetaData();

                    String schema = JdbcSchema.this.getSchemaName();
                    String catalog = JdbcSchema.this.getCatalogName();
                    String tableName = getName();
                    String columnNamePattern = "%";

                    ResultSet rs = null;
                    try {
                        Map map = getColumnMap();
                        rs = dmd.getColumns(catalog,
                                            schema,
                                            tableName,
                                            columnNamePattern);
                        while (rs.next()) {
                            String name = rs.getString(4);
                            int type = rs.getInt(5);
                            String typeName = rs.getString(6);
                            int columnSize = rs.getInt(7);
                            int decimalDigits = rs.getInt(9);
                            int numPrecRadix = rs.getInt(10);
                            int charOctetLength = rs.getInt(16);
                            String isNullable = rs.getString(18);

                            Column column = new Column(name);
                            column.setType(type);
                            column.setTypeName(typeName);
                            column.setColumnSize(columnSize);
                            column.setDecimalDigits(decimalDigits);
                            column.setNumPrecRadix(numPrecRadix);
                            column.setCharOctetLength(charOctetLength);
                            column.setIsNullable(! isNullable.equals("NO"));

                            map.put(name, column);
                            totalColumnSize += column.getColumnSize();
                        }
                    } finally {
                        if (rs != null) {
                            rs.close();
                        }
                    }
                } finally {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        //ignore
                    }
                }

                allColumnsLoaded = true;
            }
        }
        private Map getColumnMap() {
            if (columnMap == null) {
                columnMap = new HashMap();
            }
            return columnMap;
        }
    }

    private DataSource dataSource;
    private String schema;
    private String catalog;
    private boolean allTablesLoaded;
    private Map tables;

    JdbcSchema(final DataSource dataSource) {
        this.dataSource = dataSource;
        this.tables = new HashMap();
    }

    /**
     * This forces the tables to be loaded.
     *
     * @throws SQLException
     */
    public void load() throws SQLException {
        loadTables();
    }

    /**
     * For testing ONLY
     *
     * @return
    JdbcSchema copy() {
        JdbcSchema jdbcSchema = new JdbcSchema(dataSource);
        jdbcSchema.setSchemaName(getSchemaName());
        jdbcSchema.setCatalogName(getCatalogName());

        Map m = jdbcSchema.getTablesMap();
        for (Iterator it = getTables(); it.hasNext(); ) {
            Table table = (Table) it.next();
            m.put(table.getName(), table.copy());
        }

        return jdbcSchema;
    }
     */

    /**
     * For testing ONLY
    void clearUsages() {
        for (Iterator it = getTables(); it.hasNext(); ) {
            Table table = (Table) it.next();
            table.clearUsages();
        }
    }
     */

    protected void clear() {
        // keep the DataSource, clear/reset everything else
        allTablesLoaded = false;
        schema = null;
        catalog = null;
        tables.clear();
    }

    /**
     * This is used for testing allowing one to load tables and their columns
     * from more than one datasource
     */
    void resetAllTablesLoaded() {
        allTablesLoaded = false;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    protected void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    /**
     * Get the java.sql.Connection associated with this database.
     *
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    /**
     * Set the database's schema name.
     *
     * @param schema
     */
    public void setSchemaName(final String schema) {
        this.schema = schema;
    }

    /**
     * Get the database's schema name.
     *
     * @return
     */
    public String getSchemaName() {
        return schema;
    }
    /**
     * Set the database's catalog name.
     *
     * @param catalog
     */
    public void setCatalogName(final String catalog) {
        this.catalog = catalog;
    }
    /**
     * Get the database's catalog name.
     *
     * @return
     */
    public String getCatalogName() {
        return catalog;
    }

    /**
     * Get iterator over the database's tables.
     *
     * @return
     * @throws SQLException
     */
    public synchronized Iterator getTables() {
        return getTablesMap().values().iterator();
    }

    /**
     * Get a table by name.
     *
     * @param tableName
     * @return
     */
    public synchronized Table getTable(final String tableName) {
        Map tables = getTablesMap();
        Table table = (Table) tables.get(tableName);
        return table;
    }
    public String toString() {
        StringWriter sw = new StringWriter(256);
        PrintWriter pw = new PrintWriter(sw);
        print(pw, "");
        pw.flush();
        return sw.toString();
    }
    public void print(final PrintWriter pw, final String prefix) {
        pw.print(prefix);
        pw.println("JdbcSchema:");
        String subprefix = prefix + "  ";
        String subsubprefix = subprefix + "  ";

        pw.print(subprefix);
        pw.println("Tables: [");
        Iterator it = getTablesMap().values().iterator();
        while (it.hasNext()) {
            Table table = (Table) it.next();
            table.print(pw, subsubprefix);
        }
        pw.print(subprefix);
        pw.println("]");
    }

    /**
     * This method gets all of the tables (and views) in the database.
     * If called a second time, this method is a no-op.
     *
     * @throws SQLException
     */
    private void loadTables() throws SQLException {
        if (! allTablesLoaded) {
            Map tables = getTablesMap();
            Connection conn = getConnection();
            DatabaseMetaData dmd = conn.getMetaData();

            String schema = getSchemaName();
            String catalog = getCatalogName();
            String[] tableTypes = { "TABLE", "VIEW" };
            String tableName = "%";

            ResultSet rs = null;
            try {
                rs = dmd.getTables(catalog,
                                   schema,
                                   tableName,
                                   tableTypes);
                if (rs != null) {
                    while (rs.next()) {
                        addTable(rs);
                    }
                } else {
                    getLogger().debug("ERROR: rs == null");
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //ignore
            }

            allTablesLoaded = true;
        }
    }

    /**
     * Make a Table from an ResultSet - the table's name is the ResultSet third
     * entry.
     *
     * @param rs
     * @throws SQLException
     */
    protected void addTable(final ResultSet rs) throws SQLException {
        String name = rs.getString(3);
        String tableType = rs.getString(4);
        Table table = new Table(name);
        table.setTableType(tableType);

        tables.put(table.getName(), table);
    }
    private Map getTablesMap() {
        return tables;
    }
}

// End JdbcSchema.java
