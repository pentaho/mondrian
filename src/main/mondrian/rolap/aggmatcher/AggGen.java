/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.RolapAggregator;
import org.apache.log4j.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.sql.SQLException;
import java.sql.Types;

/**
 * This class is used to create "lost" and "collapsed" aggregate table
 * creation sql (creates the rdbms table and inserts into it from the base
 * fact table).
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
public class AggGen {
    private static final Logger LOGGER = Logger.getLogger(AggGen.class);

    private final RolapStar star;
    private final RolapStar.Column[] columns;

    /** map RolapStar.Table to list of JdbcSchema Column Usages */
    private final Map collapsedColumnUsages;

    /** set of JdbcSchema Column Usages */
    private final Set notLostColumnUsages;

    /** list of JdbcSchema Column Usages */
    private final List measures;
    private boolean isReady;

    public AggGen(RolapStar star, RolapStar.Column[] columns) {
        this.star = star;
        this.columns = columns;
        this.notLostColumnUsages = new HashSet();
        this.collapsedColumnUsages = new HashMap();
        this.measures = new ArrayList();
        init();
    }
    private Logger getLogger() {
        return LOGGER;
    }

    /**
     * Return true if this instance is ready to generate the sql. If false,
     * then something went wrong as it was trying to understand the columns.
     */
    public boolean isReady() {
        return isReady;
    }

    protected RolapStar.Table getFactTable() {
        return star.getFactTable();
    }

    protected String getFactTableName() {
        return getFactTable().getAlias();
    }

    protected SqlQuery getSqlQuery() {
        return star.getSqlQuery();
    }

    protected String getFactCount() {
        return "fact_count";
    }

    protected JdbcSchema.Table getTable(JdbcSchema db, RolapStar.Table rt) {
        JdbcSchema.Table jt = getTable(db, rt.getAlias());
        return (jt == null)
            ? getTable(db, rt.getTableName())
            : jt;
    }

    protected JdbcSchema.Table getTable(JdbcSchema db, String name) {
        return db.getTable(name);
    }

    protected JdbcSchema.Table.Column getColumn(
            JdbcSchema.Table table,
            String name) {
        return table.getColumn(name);
    }

    protected String getRolapStarColumnName(RolapStar.Column rColumn) {
        MondrianDef.Expression expr = rColumn.getExpression();
        if (expr instanceof MondrianDef.Column) {
            MondrianDef.Column cx = (MondrianDef.Column) expr;
            String name = cx.getColumnName();
            return name;
        }
        return null;
    }
    protected void addForeignKeyToNotLostColumnUsages(
            JdbcSchema.Table.Column column) {

        // first make sure its not already in
        String cname = column.getName();
        for (Iterator it = notLostColumnUsages.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();
            if (cname.equals(c.getName())) {
                return;
            }
        }
        JdbcSchema.Table.Column.Usage usage = null;
        if (column.hasUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE)) {
            Iterator it = column.getUsages(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE);
            it.hasNext();
            usage = (JdbcSchema.Table.Column.Usage) it.next();
        } else {
            usage = column.newUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE);
            usage.setSymbolicName(JdbcSchema.FOREIGN_KEY_COLUMN_NAME);
        }
        notLostColumnUsages.add(usage);
    }

    /**
     * The columns are the RolapStar columns taking part in an aggregation
     * request. This is what happens.
     * First, for each column, walk up the column's table until one level below
     * the base fact table. The left join condition contains the base fact table
     * and the foreign key column name. This column should not be lost.
     * Get the base fact table's measure columns.
     * With a list of columns that should not be lost and measure, one can
     * create lost create and insert commands.
     */
    private void init() {
        JdbcSchema db = JdbcSchema.makeDB(star.getDataSource());
        try {
            db.load();
        } catch (SQLException ex) {
            getLogger().error(ex);
            return;
        }

        JdbcSchema.Table factTable = getTable(db, getFactTableName());
        if (factTable == null) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Init: ");
            buf.append("No fact table with name \"");
            buf.append(getFactTableName());
            buf.append("\"");
            getLogger().warn(buf.toString());
            return;
        }
        try {
            factTable.load();
        } catch (SQLException ex) {
            getLogger().error(ex);
            return;
        }

        if (getLogger().isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(512);
            buf.append("Init: ");
            buf.append("RolapStar:");
            buf.append(Util.nl);
            buf.append(getFactTable());
            buf.append(Util.nl);
            buf.append("FactTable:");
            buf.append(Util.nl);
            buf.append(factTable);
            getLogger().debug(buf.toString());
        }

        // do foreign keys
        for (int i = 0; i < columns.length; i++) {
            RolapStar.Column column = columns[i];
            if (getLogger().isDebugEnabled()) {
                StringBuffer buf = new StringBuffer(64);
                buf.append("Init: ");
                buf.append("Column: ");
                buf.append(column);
                getLogger().debug(buf.toString());
            }
            RolapStar.Table table = column.getTable();

            if (table.getParentTable() == null) {
                // this is for those crazy dimensions which are in the
                // fact table, you know, non-shared with no table element

                // How the firetruck to enter information for the
                // collapsed case. This column is in the base fact table
                // and can be part of a dimension hierarchy but no where
                // in the RolapStar is this hiearchy captured - ugg.
                if (! addSpecialCollapsedColumn(db, column)) {
                    return;
                }


                MondrianDef.Expression expr = column.getExpression();
                if (expr instanceof MondrianDef.Column) {
                    MondrianDef.Column exprColumn = (MondrianDef.Column) expr;
                    String name = exprColumn.getColumnName();
                    JdbcSchema.Table.Column c = getColumn(factTable, name);
                    if (c == null) {
                        StringBuffer buf = new StringBuffer(64);
                        buf.append("Init: ");
                        buf.append("FactTable:");
                        buf.append(getFactTableName());
                        buf.append(Util.nl);
                        buf.append("No Column with name \"");
                        buf.append(name);
                        buf.append("\"");
                        getLogger().warn(buf.toString());
                        return;
                    }
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("  Jdbc Column: c="+c);
                    }
                    addForeignKeyToNotLostColumnUsages(c);
                }

            } else {

                if (! addCollapsedColumn(db, column)) {
                    return;
                }

                while (table.getParentTable().getParentTable() != null) {
                    table = table.getParentTable();
                }
                RolapStar.Condition cond = table.getJoinCondition();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("  RolapStar.Condition: cond="+cond);
                }
                MondrianDef.Expression left = cond.getLeft();
                if (left instanceof MondrianDef.Column) {
                    MondrianDef.Column leftColumn = (MondrianDef.Column) left;
                    String name = leftColumn.getColumnName();
                    JdbcSchema.Table.Column c = getColumn(factTable, name);
                    if (c == null) {
                        StringBuffer buf = new StringBuffer(64);
                        buf.append("Init: ");
                        buf.append("FactTable:");
                        buf.append(getFactTableName());
                        buf.append(Util.nl);
                        buf.append("No Column with name \"");
                        buf.append(name);
                        buf.append("\"");
                        getLogger().warn(buf.toString());
                        return;
                    }
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("  Jdbc Column: c="+c);
                    }
                    addForeignKeyToNotLostColumnUsages(c);
                }
            }
        }

        // do measures
        for (Iterator it = getFactTable().getColumns().iterator(); it.hasNext(); ) {
            RolapStar.Column rColumn = (RolapStar.Column) it.next();
            String name = getRolapStarColumnName(rColumn);
            if (name == null) {
                StringBuffer buf = new StringBuffer(64);
                buf.append("Init: ");
                buf.append("For fact table \"");
                buf.append(getFactTableName());
                buf.append("\", could not get column name for RolapStar.Column: ");
                buf.append(rColumn);
                getLogger().warn(buf.toString());
                return;
            }
            RolapAggregator aggregator = null;
            String symbolicName = null;
            if (! (rColumn instanceof RolapStar.Measure)) {
                // TODO: whats the solution to this?
                // its a funky dimension column in the fact table!!!
                getLogger().warn("not a measure: " +name);
                continue;
            } else {
                RolapStar.Measure rMeasure = (RolapStar.Measure) rColumn;
                aggregator = rMeasure.getAggregator();
            }
            JdbcSchema.Table.Column c = getColumn(factTable, name);
            if (c == null) {
                StringBuffer buf = new StringBuffer(64);
                buf.append("For RolapStar: \"");
                buf.append(getFactTable().getAlias());
                buf.append("\" measure with name, ");
                buf.append(name);
                buf.append(", is not a column name. ");
                buf.append("The measure's column name may be an expression");
                buf.append(" and currently AggGen does not handle expressions.");
                buf.append(" You will have to add this measure to the");
                buf.append(" aggregate table definition by hand.");
                getLogger().warn(buf.toString());
                continue;
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("  Jdbc Column m="+c);
            }
/*
            JdbcSchema.Table.Column.Usage usage =
                c.newUsage(JdbcSchema.MEASURE_COLUMN_USAGE);
            usage.setAggregator(aggregator);
            usage.setSymbolicName(rColumn.getName());
            measures.add(usage);
*/

            JdbcSchema.Table.Column.Usage usage = null;
            if (c.hasUsage(JdbcSchema.MEASURE_COLUMN_USAGE)) {
                for (Iterator uit =
                    c.getUsages(JdbcSchema.MEASURE_COLUMN_USAGE);
                    uit.hasNext(); ) {

                    JdbcSchema.Table.Column.Usage tmpUsage =
                        (JdbcSchema.Table.Column.Usage) uit.next();
                    if ((tmpUsage.getAggregator() == aggregator) &&
                        tmpUsage.getSymbolicName().equals(rColumn.getName())) {
                        usage = tmpUsage;
                        break;
                    }
                }
            }
            if (usage == null) {
                usage = c.newUsage(JdbcSchema.MEASURE_COLUMN_USAGE);
                usage.setAggregator(aggregator);
                usage.setSymbolicName(rColumn.getName());
            }
            measures.add(usage);
        }

        // If we got to here, then everything is ok.
        isReady = true;
    }
    private boolean addSpecialCollapsedColumn(final JdbcSchema db,
                                              final RolapStar.Column rColumn) {
        String rname = getRolapStarColumnName(rColumn);
        if (rname == null) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Adding Special Collapsed Column: ");
            buf.append("For fact table \"");
            buf.append(getFactTableName());
            buf.append("\", could not get column name for RolapStar.Column: ");
            buf.append(rColumn);
            getLogger().warn(buf.toString());
            return false;
        }
        // this is in fact the fact table.
        RolapStar.Table rt = rColumn.getTable();

        JdbcSchema.Table jt = getTable(db, rt);
        if (jt == null) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Adding Special Collapsed Column: ");
            buf.append("For fact table \"");
            buf.append(getFactTableName());
            buf.append("\", could not get jdbc schema table ");
            buf.append("for RolapStar.Table with alias \"");
            buf.append(rt.getAlias());
            buf.append("\"");
            getLogger().warn(buf.toString());
            return false;
        }
        try {
            jt.load();
        } catch (SQLException ex) {
            getLogger().error(ex);
            return false;
        }

        List list = (List) collapsedColumnUsages.get(rt);
        if (list == null) {
            list = new ArrayList();
            collapsedColumnUsages.put(rt, list);
        }

        JdbcSchema.Table.Column c = getColumn(jt, rname);
        if (c == null) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Adding Special Collapsed Column: ");
            buf.append("For fact table \"");
            buf.append(getFactTableName());
            buf.append("\", could not get jdbc schema column ");
            buf.append("for RolapStar.Table with alias \"");
            buf.append(rt.getAlias());
            buf.append("\" and column name \"");
            buf.append(rname);
            buf.append("\"");
            getLogger().warn(buf.toString());
            return false;
        }
        // NOTE: this creates a new usage for the fact table
        // I do not know if this is a problem is AggGen is run before
        // Mondrian uses aggregate tables.
        list.add(c.newUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE));

        RolapStar.Column prColumn = rColumn;
        while (prColumn.getParentColumn() != null) {
            prColumn = prColumn.getParentColumn();
            rname = getRolapStarColumnName(prColumn);
            if (rname == null) {
                StringBuffer buf = new StringBuffer(64);
                buf.append("Adding Special Collapsed Column: ");
                buf.append("For fact table \"");
                buf.append(getFactTableName());
                buf.append("\", could not get parent column name");
                buf.append("for RolapStar.Column \"");
                buf.append(rname);
                buf.append("\" for RolapStar.Table with alias \"");
                buf.append(rt.getAlias());
                buf.append("\"");
                getLogger().warn(buf.toString());
                return false;
            }
            c = getColumn(jt, rname);
            if (c == null) {
                getLogger().warn("Can not find column: " +rname);
                break;
            }
            // NOTE: this creates a new usage for the fact table
            // I do not know if this is a problem is AggGen is run before
            // Mondrian uses aggregate tables.
            list.add(c.newUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE));
        }

        return true;
    }

    private boolean addCollapsedColumn(final JdbcSchema db,
                                       final RolapStar.Column rColumn) {
        // TODO: if column is "id" column, then there is no collapse
        String rname = getRolapStarColumnName(rColumn);
        if (rname == null) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Adding Collapsed Column: ");
            buf.append("For fact table \"");
            buf.append(getFactTableName());
            buf.append("\", could not get column name for RolapStar.Column: ");
            buf.append(rColumn);
            getLogger().warn(buf.toString());
            return false;
        }

        RolapStar.Table rt = rColumn.getTable();

        JdbcSchema.Table jt = getTable(db, rt);
        if (jt == null) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Adding Collapsed Column: ");
            buf.append("For fact table \"");
            buf.append(getFactTableName());
            buf.append("\", could not get jdbc schema table ");
            buf.append("for RolapStar.Table with alias \"");
            buf.append(rt.getAlias());
            buf.append("\"");
            getLogger().warn(buf.toString());
            return false;
        }
        try {
            jt.load();
        } catch (SQLException ex) {
            getLogger().error(ex);
            return false;
        }

        //CG guarantee the columns has been loaded before looking up them
        try {
            jt.load();
        } catch (SQLException sqle) {
            getLogger().error(sqle);
            return false;
        }

        // if this is a dimension table, then walk down the levels until
        // we hit the current column
        List list = new ArrayList();
        for (Iterator it = rt.getColumns().iterator(); it.hasNext(); ) {
            RolapStar.Column rc = (RolapStar.Column) it.next();
            // do not include name columns
            if (rc.isNameColumn()) {
                continue;
            }
            String name = getRolapStarColumnName(rc);
            if (name == null) {
                StringBuffer buf = new StringBuffer(64);
                buf.append("Adding Collapsed Column: ");
                buf.append("For fact table \"");
                buf.append(getFactTableName());
                buf.append("\", could not get column name");
                buf.append(" for RolapStar.Column \"");
                buf.append(rc);
                buf.append("\" for RolapStar.Table with alias \"");
                buf.append(rt.getAlias());
                buf.append("\"");
                getLogger().warn(buf.toString());
                return false;
            }
            JdbcSchema.Table.Column c = getColumn(jt, name);
            if (c == null) {
                getLogger().warn("Can not find column: " +name);
                break;
            }

            JdbcSchema.Table.Column.Usage usage =
                c.newUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE);
            usage.usagePrefix = rc.getUsagePrefix();

            list.add(usage);

            if (rname.equals(name)) {
                break;
            }
        }
        // may already be there so only enter if new list is bigger
        List l = (List) collapsedColumnUsages.get(rt);
        if ((l == null) || (l.size() < list.size())) {
            collapsedColumnUsages.put(rt, list);
        }

        return true;
    }

    private static final String AGG_LOST_PREFIX = "agg_l_XXX_";

    String makeLostAggregateTableName(String factTableName) {
        StringBuffer buf = new StringBuffer(64);
        buf.append(AGG_LOST_PREFIX);
        buf.append(factTableName);
        return buf.toString();
    }

    private static final String AGG_COLLAPSED_PREFIX = "agg_c_XXX_";

    String makeCollapsedAggregateTableName(String factTableName) {
        StringBuffer buf = new StringBuffer(64);
        buf.append(AGG_COLLAPSED_PREFIX);
        buf.append(factTableName);
        return buf.toString();
    }



    /**
     * Return a String containing the sql code to create a lost dimension
     * table.
     *
     * @return lost dimension sql code
     */
    public String createLost() {
        StringWriter sw = new StringWriter(512);
        PrintWriter pw = new PrintWriter(sw);
        String prefix = "    ";
        String factTableName = getFactTableName();

        pw.print("CREATE TABLE ");
        pw.print(makeLostAggregateTableName(getFactTableName()));
        pw.println(" (");

        // do foreign keys
        for (Iterator it = notLostColumnUsages.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            addColumnCreate(pw, prefix, usage);
        }

        // do measures
        for (Iterator it = measures.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            addColumnCreate(pw, prefix, usage);
        }
        // do fact_count
        pw.print(prefix);
        pw.print(getFactCount());
        pw.println(" INTEGER NOT NULL");

        pw.println(");");
        return sw.toString();
    }

    /**
     * Return the sql code to populate a lost dimension table from the fact
     * table.
     */
    public String insertIntoLost() {
        StringWriter sw = new StringWriter(512);
        PrintWriter pw = new PrintWriter(sw);
        String prefix = "    ";
        String factTableName = getFactTableName();
        SqlQuery sqlQuery = getSqlQuery();

        pw.print("INSERT INTO ");
        pw.print(makeLostAggregateTableName(getFactTableName()));
        pw.println(" (");

        for (Iterator it = notLostColumnUsages.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            pw.print(c.getName());
            pw.println(',');
        }

        for (Iterator it = measures.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            String name = getUsageName(usage);
            pw.print(name);
            pw.println(',');
        }
        // do fact_count
        pw.print(prefix);
        pw.print(getFactCount());
        pw.println(")");

        pw.println("SELECT");
        for (Iterator it = notLostColumnUsages.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            pw.print(sqlQuery.getDialect().quoteIdentifier(factTableName, c.getName()));
            pw.print(" AS ");
            pw.print(sqlQuery.getDialect().quoteIdentifier(c.getName()));
            pw.println(',');
        }
        for (Iterator it = measures.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();
            RolapAggregator agg = usage.getAggregator();

            pw.print(prefix);
            pw.print(
                agg.getExpression(sqlQuery.getDialect().quoteIdentifier(
                    factTableName, c.getName())).toUpperCase());
            pw.print(" AS ");
            pw.print(sqlQuery.getDialect().quoteIdentifier(c.getName()));
            pw.println(',');
        }

        // do fact_count
        pw.print(prefix);
        pw.print("COUNT(*) AS ");
        pw.println(sqlQuery.getDialect().quoteIdentifier(getFactCount()));

        pw.println("FROM ");
        pw.print(prefix);
        pw.print(sqlQuery.getDialect().quoteIdentifier(factTableName));
        pw.print(" ");
        pw.println(sqlQuery.getDialect().quoteIdentifier(factTableName));

        pw.println("GROUP BY ");
        for (Iterator it = notLostColumnUsages.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            pw.print(sqlQuery.getDialect().quoteIdentifier(factTableName, c.getName()));
            if (it.hasNext()) {
                pw.println(',');
            } else {
                pw.println(';');
            }
        }

        return sw.toString();
    }
    /**
     * Return a String containing the sql code to create a collapsed dimension
     * table.
     *
     * @return collapsed dimension sql code
     */
    public String createCollapsed() {
        StringWriter sw = new StringWriter(512);
        PrintWriter pw = new PrintWriter(sw);
        String prefix = "    ";

        pw.print("CREATE TABLE ");
        pw.print(makeCollapsedAggregateTableName(getFactTableName()));
        pw.println(" (");

        // do foreign keys
        for (Iterator it = collapsedColumnUsages.values().iterator();
                    it.hasNext(); ) {
            List list = (List) it.next();
            for (Iterator lit = list.iterator(); lit.hasNext(); ) {
                JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) lit.next();
                addColumnCreate(pw, prefix, usage);
            }
        }

        // do measures
        for (Iterator it = measures.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();

            addColumnCreate(pw, prefix, usage);
        }
        // do fact_count
        pw.print(prefix);
        pw.print(getFactCount());
        pw.println(" INTEGER NOT NULL");

        pw.println(");");
        return sw.toString();
    }

    /**
     * Return the sql code to populate a collapsed dimension table from
     * the fact table.
     */
    public String insertIntoCollapsed() {
        StringWriter sw = new StringWriter(512);
        PrintWriter pw = new PrintWriter(sw);
        String prefix = "    ";
        String factTableName = getFactTableName();
        SqlQuery sqlQuery = getSqlQuery();

        pw.print("INSERT INTO ");
        pw.print(makeCollapsedAggregateTableName(getFactTableName()));
        pw.println(" (");


        for (Iterator it = collapsedColumnUsages.values().iterator();
                    it.hasNext(); ) {
            List list = (List) it.next();
            for (Iterator lit = list.iterator(); lit.hasNext(); ) {
                JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) lit.next();
                JdbcSchema.Table.Column c = usage.getColumn();
                pw.print(prefix);
                if (usage.usagePrefix != null) {
                    pw.print(usage.usagePrefix);
                }
                pw.print(c.getName());
                pw.println(',');
            }
        }

        for (Iterator it = measures.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            String name = getUsageName(usage);
            pw.print(name);
            //pw.print(c.getName());
            pw.println(',');
        }
        // do fact_count
        pw.print(prefix);
        pw.print(getFactCount());
        pw.println(")");

        pw.println("SELECT");
        int dimCnt = 0;
        for (Iterator it = collapsedColumnUsages.values().iterator();
                    it.hasNext(); ) {
            List list = (List) it.next();
            for (Iterator lit = list.iterator(); lit.hasNext(); ) {
                JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) lit.next();
                JdbcSchema.Table.Column c = usage.getColumn();
                JdbcSchema.Table t = c.getTable();

                pw.print(prefix);
                pw.print(sqlQuery.getDialect().quoteIdentifier(t.getName(), c.getName()));
                pw.print(" AS ");
                String n = (usage.usagePrefix == null)
                    ? c.getName() : usage.usagePrefix + c.getName();
                pw.print(sqlQuery.getDialect().quoteIdentifier(n));
                pw.println(',');
            }
        }
        for (Iterator it = measures.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            JdbcSchema.Table.Column c = usage.getColumn();
            JdbcSchema.Table t = c.getTable();
            RolapAggregator agg = usage.getAggregator();

            pw.print(prefix);
            pw.print(
                agg.getExpression(sqlQuery.getDialect().quoteIdentifier(
                    t.getName(), c.getName())).toUpperCase());
            pw.print(" AS ");
            pw.print(sqlQuery.getDialect().quoteIdentifier(c.getName()));
            pw.println(',');
        }

        // do fact_count
        pw.print(prefix);
        pw.print("COUNT(*) AS ");
        pw.println(sqlQuery.getDialect().quoteIdentifier(getFactCount()));

        pw.println("FROM ");
        pw.print(prefix);
        pw.print(sqlQuery.getDialect().quoteIdentifier(factTableName));
        pw.print(" ");
        pw.print(sqlQuery.getDialect().quoteIdentifier(factTableName));
        pw.println(',');

        // add dimension tables
        dimCnt = 0;
        for (Iterator it = collapsedColumnUsages.keySet().iterator();
                    it.hasNext(); ) {
            RolapStar.Table rt = (RolapStar.Table) it.next();

            pw.print(prefix);
            pw.print(sqlQuery.getDialect().quoteIdentifier(rt.getAlias()));
            pw.print(" AS ");
            pw.print(sqlQuery.getDialect().quoteIdentifier(rt.getAlias()));

            // walk up tables
            if (rt.getParentTable() != null) {
                while (rt.getParentTable().getParentTable() != null) {
                    rt = rt.getParentTable();

                    pw.println(',');

                    pw.print(prefix);
                    pw.print(sqlQuery.getDialect().quoteIdentifier(rt.getAlias()));
                    pw.print(" AS ");
                    pw.print(sqlQuery.getDialect().quoteIdentifier(rt.getAlias()));
                }
            }

            if (it.hasNext()) {
                pw.println(',');
            } else {
                pw.println();
            }
        }

        pw.println("WHERE ");
        for (Iterator it = collapsedColumnUsages.keySet().iterator();
                    it.hasNext(); ) {
            RolapStar.Table rt = (RolapStar.Table) it.next();

            RolapStar.Condition cond = rt.getJoinCondition();
            if (cond == null) {
                continue;
            }
            pw.print(prefix);
            pw.print(cond.toString(sqlQuery));

            if (rt.getParentTable() != null) {
                while (rt.getParentTable().getParentTable() != null) {
                    rt = rt.getParentTable();
                    cond = rt.getJoinCondition();

                    pw.println(" and");

                    pw.print(prefix);
                    pw.print(cond.toString(sqlQuery));
                }
            }

            if (it.hasNext()) {
                pw.println(" and");
            } else {
                pw.println();
            }
        }

        pw.println("GROUP BY ");
        dimCnt = 0;
        for (Iterator it = collapsedColumnUsages.values().iterator();
                    it.hasNext(); ) {
            List list = (List) it.next();
            for (Iterator lit = list.iterator(); lit.hasNext(); ) {
                JdbcSchema.Table.Column.Usage usage =
                    (JdbcSchema.Table.Column.Usage) lit.next();
                JdbcSchema.Table.Column c = usage.getColumn();
                JdbcSchema.Table t = c.getTable();

                String n = (usage.usagePrefix == null)
                    ? c.getName() : usage.usagePrefix + c.getName();
                pw.print(prefix);
                pw.print(sqlQuery.getDialect().quoteIdentifier(t.getName(), n));

                if (it.hasNext()) {
                    pw.println(',');
                }
            }
        }
        pw.println(';');

        return sw.toString();
    }



    private String getUsageName(final JdbcSchema.Table.Column.Usage usage) {
        JdbcSchema.Table.Column c = usage.getColumn();
        String name = c.getName();
        // if its a measure which is based upon a foreign key, then
        // the foreign key column name is already used (for the foreign key
        // column) so we must choose a different name.
        if (usage.getColumnType() == JdbcSchema.MEASURE_COLUMN_USAGE) {
            if (c.hasUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE)) {
                name = usage.getSymbolicName().replace(' ', '_').toUpperCase();
            }
        }
        return name;
    }

    private void addColumnCreate(final PrintWriter pw,
                                 final String prefix,
                                 final JdbcSchema.Table.Column.Usage usage) {
        JdbcSchema.Table.Column c = usage.getColumn();
        String name = getUsageName(usage);

        pw.print(prefix);
        if (usage.usagePrefix != null) {
            pw.print(usage.usagePrefix);
        }
        pw.print(name);
        pw.print(' ');
        pw.print(c.getTypeName().toUpperCase());
        switch (c.getType()) {
        case Types.NUMERIC :
        case Types.DECIMAL :
            pw.print('(');
            pw.print(c.getNumPrecRadix());
            pw.print(",");
            pw.print(c.getDecimalDigits());
            pw.print(')');
            break;
        case Types.CHAR :
        case Types.VARCHAR :
            pw.print('(');
            pw.print(c.getCharOctetLength());
            pw.print(')');
            break;
        default :
        }
        if (! c.isNullable()) {
            pw.print(" NOT NULL");
        }
        pw.println(',');

    }
}

// AggGen.java
