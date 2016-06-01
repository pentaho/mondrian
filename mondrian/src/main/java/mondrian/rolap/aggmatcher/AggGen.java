/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * This class is used to create "lost" and "collapsed" aggregate table
 * creation sql (creates the rdbms table and inserts into it from the base
 * fact table).
 *
 * @author Richard M. Emberson
 */
public class AggGen {
    private static final Logger LOGGER = Logger.getLogger(AggGen.class);

    private final String cubeName;
    private final RolapStar star;
    private final RolapStar.Column[] columns;

    /** map RolapStar.Table to list of JdbcSchema Column Usages */
    private final Map<RolapStar.Table, List<JdbcSchema.Table.Column.Usage>>
        collapsedColumnUsages =
        new HashMap<RolapStar.Table, List<JdbcSchema.Table.Column.Usage>>();

    /** set of JdbcSchema Column Usages */
    private final Set<JdbcSchema.Table.Column.Usage> notLostColumnUsages =
        new HashSet<JdbcSchema.Table.Column.Usage>();

    /** list of JdbcSchema Column Usages */
    private final List<JdbcSchema.Table.Column.Usage> measures =
        new ArrayList<JdbcSchema.Table.Column.Usage>();

    private boolean isReady;

    public AggGen(
        String cubeName,
        RolapStar star,
        RolapStar.Column[] columns)
    {
        this.cubeName = cubeName;
        this.star = star;
        this.columns = columns;
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
        String name)
    {
        return table.getColumn(name);
    }

    protected String getRolapStarColumnName(RolapStar.Column rColumn) {
        MondrianDef.Expression expr = rColumn.getExpression();
        if (expr instanceof MondrianDef.Column) {
            MondrianDef.Column cx = (MondrianDef.Column) expr;
            return cx.getColumnName();
        }
        return null;
    }

    protected void addForeignKeyToNotLostColumnUsages(
        JdbcSchema.Table.Column column)
    {
        // first make sure its not already in
        String cname = column.getName();
        for (JdbcSchema.Table.Column.Usage usage : notLostColumnUsages) {
            JdbcSchema.Table.Column c = usage.getColumn();
            if (cname.equals(c.getName())) {
                return;
            }
        }
        JdbcSchema.Table.Column.Usage usage;
        if (column.hasUsage(JdbcSchema.UsageType.FOREIGN_KEY)) {
            Iterator<JdbcSchema.Table.Column.Usage> it =
                column.getUsages(JdbcSchema.UsageType.FOREIGN_KEY);
            it.hasNext();
            usage = it.next();
        } else {
            usage = column.newUsage(JdbcSchema.UsageType.FOREIGN_KEY);
            usage.setSymbolicName(JdbcSchema.UsageType.FOREIGN_KEY.name());
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
            getLogger().warn(
                "Init: "
                + "No fact table with name \""
                + getFactTableName()
                + "\"");
            return;
        }
        try {
            factTable.load();
        } catch (SQLException ex) {
            getLogger().error(ex);
            return;
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                "Init: "
                + "RolapStar:"
                + Util.nl
                + getFactTable()
                + Util.nl
                + "FactTable:"
                + Util.nl
                + factTable);
        }

        // do foreign keys
        for (RolapStar.Column column : columns) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(
                    "Init: "
                    + "Column: "
                    + column);
            }
            RolapStar.Table table = column.getTable();

            if (table.getParentTable() == null) {
                // this is for those crazy dimensions which are in the
                // fact table, you know, non-shared with no table element

                // How the firetruck to enter information for the
                // collapsed case. This column is in the base fact table
                // and can be part of a dimension hierarchy but no where
                // in the RolapStar is this hiearchy captured - ugg.
                if (!addSpecialCollapsedColumn(db, column)) {
                    return;
                }


                MondrianDef.Expression expr = column.getExpression();
                if (expr instanceof MondrianDef.Column) {
                    MondrianDef.Column exprColumn = (MondrianDef.Column) expr;
                    String name = exprColumn.getColumnName();
                    JdbcSchema.Table.Column c = getColumn(factTable, name);
                    if (c == null) {
                        getLogger().warn(
                            "Init: "
                            + "FactTable:"
                            + getFactTableName()
                            + Util.nl
                            + "No Column with name \""
                            + name
                            + "\"");
                        return;
                    }
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("  Jdbc Column: c=" + c);
                    }
                    addForeignKeyToNotLostColumnUsages(c);
                }

            } else {
                if (!addCollapsedColumn(db, column)) {
                    return;
                }

                while (table.getParentTable().getParentTable() != null) {
                    table = table.getParentTable();
                }
                RolapStar.Condition cond = table.getJoinCondition();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("  RolapStar.Condition: cond=" + cond);
                }
                MondrianDef.Expression left = cond.getLeft();
                if (left instanceof MondrianDef.Column) {
                    MondrianDef.Column leftColumn = (MondrianDef.Column) left;
                    String name = leftColumn.getColumnName();
                    JdbcSchema.Table.Column c = getColumn(factTable, name);
                    if (c == null) {
                        getLogger().warn(
                            "Init: "
                            + "FactTable:"
                            + getFactTableName()
                            + Util.nl
                            + "No Column with name \""
                            + name
                            + "\"");
                        return;
                    }
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("  Jdbc Column: c=" + c);
                    }
                    addForeignKeyToNotLostColumnUsages(c);
                }
            }
        }

        // do measures
        for (RolapStar.Column rColumn : getFactTable().getColumns()) {
            String name = getRolapStarColumnName(rColumn);
            if (name == null) {
                getLogger().warn(
                    "Init: "
                    + "For fact table \""
                    + getFactTableName()
                    + "\", could not get column name for RolapStar.Column: "
                    + rColumn);
                return;
            }
            if (!(rColumn instanceof RolapStar.Measure)) {
                // TODO: whats the solution to this?
                // its a funky dimension column in the fact table!!!
                getLogger().warn("not a measure: " + name);
                continue;
            }
            RolapStar.Measure rMeasure = (RolapStar.Measure) rColumn;
            if (!rMeasure.getCubeName().equals(cubeName)) {
                continue;
            }
            final RolapAggregator aggregator = rMeasure.getAggregator();
            JdbcSchema.Table.Column c = getColumn(factTable, name);
            if (c == null) {
                getLogger().warn(
                    "For RolapStar: \""
                    + getFactTable().getAlias()
                    + "\" measure with name, "
                    + name
                    + ", is not a column name. "
                    + "The measure's column name may be an expression"
                    + " and currently AggGen does not handle expressions."
                    + " You will have to add this measure to the"
                    + " aggregate table definition by hand.");
                continue;
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("  Jdbc Column m=" + c);
            }

            JdbcSchema.Table.Column.Usage usage = null;
            if (c.hasUsage(JdbcSchema.UsageType.MEASURE)) {
                for (Iterator<JdbcSchema.Table.Column.Usage> uit =
                         c.getUsages(JdbcSchema.UsageType.MEASURE);
                     uit.hasNext();)
                {
                    JdbcSchema.Table.Column.Usage tmpUsage = uit.next();
                    if ((tmpUsage.getAggregator() == aggregator)
                        && tmpUsage.getSymbolicName().equals(rColumn.getName()))
                    {
                        usage = tmpUsage;
                        break;
                    }
                }
            }
            if (usage == null) {
                usage = c.newUsage(JdbcSchema.UsageType.MEASURE);
                usage.setAggregator(aggregator);
                usage.setSymbolicName(rColumn.getName());
            }
            measures.add(usage);
        }

        // If we got to here, then everything is ok.
        isReady = true;
    }

    private boolean addSpecialCollapsedColumn(
        final JdbcSchema db,
        final RolapStar.Column rColumn)
    {
        String rname = getRolapStarColumnName(rColumn);
        if (rname == null) {
            getLogger().warn(
                "Adding Special Collapsed Column: "
                + "For fact table \""
                + getFactTableName()
                + "\", could not get column name for RolapStar.Column: "
                + rColumn);
            return false;
        }
        // this is in fact the fact table.
        RolapStar.Table rt = rColumn.getTable();

        JdbcSchema.Table jt = getTable(db, rt);
        if (jt == null) {
            getLogger().warn(
                "Adding Special Collapsed Column: "
                + "For fact table \""
                + getFactTableName()
                + "\", could not get jdbc schema table "
                + "for RolapStar.Table with alias \""
                + rt.getAlias()
                + "\"");
            return false;
        }
        try {
            jt.load();
        } catch (SQLException ex) {
            getLogger().error(ex);
            return false;
        }

        List<JdbcSchema.Table.Column.Usage> list =
            collapsedColumnUsages.get(rt);
        if (list == null) {
            list = new ArrayList<JdbcSchema.Table.Column.Usage>();
            collapsedColumnUsages.put(rt, list);
        }

        JdbcSchema.Table.Column c = getColumn(jt, rname);
        if (c == null) {
            getLogger().warn(
                "Adding Special Collapsed Column: "
                + "For fact table \""
                + getFactTableName()
                + "\", could not get jdbc schema column "
                + "for RolapStar.Table with alias \""
                + rt.getAlias()
                + "\" and column name \""
                + rname
                + "\"");
            return false;
        }
        // NOTE: this creates a new usage for the fact table
        // I do not know if this is a problem is AggGen is run before
        // Mondrian uses aggregate tables.
        list.add(c.newUsage(JdbcSchema.UsageType.FOREIGN_KEY));

        RolapStar.Column prColumn = rColumn;
        while (prColumn.getParentColumn() != null) {
            prColumn = prColumn.getParentColumn();
            rname = getRolapStarColumnName(prColumn);
            if (rname == null) {
                getLogger().warn(
                    "Adding Special Collapsed Column: "
                    + "For fact table \""
                    + getFactTableName()
                    + "\", could not get parent column name"
                    + "for RolapStar.Column \""
                    + prColumn
                    + "\" for RolapStar.Table with alias \""
                    + rt.getAlias()
                    + "\"");
                return false;
            }
            c = getColumn(jt, rname);
            if (c == null) {
                getLogger().warn("Can not find column: "  + rname);
                break;
            }
            // NOTE: this creates a new usage for the fact table
            // I do not know if this is a problem is AggGen is run before
            // Mondrian uses aggregate tables.
            list.add(c.newUsage(JdbcSchema.UsageType.FOREIGN_KEY));
        }

        return true;
    }

    private boolean addCollapsedColumn(
        final JdbcSchema db,
        final RolapStar.Column rColumn)
    {
        // TODO: if column is "id" column, then there is no collapse
        String rname = getRolapStarColumnName(rColumn);
        if (rname == null) {
            getLogger().warn(
                "Adding Collapsed Column: "
                + "For fact table \""
                + getFactTableName()
                + "\", could not get column name for RolapStar.Column: "
                + rColumn);
            return false;
        }

        RolapStar.Table rt = rColumn.getTable();

        JdbcSchema.Table jt = getTable(db, rt);
        if (jt == null) {
            getLogger().warn(
                "Adding Collapsed Column: "
                + "For fact table \"" + getFactTableName()
                + "\", could not get jdbc schema table "
                + "for RolapStar.Table with alias \"" + rt.getAlias() + "\"");
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
        List<JdbcSchema.Table.Column.Usage> list =
            new ArrayList<JdbcSchema.Table.Column.Usage>();
        for (RolapStar.Column rc : rt.getColumns()) {
            // do not include name columns
            if (rc.isNameColumn()) {
                continue;
            }
            String name = getRolapStarColumnName(rc);
            if (name == null) {
                getLogger().warn(
                    "Adding Collapsed Column: "
                    + "For fact table \""
                    + getFactTableName()
                    + "\", could not get column name"
                    + " for RolapStar.Column \""
                    + rc
                    + "\" for RolapStar.Table with alias \""
                    + rt.getAlias()
                    + "\"");
                return false;
            }
            JdbcSchema.Table.Column c = getColumn(jt, name);
            if (c == null) {
                getLogger().warn("Can not find column: " + name);
                break;
            }

            JdbcSchema.Table.Column.Usage usage =
                c.newUsage(JdbcSchema.UsageType.FOREIGN_KEY);
            usage.usagePrefix = rc.getUsagePrefix();

            list.add(usage);

            if (rname.equals(name)) {
                break;
            }
        }
        // may already be there so only enter if new list is bigger
        List<JdbcSchema.Table.Column.Usage> l = collapsedColumnUsages.get(rt);
        if ((l == null) || (l.size() < list.size())) {
            collapsedColumnUsages.put(rt, list);
        }

        return true;
    }

    private static final String AGG_LOST_PREFIX = "agg_l_XXX_";

    String makeLostAggregateTableName(String factTableName) {
        return AGG_LOST_PREFIX
               + factTableName;
    }

    private static final String AGG_COLLAPSED_PREFIX = "agg_c_XXX_";

    String makeCollapsedAggregateTableName(String factTableName) {
        return AGG_COLLAPSED_PREFIX
               + factTableName;
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

        pw.print("CREATE TABLE ");
        pw.print(makeLostAggregateTableName(getFactTableName()));
        pw.println(" (");

        // do foreign keys
        for (JdbcSchema.Table.Column.Usage usage : notLostColumnUsages) {
            addColumnCreate(pw, prefix, usage);
        }

        // do measures
        for (JdbcSchema.Table.Column.Usage usage : measures) {
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

        for (JdbcSchema.Table.Column.Usage usage : notLostColumnUsages) {
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            pw.print(c.getName());
            pw.println(',');
        }

        for (JdbcSchema.Table.Column.Usage usage : measures) {
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
        for (JdbcSchema.Table.Column.Usage usage : notLostColumnUsages) {
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            pw.print(
                sqlQuery.getDialect().quoteIdentifier(
                    factTableName,
                    c.getName()));
            pw.print(" AS ");
            pw.print(sqlQuery.getDialect().quoteIdentifier(c.getName()));
            pw.println(',');
        }
        for (JdbcSchema.Table.Column.Usage usage : measures) {
            JdbcSchema.Table.Column c = usage.getColumn();
            RolapAggregator agg = usage.getAggregator();

            pw.print(prefix);
            pw.print(
                agg.getExpression(sqlQuery.getDialect().quoteIdentifier(
                    factTableName, c.getName())));
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
        int k = 0;
        for (JdbcSchema.Table.Column.Usage notLostColumnUsage
            : notLostColumnUsages)
        {
            if (k++ > 0) {
                pw.println(",");
            }
            JdbcSchema.Table.Column.Usage usage = notLostColumnUsage;
            JdbcSchema.Table.Column c = usage.getColumn();

            pw.print(prefix);
            pw.print(
                sqlQuery.getDialect().quoteIdentifier(
                    factTableName,
                    c.getName()));
        }

        pw.println(';');
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
        for (List<JdbcSchema.Table.Column.Usage> list
            : collapsedColumnUsages.values())
        {
            for (JdbcSchema.Table.Column.Usage usage : list) {
                addColumnCreate(pw, prefix, usage);
            }
        }

        // do measures
        for (JdbcSchema.Table.Column.Usage usage : measures) {
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


        for (List<JdbcSchema.Table.Column.Usage> list
            : collapsedColumnUsages.values())
        {
            for (JdbcSchema.Table.Column.Usage usage : list) {
                JdbcSchema.Table.Column c = usage.getColumn();
                pw.print(prefix);
                if (usage.usagePrefix != null) {
                    pw.print(usage.usagePrefix);
                }
                pw.print(c.getName());
                pw.println(',');
            }
        }

        for (JdbcSchema.Table.Column.Usage usage : measures) {
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
        for (List<JdbcSchema.Table.Column.Usage> list
            : collapsedColumnUsages.values())
        {
            for (JdbcSchema.Table.Column.Usage usage : list) {
                JdbcSchema.Table.Column c = usage.getColumn();
                JdbcSchema.Table t = c.getTable();

                pw.print(prefix);
                pw.print(
                    sqlQuery.getDialect().quoteIdentifier(
                        t.getName(),
                        c.getName()));
                pw.print(" AS ");
                String n = (usage.usagePrefix == null)
                    ? c.getName() : usage.usagePrefix + c.getName();
                pw.print(sqlQuery.getDialect().quoteIdentifier(n));
                pw.println(',');
            }
        }
        for (JdbcSchema.Table.Column.Usage usage : measures) {
            JdbcSchema.Table.Column c = usage.getColumn();
            JdbcSchema.Table t = c.getTable();
            RolapAggregator agg = usage.getAggregator();

            pw.print(prefix);
            pw.print(
                agg.getExpression(sqlQuery.getDialect().quoteIdentifier(
                    t.getName(), c.getName())));
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
        int k = 0;
        for (RolapStar.Table rt : collapsedColumnUsages.keySet()) {
            if (k++ > 0) {
                pw.println(',');
            }
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
                    pw.print(
                        sqlQuery.getDialect().quoteIdentifier(rt.getAlias()));
                    pw.print(" AS ");
                    pw.print(
                        sqlQuery.getDialect().quoteIdentifier(rt.getAlias()));
                }
            }
        }

        pw.println();
        pw.println("WHERE ");
        k = 0;
        for (RolapStar.Table rt : collapsedColumnUsages.keySet()) {
            if (k++ > 0) {
                pw.println(" and");
            }

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
        }

        pw.println();
        pw.println("GROUP BY ");
        k = 0;
        for (List<JdbcSchema.Table.Column.Usage> list
            : collapsedColumnUsages.values())
        {
            for (JdbcSchema.Table.Column.Usage usage : list) {
                if (k++ > 0) {
                    pw.println(",");
                }
                JdbcSchema.Table.Column c = usage.getColumn();
                JdbcSchema.Table t = c.getTable();

                String n = (usage.usagePrefix == null)
                    ? c.getName() : usage.usagePrefix + c.getName();
                pw.print(prefix);
                pw.print(sqlQuery.getDialect().quoteIdentifier(t.getName(), n));
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
        if (usage.getUsageType() == JdbcSchema.UsageType.MEASURE) {
            if (c.hasUsage(JdbcSchema.UsageType.FOREIGN_KEY)) {
                name = usage.getSymbolicName().replace(' ', '_').toUpperCase();
            }
        }
        return name;
    }

    private void addColumnCreate(
        final PrintWriter pw,
        final String prefix,
        final JdbcSchema.Table.Column.Usage usage)
    {
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
        case Types.NUMERIC:
        case Types.DECIMAL:
            pw.print('(');
            pw.print(c.getNumPrecRadix());
            pw.print(",");
            pw.print(c.getDecimalDigits());
            pw.print(')');
            break;
        case Types.CHAR:
        case Types.VARCHAR:
            pw.print('(');
            pw.print(c.getCharOctetLength());
            pw.print(')');
            break;
        default:
        }
        if (! c.isNullable()) {
            pw.print(" NOT NULL");
        }
        pw.println(',');
    }
}

// End AggGen.java
