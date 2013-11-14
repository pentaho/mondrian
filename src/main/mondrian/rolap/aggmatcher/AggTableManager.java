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
import mondrian.recorder.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.spi.*;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;

/**
 * Manages aggregate tables.
 *
 * <p>It is used as follows:<ul>
 * <li>A {@link mondrian.rolap.RolapSchema} creates an {@link AggTableManager},
 *     and stores it in a member variable to ensure that it is not
 *     garbage-collected.
 * <li>The {@link AggTableManager} creates and registers
 *     {@link org.eigenbase.util.property.Trigger} objects, so that it is notified
 *     when properties pertinent to aggregate tables change.
 * <li>The {@link mondrian.rolap.RolapSchema} calls {@link #initialize()},
 *     which scans the JDBC catalog and identifies aggregate tables.
 * <li>For each aggregate table, it creates an {@link AggStar} and calls
 *     {@link RolapStar#addAggStar(AggStar)}.
 *
 * @author Richard M. Emberson
 */
public class AggTableManager {
    private static final Logger LOGGER =
        Logger.getLogger(AggTableManager.class);

    private final RolapSchema schema;

    private static final MondrianResource mres = MondrianResource.instance();

    public AggTableManager(final RolapSchema schema) {
        this.schema = schema;
    }

    /**
     * This should ONLY be called if the AggTableManager is no longer going
     * to be used. In fact, it should only be called indirectly by its
     * associated RolapSchema object.
     */
    public void finalCleanUp() {
        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                "AggTableManager.finalCleanUp: schema="
                + schema.getName());
        }
    }

    /**
     * Get the Logger.
     */
    public Logger getLogger() {
        return LOGGER;
    }

    /**
     * Initializes this object, loading all aggregate tables and associating
     * them with {@link RolapStar}s.
     * This method should only be called once.
     */
    public void initialize() {
        if (Util.deprecated(false, false)
            && MondrianProperties.instance().ReadAggregates.get())
        {
            try {
                loadRolapStarAggregates();
            } catch (SQLException ex) {
                throw mres.AggLoadingError.ex(ex);
            }
        }
        printResults();
    }

    private void printResults() {
        if (getLogger().isDebugEnabled()) {
            // print everything, Star, subTables, AggStar and subTables
            // could be a lot
            StringBuilder buf = new StringBuilder(4096);
            buf.append(Util.nl);
            for (RolapStar star : getStars()) {
                buf.append(star.toString());
                buf.append(Util.nl);
            }
            getLogger().debug(buf.toString());
        }
    }

    private void reLoadRolapStarAggregates() {
        if (Util.deprecated(false, false)
            && MondrianProperties.instance().ReadAggregates.get())
        {
            try {
                clearJdbcSchema();
                loadRolapStarAggregates();
                printResults();
            } catch (SQLException ex) {
                throw mres.AggLoadingError.ex(ex);
            }
        }
    }

    private JdbcSchema getJdbcSchema() {
        DataSource dataSource = schema.getInternalConnection().getDataSource();

        DataServicesProvider provider =
            DataServicesLocator.getDataServicesProvider(
                schema.getDataServiceProviderName());
        // This actually just does a lookup or simple constructor invocation,
        // its not expected to fail
        return JdbcSchema.makeDB(dataSource, provider.getJdbcSchemaFactory());
    }

    /**
     * Clear the possibly already loaded snapshot of what is in the database.
     */
    private void clearJdbcSchema() {
        DataSource dataSource = schema.getInternalConnection().getDataSource();
        JdbcSchema.clearDB(dataSource);
    }


    /**
     * This method loads and/or reloads the aggregate tables.
     * <p>
     * NOTE: At this point all RolapStars have been made for this
     * schema (except for dynamically added cubes which I am going
     * to ignore for right now). So, All stars have their columns
     * and their BitKeys can be generated.
     *
     * @throws SQLException on error
     */
    private void loadRolapStarAggregates() throws SQLException {
        Util.deprecated("never called", true);
        ListRecorder msgRecorder = new ListRecorder();
        try {
            DefaultRules rules = DefaultRules.getInstance();
            JdbcSchema db = getJdbcSchema();
            // loads tables, not their columns
            db.load();

            for (RolapStar star : getStars()) {
                // This removes any AggStars from any previous invocation of
                // this method (if any)
                star.prepareToLoadAggregates();

                List<ExplicitRules.Group> aggGroups = getAggGroups(star);
                for (ExplicitRules.Group group : aggGroups) {
                    group.validate(msgRecorder);
                }

                String factTableName = star.getFactTable().getAlias();

                JdbcSchema.Table dbFactTable = db.getTable(factTableName);
                if (dbFactTable == null) {
                    msgRecorder.reportWarning(
                        "No Table found for fact name=" + factTableName);
                    continue;
                }

                // For each column in the dbFactTable, figure out it they are
                // measure or foreign key columns
                bindToStar(dbFactTable, star, msgRecorder);
                String schemaName = dbFactTable.table.getSchemaName();

                // Now look at all tables in the database and per table, first
                // see if it is a match for an aggregate table for this fact
                // table and second see if its columns match foreign key and
                // level columns.
                for (JdbcSchema.Table dbTable : db.getTables()) {
                    String name = dbTable.getName();

                    // Do the catalog schema aggregate excludes, exclude this
                    // table name.
                    if (ExplicitRules.excludeTable(name, aggGroups)) {
                        continue;
                    }

                    // First see if there is an ExplicitRules match. If so, then
                    // if all of the columns match up, then make an AggStar.
                    // On the other hand, if there is no ExplicitRules match,
                    // see if there is a Default match. If so and if all the
                    // columns match up, then also make an AggStar.
                    ExplicitRules.TableDef tableDef =
                        ExplicitRules.getIncludeByTableDef(name, aggGroups);

                    boolean makeAggStar = false;
                    int approxRowCount = Integer.MIN_VALUE;
                    // Is it handled by the ExplicitRules
                    if (tableDef != null) {
                        // load columns
                        dbTable.load();
                        makeAggStar =
                            tableDef.columnsOK(
                                star,
                                dbFactTable,
                                dbTable,
                                msgRecorder);
                            approxRowCount = tableDef.getApproxRowCount();
                    }
                    if (! makeAggStar) {
                        // Is it handled by the DefaultRules
                        if (rules.matchesTableName(factTableName, name)) {
                            // load columns
                            dbTable.load();
                            makeAggStar =
                                rules.columnsOK(
                                    star,
                                    dbFactTable,
                                    dbTable,
                                    msgRecorder);
                        }
                    }


                    if (makeAggStar) {
                        dbTable.setTableUsageType(
                            JdbcSchema.TableUsageType.AGG);
                        dbTable.table =
                            new RolapSchema.PhysTable(
                                schema.getPhysicalSchema(),
                                schemaName,
                                name,
                                name, // REVIEW: alias used to be null
                                null); // don't know about table hints
                        AggStar aggStar =
                            AggStar.makeAggStar(
                                star,
                                dbTable,
                                msgRecorder,
                                approxRowCount);
                        if (aggStar.getCost() > 0) {
                            star.addAggStar(aggStar);
                        } else {
                            getLogger().warn(
                                mres.AggTableZeroSize.str(
                                    aggStar.getAggFactTable().getName(),
                                    factTableName));
                        }
                    }
                    // Note: if the dbTable name matches but the columnsOK does
                    // not, then this is an error and the aggregate tables
                    // can not be loaded.
                    // We do not "reset" the column usages in the dbTable
                    // allowing it maybe to match another rule.
                }
            }
        } catch (RecorderException ex) {
            throw new MondrianException(ex);
        } finally {
            msgRecorder.logInfoMessage(getLogger());
            msgRecorder.logWarningMessage(getLogger());
            msgRecorder.logErrorMessage(getLogger());
            if (msgRecorder.hasErrors()) {
                throw mres.AggLoadingExceededErrorCount.ex(
                    msgRecorder.getErrorCount());
            }
        }
    }

    private Collection<RolapStar> getStars() {
        return schema.getStars();
    }

    private void reOrderAggStarList() {
        for (RolapStar star : getStars()) {
            star.reOrderAggStarList();
        }
    }

    /**
     * Returns a list containing every
     * {@link mondrian.rolap.aggmatcher.ExplicitRules.Group} in every
     * cubes in a given {@link RolapStar}.
     */
    protected List<ExplicitRules.Group> getAggGroups(RolapStar star) {
        List<ExplicitRules.Group> aggGroups =
            new ArrayList<ExplicitRules.Group>();
        for (RolapCube cube : schema.getCubesWithStar(star)) {
            if (cube.hasAggGroup() && cube.getAggGroup().hasRules()) {
                aggGroups.add(cube.getAggGroup());
            }
        }
        return aggGroups;
    }

    /**
     * This method mines the RolapStar and annotes the JdbcSchema.Table
     * dbFactTable by creating JdbcSchema.Table.Column.Usage instances. For
     * example, a measure in the RolapStar becomes a measure usage for the
     * column with the same name and a RolapStar foreign key column becomes a
     * foreign key usage for the column with the same name.
     *
     * @param dbFactTable fact table
     * @param star rolap star
     * @param msgRecorder message recorder
     */
    void bindToStar(
        final JdbcSchema.Table dbFactTable,
        final RolapStar star,
        final MessageRecorder msgRecorder)
        throws SQLException
    {
        Util.deprecated("never called", true);
        msgRecorder.pushContextName("AggTableManager.bindToStar");
        try {
            // load columns
            dbFactTable.load();

            dbFactTable.setTableUsageType(JdbcSchema.TableUsageType.FACT);

            // Aggregate tables are in same schema as fact table.
            // TODO: Create a default schema name for a physical schema, and
            // remove this logic.
            Util.deprecated("add PhysicalSchema@schemaName", false);
            RolapSchema.PhysRelation relation =
                star.getFactTable().getRelation();
            String schemaName = null;
            Map<String, String> tableHints = Collections.emptyMap();
            if (relation instanceof RolapSchema.PhysTable) {
                schemaName = ((RolapSchema.PhysTable) relation).getSchemaName();
                tableHints = ((RolapSchema.PhysTable) relation).getHintMap();
            }
            String tableName = dbFactTable.getName();
            String alias = relation.getSchema().newAlias();
            dbFactTable.table =
                new RolapSchema.PhysTable(
                    relation.getSchema(),
                    schemaName,
                    tableName,
                    alias,
                    tableHints);

            for (JdbcSchema.Table.Column factColumn
                : dbFactTable.getColumns())
            {
                String cname = factColumn.getName();
                List<RolapStar.Column> rcs =
                    star.getFactTable().lookupColumns(cname);

                for (RolapStar.Column rc : rcs) {
                    // its a measure
                    if (rc instanceof RolapStar.Measure) {
                        RolapStar.Measure rm = (RolapStar.Measure) rc;
                        JdbcSchema.Table.Column.Usage usage =
                            factColumn.newUsage(JdbcSchema.UsageType.MEASURE);
                        usage.setSymbolicName(rm.getName());

                        usage.setAggregator(rm.getAggregator());
                        usage.rMeasure = rm;
                    }
                }

                // it still might be a foreign key
                RolapStar.Table rTable =
                    star.getFactTable().findTableWithLeftJoinCondition(cname);
                if (rTable != null) {
                    JdbcSchema.Table.Column.Usage usage =
                        factColumn.newUsage(JdbcSchema.UsageType.FOREIGN_KEY);
                    usage.setSymbolicName("FOREIGN_KEY");
                    usage.rTable = rTable;
                } else {
                    RolapStar.Column rColumn =
                        star.getFactTable().lookupColumn(cname);
                    if ((rColumn != null)
                        && !(rColumn instanceof RolapStar.Measure))
                    {
                        // Ok, maybe its used in a non-shared dimension
                        // This is a column in the fact table which is
                        // (not necessarily) a measure but is also not
                        // a foreign key to an external dimension table.
                        JdbcSchema.Table.Column.Usage usage =
                            factColumn.newUsage(
                                JdbcSchema.UsageType.FOREIGN_KEY);
                        usage.setSymbolicName("FOREIGN_KEY");
                        usage.rColumn = rColumn;
                    }
                }

                // warn if it has not been identified
                if (!factColumn.hasUsage() && getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        mres.UnknownFactTableColumn.str(
                            msgRecorder.getContext(),
                            dbFactTable.getName(),
                            factColumn.getName()));
                }
            }
        } finally {
            msgRecorder.popContextName();
        }
    }
}

// End AggTableManager.java
