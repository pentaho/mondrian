/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.olap4j.CatalogFinder;
import mondrian.rolap.*;
import mondrian.server.monitor.*;
import mondrian.spi.CatalogLocator;
import mondrian.util.LockBox;
import mondrian.xmla.*;

import org.apache.log4j.Logger;

import org.olap4j.OlapConnection;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link mondrian.olap.MondrianServer}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 25, 2006
 */
class MondrianServerImpl
    extends MondrianServer
    implements CatalogFinder, XmlaHandler.ConnectionFactory
{
    /**
     * Id of server. Unique within JVM's lifetime. Not the same as the ID of
     * the server within a lockbox.
     */
    private final int id = ID_GENERATOR.incrementAndGet();

    /**
     * Within a server, registry of objects such as data sources and roles.
     * For convenience, all servers currently share the same lockbox.
     */
    private final LockBox lockBox;

    private final Repository repository;

    private final CatalogLocator catalogLocator;

    /**
     * Map of open connections, by id. Connections are added just after
     * construction, and are removed when they call close. Garbage collection
     * may cause a connection to be removed earlier.
     */
    private final Map<Integer, RolapConnection> connectionMap =
        new WeakHashMap<Integer, RolapConnection>();

    /**
     * Map of open statements, by id. Statements are added just after
     * construction, and are removed when they call close. Garbage collection
     * may cause a connection to be removed earlier.
     */
    private final Map<Long, Statement> statementMap =
        new WeakHashMap<Long, Statement>();

    private final MonitorImpl monitor = new MonitorImpl();

    private static final Logger LOGGER =
        Logger.getLogger(MondrianServerImpl.class);

    private static final AtomicInteger ID_GENERATOR = new AtomicInteger();

    private static final List<String> KEYWORD_LIST =
        Collections.unmodifiableList(Arrays.asList(
            "$AdjustedProbability", "$Distance", "$Probability",
            "$ProbabilityStDev", "$ProbabilityStdDeV", "$ProbabilityVariance",
            "$StDev", "$StdDeV", "$Support", "$Variance",
            "AddCalculatedMembers", "Action", "After", "Aggregate", "All",
            "Alter", "Ancestor", "And", "Append", "As", "ASC", "Axis",
            "Automatic", "Back_Color", "BASC", "BDESC", "Before",
            "Before_And_After", "Before_And_Self", "Before_Self_After",
            "BottomCount", "BottomPercent", "BottomSum", "Break", "Boolean",
            "Cache", "Calculated", "Call", "Case", "Catalog_Name", "Cell",
            "Cell_Ordinal", "Cells", "Chapters", "Children",
            "Children_Cardinality", "ClosingPeriod", "Cluster",
            "ClusterDistance", "ClusterProbability", "Clusters",
            "CoalesceEmpty", "Column_Values", "Columns", "Content",
            "Contingent", "Continuous", "Correlation", "Cousin", "Covariance",
            "CovarianceN", "Create", "CreatePropertySet", "CrossJoin", "Cube",
            "Cube_Name", "CurrentMember", "CurrentCube", "Custom", "Cyclical",
            "DefaultMember", "Default_Member", "DESC", "Descendents",
            "Description", "Dimension", "Dimension_Unique_Name", "Dimensions",
            "Discrete", "Discretized", "DrillDownLevel",
            "DrillDownLevelBottom", "DrillDownLevelTop", "DrillDownMember",
            "DrillDownMemberBottom", "DrillDownMemberTop", "DrillTrough",
            "DrillUpLevel", "DrillUpMember", "Drop", "Else", "Empty", "End",
            "Equal_Areas", "Exclude_Null", "ExcludeEmpty", "Exclusive",
            "Expression", "Filter", "FirstChild", "FirstRowset",
            "FirstSibling", "Flattened", "Font_Flags", "Font_Name",
            "Font_size", "Fore_Color", "Format_String", "Formatted_Value",
            "Formula", "From", "Generate", "Global", "Head", "Hierarchize",
            "Hierarchy", "Hierary_Unique_name", "IIF", "IsEmpty",
            "Include_Null", "Include_Statistics", "Inclusive", "Input_Only",
            "IsDescendant", "Item", "Lag", "LastChild", "LastPeriods",
            "LastSibling", "Lead", "Level", "Level_Unique_Name", "Levels",
            "LinRegIntercept", "LinRegR2", "LinRegPoint", "LinRegSlope",
            "LinRegVariance", "Long", "MaxRows", "Median", "Member",
            "Member_Caption", "Member_Guid", "Member_Name", "Member_Ordinal",
            "Member_Type", "Member_Unique_Name", "Members",
            "Microsoft_Clustering", "Microsoft_Decision_Trees", "Mining",
            "Model", "Model_Existence_Only", "Models", "Move", "MTD", "Name",
            "Nest", "NextMember", "Non", "Normal", "Not", "Ntext", "Nvarchar",
            "OLAP", "On", "OpeningPeriod", "OpenQuery", "Or", "Ordered",
            "Ordinal", "Pages", "Pages", "ParallelPeriod", "Parent",
            "Parent_Level", "Parent_Unique_Name", "PeriodsToDate", "PMML",
            "Predict", "Predict_Only", "PredictAdjustedProbability",
            "PredictHistogram", "Prediction", "PredictionScore",
            "PredictProbability", "PredictProbabilityStDev",
            "PredictProbabilityVariance", "PredictStDev", "PredictSupport",
            "PredictVariance", "PrevMember", "Probability",
            "Probability_StDev", "Probability_StdDev", "Probability_Variance",
            "Properties", "Property", "QTD", "RangeMax", "RangeMid",
            "RangeMin", "Rank", "Recursive", "Refresh", "Related", "Rename",
            "Rollup", "Rows", "Schema_Name", "Sections", "Select", "Self",
            "Self_And_After", "Sequence_Time", "Server", "Session", "Set",
            "SetToArray", "SetToStr", "Shape", "Skip", "Solve_Order", "Sort",
            "StdDev", "Stdev", "StripCalculatedMembers", "StrToSet",
            "StrToTuple", "SubSet", "Support", "Tail", "Text", "Thresholds",
            "ToggleDrillState", "TopCount", "TopPercent", "TopSum",
            "TupleToStr", "Under", "Uniform", "UniqueName", "Use", "Value",
            "Value", "Var", "Variance", "VarP", "VarianceP", "VisualTotals",
            "When", "Where", "With", "WTD", "Xor"));

    /**
     * Creates a MondrianServerImpl.
     *
     * @param registry Registry of all servers in this JVM
     * @param repository Repository of catalogs and schemas
     * @param catalogLocator Catalog locator
     */
    MondrianServerImpl(
        MondrianServerRegistry registry,
        Repository repository,
        CatalogLocator catalogLocator)
    {
        assert repository != null;
        assert catalogLocator != null;
        this.repository = repository;
        this.catalogLocator = catalogLocator;

        // All servers in a JVM share the same lockbox. This is a bit more
        // forgiving to applications which have slightly mismatched
        // specifications of the servers where they create and retrieve the
        // entry.
        this.lockBox = registry.lockBox;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        shutdown();
    }

    public int getId() {
        return id;
    }

    public List<String> getKeywords() {
        return KEYWORD_LIST;
    }

    public LockBox getLockBox() {
        return lockBox;
    }

    @Override
    public OlapConnection getConnection(
        String databaseName,
        String catalogName,
        String roleName)
        throws SQLException
    {
        return this.getConnection(
            databaseName, catalogName, roleName,
            new Properties());
    }

    @Override
    public OlapConnection getConnection(
        String databaseName,
        String catalogName,
        String roleName,
        Properties props)
        throws SQLException
    {
        return repository.getConnection(
            this, databaseName, catalogName, roleName, props);
    }

    public List<String> getCatalogNames(
        RolapConnection connection)
    {
        return
            repository.getCatalogNames(
                connection,
                // We assume that Mondrian supports a single database
                // per server.
                repository.getDatabaseNames(connection).get(0));
    }

    public List<Map<String, Object>> getDatabases(
        RolapConnection connection)
    {
        return repository.getDatabases(connection);
    }

    @Override
    public CatalogLocator getCatalogLocator() {
        return catalogLocator;
    }

    @Override
    public void shutdown() {
        monitor.shutdown();
        repository.shutdown();
    }

    @Override
    public void addConnection(RolapConnection connection) {
        connectionMap.put(
            connection.getId(),
            connection);
        monitor.sendEvent(
            new ConnectionStartEvent(
                System.currentTimeMillis(),
                connection.getServer().getId(),
                connection.getId()));
    }

    @Override
    public void removeConnection(RolapConnection connection) {
        connectionMap.remove(connection.getId());
        monitor.sendEvent(
            new ConnectionEndEvent(
                System.currentTimeMillis(),
                connection.getServer().getId(),
                connection.getId()));
    }

    @Override
    public RolapConnection getConnection(int connectionId) {
        return connectionMap.get(connectionId);
    }

    @Override
    public void addStatement(Statement statement) {
        statementMap.put(
            statement.getId(),
            statement);
        final RolapConnection connection =
            statement.getMondrianConnection();
        monitor.sendEvent(
            new StatementStartEvent(
                System.currentTimeMillis(),
                connection.getServer().getId(),
                connection.getId(),
                statement.getId()));
    }

    @Override
    public void removeStatement(Statement statement) {
        statementMap.remove(statement.getId());
        final RolapConnection connection =
            statement.getMondrianConnection();
        monitor.sendEvent(
            new StatementEndEvent(
                System.currentTimeMillis(),
                connection.getServer().getId(),
                connection.getId(),
                statement.getId()));
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public Map<String, RolapSchema> getRolapSchemas(
        RolapConnection connection,
        String catalogName)
    {
        return
            repository.getRolapSchemas(
                connection,
                // We assume that Mondrian supports a single database
                // per server.
                repository.getDatabaseNames(connection).get(0),
                catalogName);
    }

    public Map<String, Object> getPreConfiguredDiscoverDatasourcesResponse() {
        // No pre-configured response; XMLA servlet will connect to get
        // data source info.
        return null;
    }
}

// End MondrianServerImpl.java
