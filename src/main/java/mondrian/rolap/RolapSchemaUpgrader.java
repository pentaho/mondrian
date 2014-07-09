/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.olap.Id.NameSegment;
import mondrian.resource.MondrianResource;
import mondrian.spi.*;
import mondrian.util.ByteString;
import mondrian.util.Pair;

import org.apache.log4j.Logger;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.NodeDef;
import org.eigenbase.xom.TextDef;
import org.eigenbase.xom.XOMException;

import org.olap4j.metadata.NamedList;

import java.util.*;

import javax.sql.DataSource;

import static mondrian.olap.Util.first;
import static mondrian.olap.Util.identityFunctor;

/**
 * Converts a mondrian-3.x schema to a mondrian-4 schema.
 *
 * @author jhyde
 */
public class RolapSchemaUpgrader {
    private final PhysSchemaConverter physSchemaConverter;
    private final RolapSchemaLoader loader;
    private final RolapSchema schema;
    private final Map<String, CubeInfo> cubeInfoMap =
        new HashMap<String, CubeInfo>();

    public static final Logger LOGGER =
        Logger.getLogger(RolapSchemaUpgrader.class);

    /**
     * Creates a RolapSchemaUpgrader; private because you should use
     * {@link #upgrade}.
     *
     * @param loader Loader
     * @param schema Schema
     * @param physSchema Physical schema
     */
    private RolapSchemaUpgrader(
        RolapSchemaLoader loader,
        RolapSchema schema,
        RolapSchema.PhysSchema physSchema)
    {
        this.loader = loader;
        this.schema = schema;
        this.physSchemaConverter =
            new PhysSchemaConverter(loader, physSchema);
    }

    /**
     * Bootstraps the upgrade process.
     *
     * <p>Note that none of the arguments depend on {@link Mondrian3Def} types.
     * The goal is to keep dependencies on the old schema out of
     * {@link RolapSchemaLoader}.</p>
     *
     * @param loader Schema loader
     * @param def XML parse tree containing Mondrian-3 style schema
     * @param key Key
     * @param md5Bytes MD5 hash
     * @param connectInfo Connect properties
     * @param dataSource Data source
     * @param useContentChecksum Whether to use content checksum
     *
     * @return Schema in upgraded (mondrian-4) format
     * @throws XOMException on error
     */
    static MondrianDef.Schema upgrade(
        RolapSchemaLoader loader,
        DOMWrapper def,
        SchemaKey key,
        ByteString md5Bytes,
        Util.PropertyList connectInfo,
        DataSource dataSource,
        boolean useContentChecksum)
        throws XOMException
    {
        Mondrian3Def.Schema xmlLegacySchema =
            new Mondrian3Def.Schema(def);
        RolapSchema tempSchema =
            new RolapSchema(
                key,
                connectInfo,
                dataSource,
                md5Bytes,
                useContentChecksum,
                xmlLegacySchema.name,
                true,
                Collections.<Locale>emptySet(),
                Larders.create(
                    null,
                    null, // no caption available
                    xmlLegacySchema.description));
        DataServicesProvider provider =
            DataServicesLocator.getDataServicesProvider(
                tempSchema.getDataServiceProviderName());
        tempSchema.physicalSchema =
            new RolapSchema.PhysSchema(
                tempSchema.getDialect(),
                tempSchema.getInternalConnection(), provider);
        RolapSchemaUpgrader upgrader =
            new RolapSchemaUpgrader(
                loader, tempSchema, tempSchema.physicalSchema);
        return upgrader.convertSchema(xmlLegacySchema);
    }

    /**
     * Returns an xmlCalculatedMember called 'calcMemberName' in the
     * cube called 'cubeName' or return null if no calculatedMember or
     * xmlCube by those name exists.
     */
    protected static Mondrian3Def.CalculatedMember lookupXmlCalculatedMember(
        Mondrian3Def.Schema xmlSchema,
        final String calcMemberName,
        final String cubeName)
    {
        for (final Mondrian3Def.Cube cube : xmlSchema.cubes) {
            if (!Util.equalName(cube.name, cubeName)) {
                continue;
            }
            for (Mondrian3Def.CalculatedMember xmlCalcMember
                : cube.calculatedMembers)
            {
                // FIXME: Since fully-qualified names are not unique, we
                // should compare unique names. Also, the logic assumes that
                // CalculatedMember.dimension is not quoted (e.g. "Time")
                // and CalculatedMember.hierarchy is quoted
                // (e.g. "[Time].[Weekly]").
                if (Util.equalName(
                        calcMemberFqName(xmlCalcMember),
                        calcMemberName))
                {
                    return xmlCalcMember;
                }
            }
        }
        return null;
    }

    private static String calcMemberFqName(
        Mondrian3Def.CalculatedMember xmlCalcMember)
    {
        if (xmlCalcMember.dimension != null) {
            return Util.makeFqName(
                Util.quoteMdxIdentifier(xmlCalcMember.dimension),
                xmlCalcMember.name);
        } else {
            return Util.makeFqName(
                xmlCalcMember.hierarchy, xmlCalcMember.name);
        }
    }

    MondrianDef.Cube convertCube(
        Mondrian3Def.Schema xmlLegacySchema,
        final Mondrian3Def.Cube xmlLegacyCube)
    {
        final Mondrian3Def.Relation xmlFact = xmlLegacyCube.fact;
        RolapSchema.PhysRelation fact =
            xmlFact == null
                ? null
                : toPhysRelation(xmlFact);
        MondrianDef.Cube xmlCube = new MondrianDef.Cube();
        xmlCube.name = xmlLegacyCube.name;
        xmlCube.cache = xmlLegacyCube.cache;
        xmlCube.caption = xmlLegacyCube.caption;
        xmlCube.defaultMeasure = xmlLegacyCube.defaultMeasure;
        xmlCube.description = xmlLegacyCube.description;
        xmlCube.enabled = xmlLegacyCube.enabled;
        xmlCube.visible = xmlLegacyCube.visible;
        xmlCube.enableScenarios = false;
        final NamedList<MondrianDef.Dimension> xmlDimensions =
            xmlCube.children.holder(
                new MondrianDef.Dimensions()).list();
        final Map<String, MondrianDef.Dimension> xmlDimensionMap =
            new HashMap<String, MondrianDef.Dimension>();
        List<LevelInfo> levelList = new ArrayList<LevelInfo>();
        for (Mondrian3Def.CubeDimension xmlLegacyDimension
            : xmlLegacyCube.dimensions)
        {
            xmlDimensions.add(
                convertCubeDimension(
                    null,
                    xmlLegacyCube.name,
                    xmlDimensionMap,
                    fact,
                    xmlFact,
                    xmlLegacyDimension,
                    xmlLegacyDimension.visible,
                    xmlLegacySchema,
                    levelList));
        }

        final MondrianDef.MeasureGroup xmlMeasureGroup =
            convertCubeMeasures(
                xmlLegacyCube,
                fact,
                xmlCube.children.holder(
                    new MondrianDef.MeasureGroups()).list());
        if (xmlMeasureGroup == null) {
            return xmlCube;
        }

        final Map<String, String> dimensionNameFks =
            new LinkedHashMap<String, String>();
        for (Mondrian3Def.CubeDimension dimension : xmlLegacyCube.dimensions) {
            String foreignKey = dimension.foreignKey;
            if (foreignKey != null
                && isDegenerate(dimension, xmlLegacySchema, xmlFact))
            {
                // Ignore the foreignKey if the dimension is degenerate (that
                // is, its hierarchies have no <Table/> element). Turns out
                // that the SteelWheels schema has this problem.
                foreignKey = null;
            }
            dimensionNameFks.put(dimension.name, foreignKey);
        }
        convertMeasureLinks(
            xmlLegacyCube,
            xmlMeasureGroup,
            dimensionNameFks);

        for (Mondrian3Def.CalculatedMember xmlLegacyCalculatedMember
            : xmlLegacyCube.calculatedMembers)
        {
            xmlCube.children.holder(new MondrianDef.CalculatedMembers()).list()
                .add(
                    convertCalculatedMember(xmlLegacyCalculatedMember));
        }
        for (Mondrian3Def.NamedSet xmlLegacyNamedSet : xmlLegacyCube.namedSets)
        {
            xmlCube.children.holder(new MondrianDef.NamedSets()).list()
                .add(
                    convertNamedSet(xmlLegacyNamedSet));
        }
        convertAnnotations(
            xmlCube.children,
            xmlLegacyCube.annotations);
        cubeInfoMap.put(
            xmlCube.name,
            new CubeInfo(
                xmlCube.name, fact, xmlFact, xmlLegacyCube));

        if (xmlLegacyCube.fact instanceof Mondrian3Def.Table) {
            Mondrian3Def.Table xmlLegacyFactTable =
                (Mondrian3Def.Table) xmlLegacyCube.fact;
            for (Mondrian3Def.AggTable xmlLegacyAggTable
                : xmlLegacyFactTable.aggTables)
            {
                if (xmlLegacyAggTable instanceof Mondrian3Def.AggName) {
                    convertAggName(
                        xmlMeasureGroup,
                        (Mondrian3Def.AggName) xmlLegacyAggTable,
                        levelList,
                        xmlCube.children.holder(
                            new MondrianDef.MeasureGroups()).list());
                } else {
                    LOGGER.warn(
                        "Cannot convert " + xmlLegacyAggTable.getName());
                }
            }
        }

        return xmlCube;
    }

    // Upgrade old-style cube (which has a <Measures> element) to a
    // new-style cube (which has a <MeasureGroups> element).
    private MondrianDef.MeasureGroup convertCubeMeasures(
        Mondrian3Def.Cube xmlCube,
        RolapSchema.PhysRelation fact,
        NamedList<MondrianDef.MeasureGroup> xmlMeasureGroups)
    {
        final Mondrian3Def.Relation xmlFact = xmlCube.fact;
        if (xmlFact == null) {
            loader.getHandler().warning(
                "Cube '" + xmlCube.name + "' requires fact table",
                xmlCube,
                null);
            return null;
        }
        if (xmlFact.getAlias() == null) {
            throw Util.newError(
                "Must specify alias for fact table of cube '" + xmlCube.name
                + "'");
        }

        final MondrianDef.MeasureGroup xmlMeasureGroup =
            new MondrianDef.MeasureGroup();
        xmlMeasureGroup.type = "fact";
        xmlMeasureGroup.name = xmlCube.name;
        xmlMeasureGroup.table = xmlFact.getAlias();

        xmlMeasureGroups.add(xmlMeasureGroup);

        final NamedList<MondrianDef.MeasureOrRef> xmlMeasures =
            xmlMeasureGroup.children.holder(new MondrianDef.Measures()).list();
        for (Mondrian3Def.Measure xmlLegacyMeasure : xmlCube.measures) {
            xmlMeasures.add(
                convertMeasure(fact, xmlLegacyMeasure));
        }
        return xmlMeasureGroup;
    }

    private void convertMeasureLinks(
        Mondrian3Def.Cube xmlCube,
        MondrianDef.MeasureGroup xmlMeasureGroup,
        Map<String, String> dimensionNameFks)
    {
        final Mondrian3Def.Relation xmlFact = xmlCube.fact;
        List<MondrianDef.DimensionLink> xmlDimensionLinks =
            xmlMeasureGroup.children.holder(
                new MondrianDef.DimensionLinks()).list();
        for (Map.Entry<String, String> entry : dimensionNameFks.entrySet()) {
            final String dimensionName = entry.getKey();
            final String foreignKey = entry.getValue();
            final MondrianDef.DimensionLink xmlDimensionLink;
            if (foreignKey == null) {
                // Degenerate link (i.e. dimension lives within fact table)
                final MondrianDef.FactLink xmlFactLink =
                    new MondrianDef.FactLink();
                xmlFactLink.dimension = dimensionName;
                xmlDimensionLink = xmlFactLink;
            } else {
                MondrianDef.ForeignKeyLink xmlRegularLink =
                    new MondrianDef.ForeignKeyLink();
                xmlRegularLink.dimension = dimensionName;
                xmlRegularLink.foreignKey = new MondrianDef.ForeignKey();
                xmlRegularLink.foreignKey.array =
                    new MondrianDef.Column[] {
                        new MondrianDef.Column(
                            xmlFact.getAlias(), foreignKey)
                    };
                xmlDimensionLink = xmlRegularLink;
            }
            xmlDimensionLinks.add(xmlDimensionLink);
        }
    }

    // For example,
    //
    // <AggName name="agg_c_special_sales_fact_1997">
    //     <AggFactCount column="FACT_COUNT"/>
    //     <AggIgnoreColumn column="foo"/>
    //     <AggIgnoreColumn column="bar"/>
    //     <AggForeignKey factColumn="product_id" aggColumn="PRODUCT_ID" />
    //     <AggForeignKey factColumn="customer_id" aggColumn="CUSTOMER_ID" />
    //     <AggForeignKey factColumn="promotion_id" aggColumn="PROMOTION_ID" />
    //     <AggForeignKey factColumn="store_id" aggColumn="STORE_ID" />
    //     <AggMeasure name="[Measures].[Unit Sales]" column="UNIT_SALES_SUM" />
    //     <AggMeasure name="[Measures].[Store Cost]" column="STORE_COST_SUM" />
    //     <AggMeasure name="[Measures].[Store Sales]"
    //                 column="STORE_SALES_SUM" />
    //     <AggLevel name="[Time].[Year]" column="TIME_YEAR" />
    //     <AggLevel name="[Time].[Quarter]" column="TIME_QUARTER" />
    //     <AggLevel name="[Time].[Month]" column="TIME_MONTH" />
    // </AggName>
    //
    // becomes
    //
    // <MeasureGroup table='agg_c_special_sales_fact_1997' type='aggregate'>
    //     <Measures>
    //         <Measure name='Unit Sales 2' column='unit_sales_sum'
    //                  aggregator='sum' formatString='Standard'/>
    //         <MeasureRef name='Fact Count' aggColumn='fact_count'/>
    //         <MeasureRef name='Unit Sales' aggColumn='unit_sales_sum'/>
    //         <MeasureRef name='Store Cost' aggColumn='store_cost_sum'/>
    //         <MeasureRef name='Store Sales' aggColumn='store_sales_sum'/>
    //     </Measures>
    //     <DimensionLinks>
    //         <ForeignKeyLink dimension='Store' foreignKeyColumn='store_id'/>
    //         <ForeignKeyLink dimension='Product'
    //                         foreignKeyColumn='product_id'/>
    //         <ForeignKeyLink dimension='Promotion'
    //                         foreignKeyColumn='promotion_id'/>
    //         <ForeignKeyLink dimension='Customer'
    //                         foreignKeyColumn='customer_id'/>
    //         <CopyLink dimension='Time' attribute='Month'>
    //             <Column aggColumn='time_year' table='time_by_day'
    //                     name='the_year'/>
    //             <Column aggColumn='time_quarter' table='time_by_day'
    //                     name='quarter'/>
    //             <Column aggColumn='time_month' table='time_by_day'
    //                     name='month_of_year'/>
    //         </CopyLink>
    //     </DimensionLinks>
    // </MeasureGroup>
    private MondrianDef.MeasureGroup convertAggName(
        MondrianDef.MeasureGroup xmlFactMeasureGroup,
        Mondrian3Def.AggName xmlLegacyAggName,
        List<LevelInfo> levelList,
        NamedList<MondrianDef.MeasureGroup> xmlMeasureGroupList)
    {
        final MondrianDef.MeasureGroup xmlMeasureGroup =
            new MondrianDef.MeasureGroup();
        String nameTest = xmlLegacyAggName.name;
        while (true) {
            if (xmlMeasureGroupList.get(nameTest) != null) {
                nameTest += "_";
            } else {
                xmlMeasureGroup.name = nameTest;
                break;
            }
        }
        xmlMeasureGroup.table = xmlLegacyAggName.name;
        xmlMeasureGroup.type = "aggregate";
        xmlMeasureGroup.approxRowCount = xmlLegacyAggName.approxRowCount;
        Util.discard(xmlLegacyAggName.ignoreColumns);

        // Make sure table is listed in physical schema.
        final RolapSchema.PhysTable relation =
            (RolapSchema.PhysTable)
                lookupOrCreateTable(xmlLegacyAggName.name);
        relation.populateColumns(loader, null, null);

        Util.Function1<String, String> sanitizer;
        if (xmlLegacyAggName.ignorecase) {
            sanitizer =
                new Util.Function1<String, String>() {
                    public String apply(String param) {
                        for (String s : relation.columnsByName.keySet()) {
                            if (s.equalsIgnoreCase(param)) {
                                return s;
                            }
                        }
                        return param;
                    }
                };
        } else {
            sanitizer = identityFunctor();
        }
        final MondrianDef.Measures xmlMeasures =
            xmlMeasureGroup.children.holder(new MondrianDef.Measures());
        final MondrianDef.DimensionLinks xmlDimensionLinks =
            xmlMeasureGroup.children.holder(new MondrianDef.DimensionLinks());

        // For each dimension-link that uses "factColumn" in the fact table,
        // create a dimension-link in the agg table's measure group.
        for (Mondrian3Def.AggForeignKey xmlLegacyForeignKey
            : xmlLegacyAggName.foreignKeys)
        {
            convertAggForeignKey(
                xmlFactMeasureGroup,
                xmlDimensionLinks,
                xmlLegacyForeignKey.factColumn,
                sanitizer.apply(xmlLegacyForeignKey.aggColumn));
        }

        Map<String, MondrianDef.CopyLink> copyLinks =
            new HashMap<String, MondrianDef.CopyLink>();
        for (Mondrian3Def.AggLevel xmlLegacyLevel : xmlLegacyAggName.levels) {
            final LevelInfo level =
                lookupLevelInfo(levelList, xmlLegacyLevel.name);
            if (level == null) {
                LOGGER.warn(
                    "Level '" + xmlLegacyLevel.name
                    + "' not found; skipping this AggLevel");
                continue;
            }
            MondrianDef.CopyLink copyLink = copyLinks.get(level.dimension);
            if (copyLink == null) {
                copyLink = new MondrianDef.CopyLink();
                copyLink.dimension = level.dimension;
                copyLinks.put(level.dimension, copyLink);
                copyLink.columnRefs = new MondrianDef.Column[0];
                xmlDimensionLinks.list().add(copyLink);
            }
            final MondrianDef.Column column = new MondrianDef.Column();
            column.aggColumn = sanitizer.apply(xmlLegacyLevel.column);
            column.name = sanitizer.apply(level.column);
            column.table = level.table;
            copyLink.columnRefs = Util.append(copyLink.columnRefs, column);
            if (!RolapSchemaLoader.toBoolean(xmlLegacyLevel.collapsed, true)) {
                    // TODO:
            }
        }
        if (xmlLegacyAggName.factcount != null) {
            final MondrianDef.MeasureRef xmlCountMeasureRef =
                new MondrianDef.MeasureRef();
            xmlCountMeasureRef.aggColumn =
                sanitizer.apply(xmlLegacyAggName.factcount.column);
            xmlCountMeasureRef.name = "Fact Count";
            xmlMeasures.list().add(xmlCountMeasureRef);
        }
        for (Mondrian3Def.AggMeasure xmlLegacyMeasure
            : xmlLegacyAggName.measures)
        {
            final MondrianDef.MeasureRef xmlMeasureRef =
                new MondrianDef.MeasureRef();
            xmlMeasureRef.aggColumn = sanitizer.apply(xmlLegacyMeasure.column);
            xmlMeasureRef.name = xmlLegacyMeasure.name;
            xmlMeasures.list().add(xmlMeasureRef);
        }

        xmlMeasureGroupList.add(xmlMeasureGroup);
        return xmlMeasureGroup;
    }

    private void convertAggForeignKey(
        MondrianDef.MeasureGroup xmlFactMeasureGroup,
        MondrianDef.DimensionLinks xmlDimensionLinks,
        String factColumn,
        String aggColumn)
    {
        for (MondrianDef.DimensionLink xmlLink
            : xmlFactMeasureGroup.getDimensionLinks())
        {
            if (xmlLink instanceof MondrianDef.ForeignKeyLink) {
                MondrianDef.ForeignKeyLink xmlFKLink =
                    (MondrianDef.ForeignKeyLink) xmlLink;
                if (matchesForeignKey(
                        xmlFKLink, factColumn))
                {
                    final MondrianDef.ForeignKeyLink xmlAggFKLink =
                        new MondrianDef.ForeignKeyLink();
                    xmlAggFKLink.foreignKeyColumn = aggColumn;
                    xmlAggFKLink.dimension = xmlFKLink.dimension;
                    xmlDimensionLinks.list().add(xmlAggFKLink);
                }
            }
        }
    }

    private boolean matchesForeignKey(
        MondrianDef.ForeignKeyLink xmlForeignKeyLink,
        String factColumn)
    {
        if (xmlForeignKeyLink.foreignKeyColumn != null
            && xmlForeignKeyLink.foreignKeyColumn.equals(factColumn))
        {
            return true;
        }
        if (xmlForeignKeyLink.foreignKey != null
            && xmlForeignKeyLink.foreignKey.array.length == 1
            && xmlForeignKeyLink.foreignKey.array[0].name.equals(factColumn))
        {
            return true;
        }
        return false;
    }

    private RolapSchema.PhysRelation lookupOrCreateTable(String table) {
        final RolapSchema.PhysSchema physSchema =
            physSchemaConverter.physSchema;
        RolapSchema.PhysRelation physRelation =
            physSchema.tablesByName.get(table);
        if (physRelation == null) {
            physRelation =
                new RolapSchema.PhysTable(
                    physSchema,
                    null,
                    table,
                    table,
                    Collections.<String, String>emptyMap());
            physSchema.tablesByName.put(table, physRelation);
        }
        return physRelation;
    }

    private boolean findOriginalMembers(
        Formula formula,
        List<MondrianDef.CalculatedMember> xmlCalcMemberList,
        List<Formula> calcMemberList)
    {
        for (MondrianDef.CalculatedMember xmlCalcMember : xmlCalcMemberList) {
            Dimension dimension =
                lookupDimension(
                    new Id.NameSegment(
                        xmlCalcMember.dimension,
                        Id.Quoting.UNQUOTED));
            if (formula.getName().equals(xmlCalcMember.name)
                && formula.getMdxMember().getDimension().getName().equals(
                    dimension.getName()))
            {
                calcMemberList.add(formula);
                return true;
            }
        }
        return false;
    }

    private Dimension lookupDimension(Id.Segment segment) {
        return null;
    }

    private MondrianDef.Dimension convertCubeDimension(
        Map<String, Info> infoMap,
        String cubeName,
        Map<String, MondrianDef.Dimension> xmlDimensionMap,
        RolapSchema.PhysRelation fact,
        Mondrian3Def.RelationOrJoin xmlFact,
        Mondrian3Def.CubeDimension xmlLegacyCubeDimension,
        boolean visible,
        Mondrian3Def.Schema xmlLegacySchema,
        List<LevelInfo> levelList)
    {
        Mondrian3Def.Dimension xmlLegacyDimension;
        if (xmlLegacyCubeDimension instanceof Mondrian3Def.Dimension) {
            xmlLegacyDimension =
                (Mondrian3Def.Dimension) xmlLegacyCubeDimension;
        } else {
            Mondrian3Def.DimensionUsage xmlLegacyDimensionUsage =
                (Mondrian3Def.DimensionUsage) xmlLegacyCubeDimension;
            xmlLegacyDimension =
                xmlLegacyDimensionUsage.getDimension(xmlLegacySchema);
        }

        assert physSchemaConverter != null;
        final List<Link> links;
        String dimensionName = xmlLegacyDimension.name;
        if (xmlLegacyCubeDimension
            instanceof Mondrian3Def.VirtualCubeDimension)
        {
            final Mondrian3Def.VirtualCubeDimension xmlVirtualCubeDimension =
                (Mondrian3Def.VirtualCubeDimension) xmlLegacyCubeDimension;
            assert xmlFact == null
                : "VirtualCubeDimension only occurs within virtual cube, "
                  + "which has no fact table";
            if (xmlVirtualCubeDimension.cubeName == null) {
                // No cube specified. It's a shared dimension, and it is
                // implicitly joined to all cubes mentioned in other
                // VirtualCubeDimensions of this VirtualCube.
                links = new ArrayList<Link>();
                for (Info info : infoMap.values()) {
                    // Usage of a cube. E.g. the [Warehouse and Sales]
                    // virtual cube uses real cubes [Sales] and [Warehouse]
                    final Mondrian3Def.Cube xmlCubeUsed = info.xmlLegacyCube;
                    // Find the unique dimension in the cube that uses the
                    // given shared dimension. If there is more than one,
                    // it is a user error.
                    List<Mondrian3Def.DimensionUsage> usageList =
                        lookupSharedDimension(
                            xmlCubeUsed, xmlVirtualCubeDimension.name);
                    switch (usageList.size()) {
                    case 0:
                        break;
                    case 1:
                        links.add(
                            new Link(
                                toPhysRelation(
                                    xmlCubeUsed.fact),
                                usageList.get(0).foreignKey));
                        break;
                    default:
                        Util.deprecated("test this", false);
                        loader.getHandler().error(
                            "Shared cube dimension is ambiguous: more than "
                            + "one dimension in base cube "
                            + xmlCubeUsed.name
                            + " uses shared dimension "
                            + xmlVirtualCubeDimension.name,
                            xmlLegacyCubeDimension,
                            null);
                        break;
                    }
                }
                if (links.isEmpty()) {
                    loader.getHandler().error(
                        "Virtual cube dimension must join to at least one "
                        + "cube: dimension '" + xmlVirtualCubeDimension.name
                        + "' in cube '" + cubeName + "'",
                        xmlLegacyCubeDimension,
                        null);
                }
            } else {
                Mondrian3Def.Cube cube =
                    getCube(xmlLegacySchema, xmlVirtualCubeDimension.cubeName);
                if (cube == null) {
                    Util.deprecated("use schema.error, and test", false);
                    throw Util.newError(
                        "Unknown cube '"
                        + xmlVirtualCubeDimension.cubeName + "'");
                }
                xmlFact = cube.fact;
                links = Collections.singletonList(
                    new Link(
                        toPhysRelation(cube.fact),
                        xmlVirtualCubeDimension.foreignKey));
            }
        } else {
            final String foreignKey;
            if (xmlLegacyCubeDimension instanceof Mondrian3Def.DimensionUsage) {
                final Mondrian3Def.DimensionUsage xmlLegacyDimensionUsage =
                    (Mondrian3Def.DimensionUsage) xmlLegacyCubeDimension;
                foreignKey = xmlLegacyDimensionUsage.foreignKey;
                if (xmlLegacyDimensionUsage.name != null) {
                    dimensionName = xmlLegacyDimensionUsage.name;
                }
            } else if (xmlLegacyCubeDimension instanceof Mondrian3Def.Dimension)
            {
                foreignKey = xmlLegacyDimension.foreignKey;
            } else {
                throw new AssertionError("unknown dimension type");
            }
            final boolean degenerate =
                isDegenerate(xmlLegacyCubeDimension, xmlLegacySchema, xmlFact);
            if (foreignKey == null && !degenerate) {
                throw loader.getHandler().fatal(
                    "Dimension or DimensionUsage must have foreignKey",
                    xmlLegacyCubeDimension,
                    null);
            }
            links =
                Collections.singletonList(
                    new Link(fact, foreignKey));
            final Pair<String, String> pair =
                getUniquePrimaryKey(xmlLegacyDimension);
            physSchemaConverter.dimensionLinks.put(
                dimensionName,
                new RolapSchemaLoader.PhysSchemaBuilder.DimensionLink(
                    this, dimensionName, fact, foreignKey,
                    pair.left, pair.right, degenerate));
        }

        MondrianDef.Dimension xmlDimension =
            convertDimension(
                links,
                xmlDimensionMap,
                xmlFact,
                xmlLegacyDimension,
                dimensionName,
                visible,
                first(
                    xmlLegacyCubeDimension.description,
                    xmlLegacyDimension.description),
                levelList);
        convertAnnotations(
            xmlDimension.children,
            xmlLegacyCubeDimension.annotations);
        convertCaption(
            xmlLegacyCubeDimension, xmlLegacyDimension, xmlDimension);
        if (xmlLegacyCubeDimension instanceof Mondrian3Def.DimensionUsage) {
            xmlDimension.source =
                ((Mondrian3Def.DimensionUsage) xmlLegacyCubeDimension).source;
            xmlDimension.key = null;
        }
        return xmlDimension;
    }

    private void convertCaption(
        Mondrian3Def.CubeDimension xmlLegacyCubeDimension,
        Mondrian3Def.Dimension xmlLegacyDimension,
        MondrianDef.Dimension xmlDimension)
    {
        xmlDimension.caption =
            first(xmlLegacyCubeDimension.caption, xmlLegacyDimension.caption);
    }

    /**
     * Returns whether a dimension is degenerate; that is, all of its
     * hierarchies are in the fact table.
     *
     * @param xmlCubeDimension Dimension
     * @param xmlSchema Schema
     * @param xmlFact Fact table
     * @return Whether dimension is degenerate
     */
    private boolean isDegenerate(
        Mondrian3Def.CubeDimension xmlCubeDimension,
        Mondrian3Def.Schema xmlSchema,
        Mondrian3Def.RelationOrJoin xmlFact)
    {
        if (xmlCubeDimension instanceof Mondrian3Def.Dimension) {
            Mondrian3Def.Dimension dimension =
                (Mondrian3Def.Dimension) xmlCubeDimension;
            for (Mondrian3Def.Hierarchy hierarchy : dimension.hierarchies) {
                if (hierarchy.relation != null
                    && !Util.equals(hierarchy.relation, xmlFact))
                {
                    return false;
                }
            }
            return true;
        } else if (xmlCubeDimension instanceof Mondrian3Def.DimensionUsage) {
            final Mondrian3Def.Dimension dimension =
                xmlCubeDimension.getDimension(xmlSchema);
            if (dimension == null) {
                // Dimension usage uses an invalid dimension. It doesn't matter
                // whether the dimension is degenerate, they'll get an error
                // either way.
                return false;
            }
            return isDegenerate(dimension, xmlSchema, xmlFact);
        }
        return false;
    }

    /**
     * Returns a pair (primary key table, primary key) if all of the hierarchies
     * in a dimension have the same primary key details; throws otherwise.
     *
     * @param dimension XML dimension
     * @return Primary key table and column, never null
     */
    Pair<String, String> getUniquePrimaryKey(
        Mondrian3Def.Dimension dimension)
    {
        if (dimension.hierarchies.length == 0) {
            throw loader.getHandler().fatal(
                "Dimension has no hierarchies",
                dimension,
                null);
        }
        Set<Pair<String, String>> primaryKeySet =
            new HashSet<Pair<String, String>>();
        for (Mondrian3Def.Hierarchy xmlHierarchy : dimension.hierarchies) {
            String primaryKeyTable = xmlHierarchy.primaryKeyTable;
            if (primaryKeyTable == null
                && xmlHierarchy.relation instanceof Mondrian3Def.Relation)
            {
                primaryKeyTable =
                    ((Mondrian3Def.Relation) xmlHierarchy.relation).getAlias();
            }
            primaryKeySet.add(
                Pair.of(
                    primaryKeyTable,
                    xmlHierarchy.primaryKey));
        }
        if (primaryKeySet.size() != 1) {
            throw loader.getHandler().fatal(
                "Cannot convert schema: hierarchies in dimension '"
                + dimension.name
                + "' do not have consistent primary keys",
                dimension,
                null);
        }
        return primaryKeySet.iterator().next();
    }

    /**
     * Returns a set of the names of the cubes used by a virtual cube. The
     * order is deterministic.
     *
     *
     * @param xmlLegacySchema XML schema containing cube; for shared dimensions
     *                        and other cubes
     * @param xmlVirtualCube XML virtual cube
     * @return Map containing constituent cubes, and list of dimension names
     */
    private Pair<Set<String>, Map<String, Info>> buildInfoMap(
        Mondrian3Def.Schema xmlLegacySchema,
        Mondrian3Def.VirtualCube xmlVirtualCube)
    {
        // First pass: collect names.
        Set<String> dimensionNames = new LinkedHashSet<String>();
        HashSet<String> names = new LinkedHashSet<String>();
        if (xmlVirtualCube.cubeUsage != null) {
            for (Mondrian3Def.CubeUsage xmlCubeUsage
                : xmlVirtualCube.cubeUsage.cubeUsages)
            {
                names.add(xmlCubeUsage.cubeName);
            }
        }
        for (Mondrian3Def.VirtualCubeDimension virtualCubeDimension
            : xmlVirtualCube.dimensions)
        {
            if (virtualCubeDimension.cubeName != null) {
                names.add(virtualCubeDimension.cubeName);
            }
            dimensionNames.add(virtualCubeDimension.name);
        }
        for (Mondrian3Def.VirtualCubeMeasure virtualCubeMeasure
            : xmlVirtualCube.measures)
        {
            if (virtualCubeMeasure.cubeName != null) {
                names.add(virtualCubeMeasure.cubeName);
            }
        }

        // Second pass, populate map.
        final Map<String, Info> infoMap = new LinkedHashMap<String, Info>();
        for (String name : names) {
            Mondrian3Def.Cube xmlLegacyCube = getCube(xmlLegacySchema, name);
            if (xmlLegacyCube == null) {
                throw Util.newError(
                    "Cube '" + name + "' not found");
            }
            infoMap.put(name, new Info(name, xmlLegacyCube));
        }

        // Third pass, populate ignoreUnrelatedDimensions
        if (xmlVirtualCube.cubeUsage != null) {
            for (Mondrian3Def.CubeUsage xmlCubeUsage
                : xmlVirtualCube.cubeUsage.cubeUsages)
            {
                infoMap.get(xmlCubeUsage.cubeName).ignoreUnrelatedDimensions =
                    RolapSchemaLoader.toBoolean(
                        xmlCubeUsage.ignoreUnrelatedDimensions, false);
            }
        }

        return Pair.of(dimensionNames, infoMap);
    }

    private Mondrian3Def.VirtualCube getVirtualCube(
        Mondrian3Def.Schema xmlSchema,
        String name)
    {
        for (Mondrian3Def.VirtualCube cube : xmlSchema.virtualCubes) {
            if (cube.name.equals(name)) {
                return cube;
            }
        }
        return null;
    }

    private Mondrian3Def.Cube getCube(
        Mondrian3Def.Schema xmlSchema,
        String name)
    {
        for (Mondrian3Def.Cube cube : xmlSchema.cubes) {
            if (cube.name.equals(name)) {
                return cube;
            }
        }
        return null;
    }

    /**
     * Understand this and you are no longer a novice.
     *
     * @param dimension Dimension
     */
    void registerDimension_old(
        RolapCubeDimension dimension)
    {
        if (true) {
            return;
        }
        RolapStar star = null; //getStar();
        RolapCube cube = dimension.getCube();

        for (RolapCubeHierarchy hierarchy : dimension.getHierarchyList()) {
            Util.deprecated("obsolete method?", false);
            Mondrian3Def.RelationOrJoin relation = null;
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }
            final List<? extends RolapCubeLevel> levels =
                hierarchy.getLevelList();

            HierarchyUsage[] hierarchyUsages = getUsages(hierarchy);
            if (hierarchyUsages.length == 0) {
                if (getLogger().isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(64);
                    buf.append("RolapCube.registerDimension: ");
                    buf.append("hierarchyUsages == null for cube=\"");
                    buf.append(dimension.cube.getName());
                    buf.append("\", hierarchy=\"");
                    buf.append(hierarchy.getName());
                    buf.append("\"");
                    getLogger().debug(buf.toString());
                }
                continue;
            }

            for (HierarchyUsage hierarchyUsage : hierarchyUsages) {
                String usagePrefix = hierarchyUsage.getUsagePrefix();
                RolapStar.Table table = star.getFactTable();

                String levelName = hierarchyUsage.getLevelName();

                // RME
                // If a DimensionUsage has its level attribute set, then
                // one wants joins to occur at that level and not below (not
                // at a finer level), i.e., if you have levels: Year, Quarter,
                // Month, and Day, and the level attribute is set to Month, the
                // you do not want aggregate joins to include the Day level.
                // By default, it is the lowest level that the fact table
                // joins to, the Day level.
                // To accomplish this, we reorganize the relation and then
                // copy it (so that elsewhere the original relation can
                // still be used), and finally, clip off those levels below
                // the DimensionUsage level attribute.
                // Note also, if the relation (MondrianDef.Relation) is not
                // a MondrianDef.Join, i.e., the dimension is not a snowflake,
                // there is a single dimension table, then this is currently
                // an unsupported configuration and all bets are off.
                if (relation instanceof Mondrian3Def.Join) {
                    // RME
                    // take out after things seem to be working
                    Mondrian3Def.RelationOrJoin relationTmp1 = relation;

                    relation = reorder(relation, levels);

                    if (relation == null && getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.registerDimension: after reorder relation==null");
                        getLogger().debug(
                            "RolapCube.registerDimension: reorder relationTmp1="
                            + format(relationTmp1));
                    }
                }

                Mondrian3Def.RelationOrJoin relationTmp2 = relation;

                if (levelName != null) {
                    // When relation is a table, this does nothing. Otherwise
                    // it tries to arrange the joins so that the fact table
                    // in the RolapStar will be joining at the lowest level.
                    //

                    // Make sure the level exists
                    RolapLevel level = lookupLevel(levels, levelName);
                    if (level == null) {
                        StringBuilder buf = new StringBuilder(64);
                        buf.append("For cube \"");
                        buf.append(cube.getName());
                        buf.append("\" and HierarchyUsage [");
                        buf.append(hierarchyUsage);
                        buf.append("], there is no level with given");
                        buf.append(" level name \"");
                        buf.append(levelName);
                        buf.append("\"");
                        throw Util.newInternal(buf.toString());
                    }

                    // If level has child, not the lowest level, then snip
                    // relation between level and its child so that
                    // joins do not include the lower levels.
                    // If the child level is null, then the DimensionUsage
                    // level attribute was simply set to the default, lowest
                    // level and we do nothing.
                    if (relation instanceof Mondrian3Def.Join) {
                        RolapLevel childLevel = level.getChildLevel();
                        if (childLevel != null) {
                            String tableName =
                                RolapSchemaLoader.getTableName(childLevel);
                            if (tableName != null) {
                                relation = snip(relation, tableName);

                                if (relation == null
                                    && getLogger().isDebugEnabled())
                                {
                                    getLogger().debug(
                                        "RolapCube.registerDimension: after snip relation==null");
                                    getLogger().debug(
                                        "RolapCube.registerDimension: snip relationTmp2="
                                        + format(relationTmp2));
                                }
                            }
                        }
                    }
                }

                // cube and dimension usage are in different tables
                if (!relation.equals(table.getRelation())) {
                    // HierarchyUsage should have checked this.
                    if (hierarchyUsage.getForeignKey() == null) {
                        throw MondrianResource.instance()
                            .HierarchyMustHaveForeignKey.ex(
                                hierarchy.getName(), cube.getName());
                    }
                    // jhyde: check is disabled until we handle <View> correctly
                    if (false
                        && !star.getFactTable().containsColumn(
                            hierarchyUsage.getForeignKey()))
                    {
                        throw MondrianResource.instance()
                            .HierarchyInvalidForeignKey.ex(
                                hierarchyUsage.getForeignKey(),
                                hierarchy.getName(),
                                cube.getName());
                    }
                    // parameters:
                    //   fact table,
                    //   fact table foreign key,
                    RolapSchema.PhysColumn column =
                        star.getFactTable().getRelation().getColumn(
                            hierarchyUsage.getForeignKey(),
                            true);
                    // parameters:
                    //   left column
                    //   right column
                    RolapStar.Condition joinCondition =
                        new RolapStar.Condition(
                            column,
                            hierarchyUsage.getJoinExp());
                    RolapSchema.PhysHop hop = null;

                    // (rchen) potential bug?:
                    // FACT table joins with tables in a hierarchy in the
                    // order they appear in the schema definition, even though
                    // the primary key for this hierarchy can be on a table
                    // which is not the leftmost.
                    // e.g.
                    //
                    // <Dimension name="Product">
                    // <Hierarchy hasAll="true" primaryKey="product_id"
                    //    primaryKeyTable="product">
                    //  <Join
                    //      leftKey="product_class_id"
                    //      rightKey="product_class_id">
                    //    <Table name="product_class"/>
                    //    <Table name="product"/>
                    //  </Join>
                    // </Hierarchy>
                    // </Dimension>
                    //
                    // When this hierarchy is referenced in a cube, the fact
                    // table is joined with the dimension tables using this
                    // incorrect join condition which assumes the leftmost
                    // table produces the primaryKey:
                    //   "fact"."foreignKey" = "product_class"."product_id"

                    table = addJoin(
                        table, physSchemaConverter, relation, hop);
                }

                // The parent Column is used so that non-shared dimensions
                // which use the fact table (not a separate dimension table)
                // can keep a record of what other columns are in the
                // same set of levels.
                RolapStar.Column parentColumn = null;

                // If the level name is not null, then we need only register
                // those columns for that level and above.
                if (levelName != null) {
                    for (RolapCubeLevel level : levels) {
                        if (null /* level.getKeyExp() */ != null) {
                            parentColumn = null;
//                                makeColumns(
//                                    table, level, parentColumn, usagePrefix);
                        }
                        if (levelName.equals(level.getName())) {
                            break;
                        }
                    }
                } else {
                    // This is the normal case, no level attribute so register
                    // all columns.
                    for (RolapCubeLevel level : levels) {
                        if (null /* level.getKeyExp() */ != null) {
                            parentColumn = null;
//                                makeColumns(
//                                    table, level, parentColumn, usagePrefix);
                        }
                    }
                }
            }
        }
    }

    private Logger getLogger() {
        return RolapSchema.LOGGER;
    }

    private HierarchyUsage[] getUsages(RolapCubeHierarchy hierarchy) {
        return new HierarchyUsage[0];
    }

    // The following code deals with handling the DimensionUsage level attribute
    // and snowflake dimensions only.

    /**
     * Formats a {@link mondrian.olap.Mondrian3Def.RelationOrJoin}, indenting
     * joins for readability.
     *
     * @param relation Relation
     */
    private static String format(Mondrian3Def.RelationOrJoin relation) {
        StringBuilder buf = new StringBuilder();
        format(relation, buf, "");
        return buf.toString();
    }

    private static void format(
        Mondrian3Def.RelationOrJoin relation,
        StringBuilder buf,
        String indent)
    {
        if (relation instanceof Mondrian3Def.Table) {
            Mondrian3Def.Table table = (Mondrian3Def.Table) relation;

            buf.append(indent);
            buf.append(table.name);
            if (table.alias != null) {
                buf.append('(');
                buf.append(table.alias);
                buf.append(')');
            }
            buf.append(Util.nl);
        } else {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relation;
            String subindent = indent + "  ";

            buf.append(indent);
            //buf.append(join.leftAlias);
            buf.append(join.getLeftAlias());
            buf.append('.');
            buf.append(join.leftKey);
            buf.append('=');
            buf.append(join.getRightAlias());
            //buf.append(join.rightAlias);
            buf.append('.');
            buf.append(join.rightKey);
            buf.append(Util.nl);
            format(join.left, buf, subindent);
            format(join.right, buf, indent);
        }
    }

    public static <T extends OlapElement> T lookupLevel(
        List<? extends T> elements,
        String name)
    {
        for (T element : elements) {
            if (element.getName().equals(name)) {
                return element;
            }
        }
        return null;
    }

    public static LevelInfo lookupLevelInfo(
        List<LevelInfo> elements,
        String name)
    {
        for (LevelInfo element : elements) {
            if (element.level.equals(name)) {
                return element;
            }

            final String elementName =
                Util.quoteMdxIdentifier(
                    NameSegment.toList(
                        element.dimension,
                        element.hierarchy,
                        element.level));

            if (elementName.equals(name)) {
                return element;
            }
        }

        // Lets try [Dimension Name].[Level Name]
        for (LevelInfo element : elements) {
            if (element.level.equals(name)) {
                return element;
            }

            final String shortElementName =
                Util.quoteMdxIdentifier(
                    NameSegment.toList(
                        element.dimension,
                        element.level));

            if (shortElementName.equals(name)) {
                return element;
            }
        }

        return null;
    }

    /**
     * Attempts to transform a {@link mondrian.olap.Mondrian3Def.RelationOrJoin}
     * into the "canonical" form.
     *
     * <p>What is the canonical form? It is only relevant
     * when the relation is a snowflake (nested joins), not simply a table.
     * The canonical form has lower levels to the left of higher levels (Day
     * before Month before Quarter before Year) and the nested joins are always
     * on the right side of the parent join.
     *
     * <p>The canonical form is (using a Time dimension example):
     * <pre>
     *            |
     *    ----------------
     *    |             |
     *   Day      --------------
     *            |            |
     *          Month      ---------
     *                     |       |
     *                   Quarter  Year
     * </pre>
     * <p>
     * When the relation looks like the above, then the fact table joins to the
     * lowest level table (the Day table) which joins to the next level (the
     * Month table) which joins to the next (the Quarter table) which joins to
     * the top level table (the Year table).
     * <p>
     * This method supports the transformation of a subset of all possible
     * join/table relation trees (and anyone who whats to generalize it is
     * welcome to). It will take any of the following and convert them to
     * the canonical.
     * <pre>
     *            |
     *    ----------------
     *    |             |
     *   Year     --------------
     *            |            |
     *         Quarter     ---------
     *                     |       |
     *                   Month    Day
     *
     *                  |
     *           ----------------
     *           |              |
     *        --------------   Year
     *        |            |
     *    ---------     Quarter
     *    |       |
     *   Day     Month
     *
     *                  |
     *           ----------------
     *           |              |
     *        --------------   Day
     *        |            |
     *    ---------      Month
     *    |       |
     *   Year   Quarter
     *
     *            |
     *    ----------------
     *    |             |
     *   Day      --------------
     *            |            |
     *          Month      ---------
     *                     |       |
     *                   Quarter  Year
     *
     * </pre>
     * <p>
     * In addition, at any join node, it can exchange the left and right
     * child relations so that the lower level depth is to the left.
     * For example, it can also transform the following:
     * <pre>
     *                |
     *         ----------------
     *         |              |
     *      --------------   Day
     *      |            |
     *    Month     ---------
     *              |       |
     *             Year   Quarter
     * </pre>
     * <p>
     * What it can not handle are cases where on both the left and right side of
     * a join there are child joins:
     * <pre>
     *                |
     *         ----------------
     *         |              |
     *      ---------     ----------
     *      |       |     |        |
     *    Month    Day   Year    Quarter
     *
     *                |
     *         ----------------
     *         |              |
     *      ---------     ----------
     *      |       |     |        |
     *    Year     Day   Month   Quarter
     * </pre>
     * <p>
     * When does this method do nothing? 1) when there are less than 2 levels,
     * 2) when any level does not have a table name, and 3) when for every table
     * in the relation there is not a level. In these cases, this method simply
     * return the original relation.
     *
     * @param relation Relation
     * @param levels Levels
     */
    private static Mondrian3Def.RelationOrJoin reorder(
        Mondrian3Def.RelationOrJoin relation,
        List<? extends RolapCubeLevel> levels)
    {
        // Need at least two levels; with only one level there's nothing to do.
        if (levels.size() < 2) {
            return relation;
        }

        Map<String, RelNode> nodeMap = new HashMap<String, RelNode>();

        // Create RelNode in top down order (year -> day)
        for (int i = 0; i < levels.size(); i++) {
            RolapLevel level = levels.get(i);

            if (level.isAll()) {
                continue;
            }

            // this is the table alias
            String tableName = RolapSchemaLoader.getTableName(level);
            if (tableName == null) {
                // punt, no table name
                return relation;
            }
            RelNode rnode = new RelNode(tableName, i);
            nodeMap.put(tableName, rnode);
        }
        if (! validateNodes(relation, nodeMap)) {
            return relation;
        }
        relation = copy(relation);

        // Put lower levels to the left of upper levels
        leftToRight(relation, nodeMap);

        // Move joins to the right side
        topToBottom(relation);

        return relation;
    }

    /**
     * The map has to be validated against the relation because there are
     * certain cases where we do not want to (read: can not) do reordering, for
     * instance, when closures are involved.
     *
     * @param relation Relation
     * @param map Map
     */
    private static boolean validateNodes(
        Mondrian3Def.RelationOrJoin relation,
        Map<String, RelNode> map)
    {
        if (relation instanceof Mondrian3Def.Relation) {
            Mondrian3Def.Relation table =
                (Mondrian3Def.Relation) relation;

            RelNode relNode = RelNode.lookup(table, map);
            return (relNode != null);

        } else if (relation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relation;

            return validateNodes(join.left, map)
                && validateNodes(join.right, map);

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Transforms the Relation moving the tables associated with
     * lower levels (greater level depth, i.e., Day is lower than Month) to the
     * left of tables with high levels.
     *
     * @param relation Relaion
     * @param map Map
     */
    private static int leftToRight(
        Mondrian3Def.RelationOrJoin relation,
        Map<String, RelNode> map)
    {
        if (relation instanceof Mondrian3Def.Relation) {
            Mondrian3Def.Relation table =
                (Mondrian3Def.Relation) relation;

            RelNode relNode = RelNode.lookup(table, map);
            // Associate the table with its RelNode!!!! This is where this
            // happens.
            relNode.table = table;

            return relNode.depth;

        } else if (relation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relation;
            int leftDepth = leftToRight(join.left, map);
            int rightDepth = leftToRight(join.right, map);

            // we want the right side to be less than the left
            if (rightDepth > leftDepth) {
                // switch
                String leftAlias = join.leftAlias;
                String leftKey = join.leftKey;
                Mondrian3Def.RelationOrJoin left = join.left;
                join.leftAlias = join.rightAlias;
                join.leftKey = join.rightKey;
                join.left = join.right;
                join.rightAlias = leftAlias;
                join.rightKey = leftKey;
                join.right = left;
            }
            // Does not currently matter which is returned because currently we
            // only support structures where the left and right depth values
            // form an inclusive subset of depth values, that is, any
            // node with a depth value between the left or right values is
            // a child of this current join.
            return leftDepth;

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Transforms so that all joins have a table as their left child and either
     * a table of child join on the right.
     *
     * @param relation Relation
     */
    private static void topToBottom(Mondrian3Def.RelationOrJoin relation) {
        if (relation instanceof Mondrian3Def.Table) {
            // nothing

        } else if (relation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relation;

            while (join.left instanceof Mondrian3Def.Join) {
                Mondrian3Def.Join jleft = (Mondrian3Def.Join) join.left;

                join.right =
                    new Mondrian3Def.Join(
                        join.leftAlias,
                        join.leftKey,
                        jleft.right,
                        join.rightAlias,
                        join.rightKey,
                        join.right);

                join.left = jleft.left;

                join.rightAlias = jleft.rightAlias;
                join.rightKey = jleft.rightKey;
                join.leftAlias = jleft.leftAlias;
                join.leftKey = jleft.leftKey;
            }
        }
    }

    /**
     * Copies a {@link mondrian.olap.Mondrian3Def.RelationOrJoin}.
     *
     * @param relation Relation
     */
    private static Mondrian3Def.RelationOrJoin copy(
        Mondrian3Def.RelationOrJoin relation)
    {
        if (relation instanceof Mondrian3Def.Table) {
            Mondrian3Def.Table table = (Mondrian3Def.Table) relation;
            return new Mondrian3Def.Table(table);

        } else if (relation instanceof Mondrian3Def.InlineTable) {
            Mondrian3Def.InlineTable table =
                (Mondrian3Def.InlineTable) relation;
            return new Mondrian3Def.InlineTable(table);

        } else if (relation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relation;

            Mondrian3Def.RelationOrJoin left = copy(join.left);
            Mondrian3Def.RelationOrJoin right = copy(join.right);

            return new Mondrian3Def.Join(
                join.leftAlias, join.leftKey, left,
                join.rightAlias, join.rightKey, right);

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Takes a relation in canonical form and snips off the
     * the tables with the given tableName (or table alias). The matching table
     * only appears once in the relation.
     *
     * @param relation Relation
     * @param tableName Table name
     */
    private static Mondrian3Def.RelationOrJoin snip(
        Mondrian3Def.RelationOrJoin relation,
        String tableName)
    {
        Util.deprecated("unused?", false);
        if (relation instanceof Mondrian3Def.Table) {
            Mondrian3Def.Table table = (Mondrian3Def.Table) relation;
            // Return null if the table's name or alias matches tableName
            return ((table.alias != null) && table.alias.equals(tableName))
                ? null
                : (table.name.equals(tableName) ? null : table);

        } else if (relation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relation;

            // snip left
            Mondrian3Def.RelationOrJoin left = snip(join.left, tableName);
            if (left == null) {
                // left got snipped so return the right
                // (the join is no longer a join).
                return join.right;

            } else {
                // whatever happened on the left, save it
                join.left = left;

                // snip right
                Mondrian3Def.RelationOrJoin right = snip(join.right, tableName);
                if (right == null) {
                    // right got snipped so return the left.
                    return join.left;

                } else {
                    // save the right, join still has right and left children
                    // so return it.
                    join.right = right;
                    return join;
                }
            }


        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Extends this 'leg' of the star by adding <code>relation</code>
     * joined by <code>joinCondition</code>. If the same expression is
     * already present, does not create it again. Stores the unaliased
     * table names to RolapStar.Table mapping associated with the
     * input <code>cube</code>.
     */
    RolapStar.Table addJoin(
        RolapStar.Table table,
        RolapSchemaLoader.PhysSchemaBuilder physSchemaBuilder,
        Mondrian3Def.RelationOrJoin relationOrJoin,
        RolapSchema.PhysHop joinCondition)
    {
        Util.deprecated("move this to PhysSchmaBuilder?", false);
        if (relationOrJoin instanceof Mondrian3Def.Relation) {
            final Mondrian3Def.Relation relation =
                (Mondrian3Def.Relation) relationOrJoin;
            final RolapSchema.PhysRelation physRelation =
                toPhysRelation(relation);
            RolapStar.Table starTable =
                table.findChild(joinCondition, true);
            assert starTable != null;
            return starTable;
        } else if (relationOrJoin instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relationOrJoin;
            RolapStar.Table leftTable =
                addJoin(table, physSchemaBuilder, join.left, joinCondition);
            String leftAlias = join.leftAlias;
            if (leftAlias == null) {
                // REVIEW: is cast to Relation valid?
                leftAlias = ((Mondrian3Def.Relation) join.left).getAlias();
                if (leftAlias == null) {
                    throw Util.newError(
                        "missing leftKeyAlias in " + relationOrJoin);
                }
            }
            assert leftTable.findAncestor(leftAlias) == leftTable;
            // switch to uniquified alias
            leftAlias = leftTable.getAlias();

            String rightAlias = join.rightAlias;
            if (rightAlias == null) {
                // the right relation of a join may be a join
                // if so, we need to use the right relation join's
                // left relation's alias.
                if (join.right instanceof Mondrian3Def.Join) {
                    Mondrian3Def.Join joinright =
                        (Mondrian3Def.Join) join.right;
                    // REVIEW: is cast to Relation valid?
                    rightAlias =
                        ((Mondrian3Def.Relation) joinright.left)
                            .getAlias();
                } else {
                    // REVIEW: is cast to Relation valid?
                    rightAlias =
                        ((Mondrian3Def.Relation) join.right)
                            .getAlias();
                }
                if (rightAlias == null) {
                    throw Util.newError(
                        "missing rightKeyAlias in " + relationOrJoin);
                }
            }
            RolapStar.Condition joinCondition2 =
                new RolapStar.Condition(
                    physSchemaBuilder.getPhysRelation(leftAlias, true)
                        .getColumn(
                            join.leftKey,
                            true),
                    physSchemaBuilder.getPhysRelation(rightAlias, true)
                        .getColumn(
                            join.rightKey,
                            true));
            return addJoin(
                leftTable, physSchemaBuilder, join.right, joinCondition);

        } else {
            throw Util.newInternal("bad relation type " + relationOrJoin);
        }
    }

    /**
     * Converts an xml relation to a physical table, creating if necessary.
     * For legacy schema support.
     *
     * @param xmlRelation XML relation
     * @return Physical table
     */
    RolapSchema.PhysRelation toPhysRelation2(
        final Mondrian3Def.Relation xmlRelation)
    {
        RolapSchema.PhysSchema physSchema = physSchemaConverter.physSchema;
        final String alias = xmlRelation.getAlias();
        RolapSchema.PhysRelation physRelation =
            physSchema.tablesByName.get(alias);
        if (physRelation == null) {
            if (xmlRelation instanceof Mondrian3Def.Table) {
                Mondrian3Def.Table xmlTable =
                    (Mondrian3Def.Table) xmlRelation;
                final RolapSchema.PhysTable physTable =
                    new RolapSchema.PhysTable(
                        physSchema,
                        xmlTable.schema,
                        xmlTable.name,
                        alias,
                        xmlTable.getHintMap());
                // Read columns from JDBC.
                physTable.ensurePopulated(
                    loader,
                    xmlTable);
                physRelation = physTable;
            } else if (xmlRelation instanceof Mondrian3Def.InlineTable) {
                final Mondrian3Def.InlineTable xmlInlineTable =
                    (Mondrian3Def.InlineTable) xmlRelation;
                RolapSchema.PhysInlineTable physInlineTable =
                    new RolapSchema.PhysInlineTable(
                        physSchema,
                        alias);
                for (Mondrian3Def.RealOrCalcColumnDef columnDef
                    : xmlInlineTable.columnDefs.array)
                {
                    physInlineTable.addColumn(
                        new RolapSchema.PhysRealColumn(
                            physInlineTable,
                            columnDef.name,
                            Dialect.Datatype.valueOf(columnDef.type),
                            null,
                            4));
                }
                // We must define a Key element. This wasn't present in
                // 3.4 schemas, so by default we use the first column.
                assert physInlineTable.getTotalColumnSize() > 0
                    : "Inline tables must have at least one column to work properly in Mondrian 4+.";
                physInlineTable.lookupKey(
                    Collections.singletonList(
                        physInlineTable.columnsByName
                            .values().iterator().next()),
                    true);
                final int columnCount =
                    physInlineTable.columnsByName.size();
                for (Mondrian3Def.Row row : xmlInlineTable.rows.array) {
                    String[] values = new String[columnCount];
                    for (Mondrian3Def.Value value : row.values) {
                        int columnOrdinal = 0;
                        for (String columnName
                            : physInlineTable.columnsByName.keySet())
                        {
                            if (columnName.equals(value.column)) {
                                break;
                            }
                            ++columnOrdinal;
                        }
                        if (columnOrdinal >= columnCount) {
                            throw Util.newError(
                                "Unknown column '" + value.column + "'");
                        }
                        values[columnOrdinal] = value.cdata;
                    }
                    physInlineTable.rowList.add(values);
                }
                if (true) {
                    final RolapSchema.PhysView physView =
                        RolapUtil.convertInlineTableToRelation(
                            physInlineTable, physSchema.dialect);
                    physView.ensurePopulated(
                        loader,
                        xmlInlineTable);
                    physRelation = physView;
                } else {
                    physRelation = physInlineTable;
                }
            } else if (xmlRelation instanceof Mondrian3Def.View) {
                final Mondrian3Def.View xmlView =
                    (Mondrian3Def.View) xmlRelation;
                final Mondrian3Def.SQL sql =
                    Mondrian3Def.SQL.choose(
                        xmlView.selects,
                        schema.getDialect());
                final RolapSchema.PhysView physView =
                    new RolapSchema.PhysView(
                        physSchema, alias, getText(sql));
                physView.ensurePopulated(
                    loader,
                    xmlView);
                physRelation = physView;
            } else {
                throw Util.needToImplement(
                    "translate xml table to phys table for table type"
                    + xmlRelation.getClass());
            }
            physSchema.tablesByName.put(alias, physRelation);
        }
        assert physRelation.getSchema() == physSchema;
        return physRelation;
    }

    static String getText(Mondrian3Def.SQL sql) {
        final StringBuilder buf = new StringBuilder();
        for (NodeDef child : sql.children) {
            if (child instanceof TextDef) {
                TextDef textDef = (TextDef) child;
                buf.append(textDef.s);
            }
        }
        return buf.toString();
    }

    /**
     * Converts an xml relation to a physical table, creating if necessary.
     * For legacy schema support.
     *
     * @param xmlLegacyRelation XML relation
     * @return Physical table
     */
    RolapSchema.PhysRelation toPhysRelation(
        final Mondrian3Def.Relation xmlLegacyRelation)
    {
        final String alias = xmlLegacyRelation.getAlias();
        RolapSchema.PhysRelation physRelation =
            physSchemaConverter.physSchema.tablesByName.get(alias);
        MondrianDef.Relation xmlRelation;
        if (physRelation == null) {
            if (xmlLegacyRelation instanceof Mondrian3Def.Table) {
                Mondrian3Def.Table xmlLegacyTable =
                    (Mondrian3Def.Table) xmlLegacyRelation;
                final RolapSchema.PhysTable physTable =
                    new RolapSchema.PhysTable(
                        physSchemaConverter.physSchema,
                        xmlLegacyTable.schema,
                        xmlLegacyTable.name,
                        alias,
                        buildHintMap(
                            xmlLegacyTable.tableHints));
                // Read columns from JDBC.
                physTable.ensurePopulated(
                    loader,
                    xmlLegacyTable);
                physRelation = physTable;

                MondrianDef.Table xmlTable = convert(xmlLegacyTable);
                xmlRelation = xmlTable;
            } else if (xmlLegacyRelation instanceof Mondrian3Def.InlineTable) {
                final Mondrian3Def.InlineTable xmlLegacyInlineTable =
                    (Mondrian3Def.InlineTable) xmlLegacyRelation;
                RolapSchema.PhysInlineTable physInlineTable =
                    new RolapSchema.PhysInlineTable(
                        physSchemaConverter.physSchema,
                        alias);
                for (Mondrian3Def.RealOrCalcColumnDef columnDef
                    : xmlLegacyInlineTable.columnDefs.array)
                {
                    physInlineTable.addColumn(
                        new RolapSchema.PhysRealColumn(
                            physInlineTable,
                            columnDef.name,
                            Dialect.Datatype.valueOf(columnDef.type),
                            null,
                            -1)); // TODO: jdbcColumn.getColumnSize()
                }
                final int columnCount =
                    physInlineTable.columnsByName.size();
                // We must define a Key element. This wasn't present in
                // 3.4 schemas, so by default we use the first column.
                assert columnCount > 0
                    : "Inline tables must have at least one column to work properly in Mondrian 4+.";
                physInlineTable.lookupKey(
                    Collections.singletonList(
                        physInlineTable.columnsByName
                            .values().iterator().next()),
                    true);
                for (Mondrian3Def.Row row : xmlLegacyInlineTable.rows.array) {
                    String[] values = new String[columnCount];
                    for (Mondrian3Def.Value value : row.values) {
                        int columnOrdinal = 0;
                        for (String columnName
                            : physInlineTable.columnsByName.keySet())
                        {
                            if (columnName.equals(value.column)) {
                                break;
                            }
                            ++columnOrdinal;
                        }
                        if (columnOrdinal >= columnCount) {
                            throw Util.newError(
                                "Unknown column '" + value.column + "'");
                        }
                        values[columnOrdinal] = value.cdata;
                    }
                    physInlineTable.rowList.add(values);
                }
                if (Util.deprecated(false, false)) {
                    final RolapSchema.PhysView physView =
                        RolapUtil.convertInlineTableToRelation(
                            physInlineTable,
                            physSchemaConverter.physSchema.dialect);
                    physView.ensurePopulated(
                        loader,
                        xmlLegacyInlineTable);
                    physRelation = physView;
                } else {
                    physRelation = physInlineTable;
                }

                MondrianDef.InlineTable xmlInlineTable =
                    convert(xmlLegacyInlineTable);
                xmlRelation = xmlInlineTable;
            } else if (xmlLegacyRelation instanceof Mondrian3Def.View) {
                final Mondrian3Def.View xmlView =
                    (Mondrian3Def.View) xmlLegacyRelation;
                final Mondrian3Def.SQL sql =
                    Mondrian3Def.SQL.choose(
                        xmlView.selects,
                        schema.getDialect());
                final RolapSchema.PhysView physView =
                    new RolapSchema.PhysView(
                        physSchemaConverter.physSchema,
                        alias,
                        getText(sql));
                physView.ensurePopulated(
                    loader,
                    xmlView);
                physRelation = physView;

                MondrianDef.Query xmlQuery = new MondrianDef.Query();
                xmlQuery.alias = alias;
                MondrianDef.ExpressionView xmlExpressionView =
                    new MondrianDef.ExpressionView();
                xmlExpressionView.expressions = convert(xmlView.selects);
                xmlQuery.children.add(xmlExpressionView);
                xmlRelation = xmlQuery;
            } else {
                throw Util.needToImplement(
                    "translate xml table to phys table for table type"
                    + xmlLegacyRelation.getClass());
            }
            physSchemaConverter.physSchema.tablesByName.put(
                alias,
                physRelation);
            physSchemaConverter.xmlTables.put(alias, xmlRelation);
        }
        assert physRelation.getSchema() == physSchemaConverter.physSchema;
        return physRelation;
    }

    private MondrianDef.Table convert(Mondrian3Def.Table xmlLegacyTable) {
        MondrianDef.Table xmlTable = new MondrianDef.Table();
        xmlTable.name = xmlLegacyTable.name;
        xmlTable.alias = xmlLegacyTable.alias;
        if (xmlLegacyTable.filter != null) {
            throw Util.needToImplement(
                "translate xml table SQL filter for table '"
                + xmlLegacyTable + "'");
        }
        return xmlTable;
    }

    private MondrianDef.InlineTable convert(
        Mondrian3Def.InlineTable xmlLegacyInlineTable)
    {
        MondrianDef.InlineTable xmlInlineTable = new MondrianDef.InlineTable();
        List<MondrianDef.RealOrCalcColumnDef> xmlColumnDefs =
            xmlInlineTable.children.holder(new MondrianDef.ColumnDefs()).list();
        for (Mondrian3Def.RealOrCalcColumnDef xmlLegacyColumnDef
            : xmlLegacyInlineTable.columnDefs.array)
        {
            xmlColumnDefs.add(
                convert((Mondrian3Def.ColumnDef) xmlLegacyColumnDef));
        }

        // We must define a Key element. This wasn't present in
        // 3.4 schemas, so by default we use the first column.
        assert xmlColumnDefs.size() > 0
            : "Inline tables must have at least one column to work properly in Mondrian 4+.";
        final List<MondrianDef.Column> xmlKeys =
            xmlInlineTable.children.holder(new MondrianDef.Key()).list();
        final MondrianDef.Key key = new MondrianDef.Key();
        final MondrianDef.Column column = new MondrianDef.Column();
        column.name = xmlColumnDefs.get(0).name;
        column.table = xmlLegacyInlineTable.alias;
        key.array = new MondrianDef.Column[1];
        key.array[0] = column;
        key.name = "key$0";
        xmlKeys.add(column);

        List<MondrianDef.Row> xmlRows =
            xmlInlineTable.children.holder(new MondrianDef.Rows()).list();
        for (Mondrian3Def.Row xmlLegacyRow : xmlLegacyInlineTable.rows.array) {
            xmlRows.add(convert(xmlLegacyRow));
        }
        return xmlInlineTable;
    }

    private MondrianDef.Row convert(Mondrian3Def.Row xmlLegacyRow) {
        MondrianDef.Row xmlRow = new MondrianDef.Row();
        xmlRow.values = convert(xmlLegacyRow.values);
        return xmlRow;
    }

    private MondrianDef.Value[] convert(Mondrian3Def.Value[] xmlLegacyValues) {
        MondrianDef.Value[] xmlValues =
            new MondrianDef.Value[xmlLegacyValues.length];
        for (int i = 0; i < xmlValues.length; i++) {
            xmlValues[i] = convert(xmlLegacyValues[i]);
        }
        return xmlValues;
    }

    private MondrianDef.Value convert(Mondrian3Def.Value xmlLegacyValue) {
        MondrianDef.Value xmlValue = new MondrianDef.Value();
        xmlValue.column = xmlLegacyValue.column;
        xmlValue.cdata = xmlLegacyValue.cdata;
        return xmlValue;
    }

    private MondrianDef.ColumnDef convert(
        Mondrian3Def.ColumnDef xmlLegacyColumnDef)
    {
        MondrianDef.ColumnDef xmlColumnDef = new MondrianDef.ColumnDef();
        xmlColumnDef.name = xmlLegacyColumnDef.name;
        xmlColumnDef.type = xmlLegacyColumnDef.type;
        return xmlColumnDef;
    }


    private MondrianDef.SQL[] convert(Mondrian3Def.SQL[] xmlLegacySqls) {
        MondrianDef.SQL[] xmlSqls = new MondrianDef.SQL[xmlLegacySqls.length];
        for (int i = 0; i < xmlLegacySqls.length; i++) {
            xmlSqls[i] = convert(xmlLegacySqls[i]);
        }
        return xmlSqls;
    }

    private MondrianDef.SQL convert(Mondrian3Def.SQL xmlLegacySql) {
        MondrianDef.SQL xmlSql = new MondrianDef.SQL();
        xmlSql.dialect = xmlLegacySql.dialect;
        xmlSql.children = new NodeDef[] {
            new TextDef(xmlLegacySql.getCData())
        };
        return xmlSql;
    }

    private MondrianDef.ExpressionView convert(
        RolapSchema.PhysRelation relation,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName,
        Mondrian3Def.ExpressionView xmlLegacyExpressionView)
    {
        final Mondrian3Def.SQL[] xmlLegacySqls =
            xmlLegacyExpressionView.expressions;
        MondrianDef.SQL[] xmlSqls = new MondrianDef.SQL[xmlLegacySqls.length];
        for (int i = 0; i < xmlLegacySqls.length; i++) {
            xmlSqls[i] =
                convert(relation, relations, levelUniqueName, xmlLegacySqls[i]);
        }
        final MondrianDef.ExpressionView xmlExpressionView =
            new MondrianDef.ExpressionView();
        xmlExpressionView.expressions = xmlSqls;
        return xmlExpressionView;
    }

    private MondrianDef.SQL convert(
        RolapSchema.PhysRelation relation,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName,
        Mondrian3Def.SQL xmlLegacySql)
    {
        List<NodeDef> list =
            asdasd(
                relation,
                relations,
                levelUniqueName,
                xmlLegacySql);
        final MondrianDef.SQL xmlSql = new MondrianDef.SQL();
        xmlSql.children = list.toArray(new NodeDef[list.size()]);
        xmlSql.dialect = xmlLegacySql.dialect;
        return xmlSql;
    }

    private List<NodeDef> asdasd(
        RolapSchema.PhysRelation relation,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName,
        Mondrian3Def.SQL legacySql)
    {
        List<NodeDef> list = new ArrayList<NodeDef>();
        for (NodeDef legacyChild : legacySql.children) {
            if (legacyChild instanceof TextDef) {
                TextDef text = (TextDef) legacyChild;
                list.add(text);
            } else if (legacyChild instanceof Mondrian3Def.Column) {
                Mondrian3Def.Column legacyColumn =
                    (Mondrian3Def.Column) legacyChild;
                final RolapSchema.PhysRelation relation2;
                if (legacyColumn.table != null) {
                    Util.assertTrue(
                        relations.containsKey(legacyColumn.table));
                    relation2 = relations.get(legacyColumn.table);
                } else {
                    relation2 = relation;
                }
                final MondrianDef.Column column =
                    registerLevelColumn(
                        relation2,
                        legacyColumn.name,
                        relations,
                        levelUniqueName);
                list.add(column);
            } else {
                throw Util.newInternal(
                    "unexpected element in expression: "
                    + legacyChild.getName());
            }
        }
        return list;
    }

    public static Map<String, String> buildHintMap(
        Mondrian3Def.Hint[] hints)
    {
        if (hints == null || hints.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> hintMap = new HashMap<String, String>();
        for (Mondrian3Def.Hint hint : hints) {
            hintMap.put(hint.type, hint.cdata);
        }
        return hintMap;
    }

    /**
     * Finds the dimension of a cube that uses a given shared dimension.
     *
     * <p>If there is more than one, it is a user error. If there are none,
     * returns null.
     *
     * @param xmlCube XML cube definition
     * @param sharedDimName Name of shared dimension
     * @return Dimension in cube that uses given shared dimension
     */
    private static List<Mondrian3Def.DimensionUsage> lookupSharedDimension(
        Mondrian3Def.Cube xmlCube,
        final String sharedDimName)
    {
        List<Mondrian3Def.DimensionUsage> usageList =
            new ArrayList<Mondrian3Def.DimensionUsage>();
        for (Mondrian3Def.CubeDimension xmlCubeDimension : xmlCube.dimensions) {
            if (xmlCubeDimension instanceof Mondrian3Def.DimensionUsage) {
                final Mondrian3Def.DimensionUsage dimensionUsage =
                    (Mondrian3Def.DimensionUsage) xmlCubeDimension;
                if (dimensionUsage.source.equals(sharedDimName)) {
                    usageList.add(dimensionUsage);
                }
            }
        }
        return usageList;
    }

    public MondrianDef.Schema convertSchema(
        Mondrian3Def.Schema xmlLegacySchema)
    {
        MondrianDef.Schema xmlSchema = new MondrianDef.Schema();
        xmlSchema.name = xmlLegacySchema.name;
        xmlSchema.metamodelVersion =
            MondrianServer.forConnection(schema.getInternalConnection())
                .getVersion().getVersionString();
        xmlSchema.missingLink = "ignore";
        final List<LevelInfo> levelList = new ArrayList<LevelInfo>(); // unused
        for (Mondrian3Def.Dimension xmlLegacyDimension
            : xmlLegacySchema.dimensions)
        {
            xmlSchema.children.add(
                convertSharedDimension(
                    xmlLegacyDimension,
                    levelList));
        }
        for (Mondrian3Def.Cube xmlLegacyCube : xmlLegacySchema.cubes) {
            xmlSchema.children.add(
                convertCube(
                    xmlLegacySchema, xmlLegacyCube));
        }
        for (Mondrian3Def.VirtualCube xmlLegacyVirtualCube
            : xmlLegacySchema.virtualCubes)
        {
            xmlSchema.children.add(
                convertVirtualCube(
                    xmlLegacySchema,
                    xmlLegacyVirtualCube));
        }
        for (Mondrian3Def.NamedSet xmlLegacyNamedSet
            : xmlLegacySchema.namedSets)
        {
            xmlSchema.children.add(
                convertNamedSet(
                    xmlLegacyNamedSet));
        }
        for (Mondrian3Def.Parameter xmlLegacyParameter
            : xmlLegacySchema.parameters)
        {
            xmlSchema.children.add(
                convertParameter(
                    xmlLegacyParameter));
        }
        for (Mondrian3Def.Role xmlLegacyRole : xmlLegacySchema.roles) {
            xmlSchema.children.add(
                convertRole(xmlLegacyRole));
        }
        MondrianDef.PhysicalSchema xmlPhysicalSchema =
            physSchemaConverter.toDef(physSchemaConverter.physSchema);
        xmlSchema.children.add(0, xmlPhysicalSchema);
        convertAnnotations(
            xmlSchema.children,
            xmlLegacySchema.annotations);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(xmlSchema.toXML());
        }
        return xmlSchema;
    }

    private MondrianDef.Parameter convertParameter(
        Mondrian3Def.Parameter xmlLegacyParameter)
    {
        MondrianDef.Parameter xmlParameter = new MondrianDef.Parameter();
        xmlParameter.name = xmlLegacyParameter.name;
        xmlParameter.defaultValue = xmlLegacyParameter.defaultValue;
        xmlParameter.description = xmlLegacyParameter.description;
        xmlParameter.modifiable = xmlLegacyParameter.modifiable;
        xmlParameter.type = xmlLegacyParameter.type;
        return xmlParameter;
    }

    private MondrianDef.Cube convertVirtualCube(
        Mondrian3Def.Schema xmlLegacySchema,
        Mondrian3Def.VirtualCube xmlLegacyVirtualCube)
    {
        final MondrianDef.Cube xmlCube = new MondrianDef.Cube();
        xmlCube.name = xmlLegacyVirtualCube.name;
        xmlCube.cache = true;
        xmlCube.caption = xmlLegacyVirtualCube.caption;
        xmlCube.defaultMeasure = xmlLegacyVirtualCube.defaultMeasure;
        xmlCube.description = xmlLegacyVirtualCube.description;
        xmlCube.enabled = xmlLegacyVirtualCube.enabled;
        xmlCube.enableScenarios = false;
        xmlCube.visible = xmlLegacyVirtualCube.visible;

        convertAnnotations(
            xmlCube.children, xmlLegacyVirtualCube.annotations);

        Pair<Set<String>, Map<String, Info>> pair =
            buildInfoMap(
                xmlLegacySchema,
                xmlLegacyVirtualCube);
        final Set<String> dimensionNames = pair.left;
        final Map<String, Info> infoMap = pair.right;

        // For each base cube, create a measure group. Populate with measures
        // and link dimensions.
        final NamedList<MondrianDef.MeasureGroup> xmlMeasureGroups =
            xmlCube.children.holder(
                new MondrianDef.MeasureGroups()).list();
        for (Info info : infoMap.values()) {
            MondrianDef.MeasureGroup xmlMeasureGroup =
                new MondrianDef.MeasureGroup();
            info.xmlMeasureGroup = xmlMeasureGroup;
            xmlMeasureGroup.type = "fact";
            xmlMeasureGroup.ignoreUnrelatedDimensions =
                info.ignoreUnrelatedDimensions;
            xmlMeasureGroup.table = info.xmlLegacyCube.fact.getAlias();
            xmlMeasureGroup.name = info.cubeName;
            xmlMeasureGroups.add(xmlMeasureGroup);
        }

        // For each dimension name, build a list of things that dimension links
        // to.
        final Map<String, MondrianDef.Dimension> xmlDimensionMap =
            new LinkedHashMap<String, MondrianDef.Dimension>();
        for (Mondrian3Def.VirtualCubeDimension xmlVirtualCubeDimension
            : xmlLegacyVirtualCube.dimensions)
        {
            List<CubeInfo> cubeInfoList;
            if (xmlVirtualCubeDimension.cubeName == null) {
                cubeInfoList = new ArrayList<CubeInfo>();
                for (Info info : infoMap.values()) {
                    cubeInfoList.add(lookupCube(info.cubeName));
                }
            } else {
                cubeInfoList = Collections.singletonList(
                    lookupCube(xmlVirtualCubeDimension.cubeName));
            }
            int nonJoinCount = 0;
            for (CubeInfo cubeInfo : cubeInfoList) {
                final Mondrian3Def.CubeDimension xmlLegacyCubeDimension =
                    lookupDimension(
                        cubeInfo.xmlLegacyCube, xmlVirtualCubeDimension.name);
                List<LevelInfo> levelList = new ArrayList<LevelInfo>();
                if (xmlLegacyCubeDimension == null) {
                    if (xmlVirtualCubeDimension.cubeName == null) {
                        // Ignore non-joining base cubes.
                        ++nonJoinCount;
                        continue;
                    } else {
                        throw Util.newError(
                            "Dimension '" + xmlVirtualCubeDimension.name
                            + "' not found in base cube '"
                            + cubeInfo.xmlLegacyCube.name + "'");
                    }
                }
                final MondrianDef.Dimension xmlDimension =
                    convertCubeDimension(
                        infoMap,
                        xmlLegacyVirtualCube.name,
                        xmlDimensionMap,
                        cubeInfo.fact,
                        cubeInfo.xmlFact,
                        xmlLegacyCubeDimension,
                        xmlVirtualCubeDimension.visible,
                        xmlLegacySchema,
                        levelList);
                cubeInfo.dimensionKeys.put(
                    xmlDimension.name, xmlLegacyCubeDimension.foreignKey);
            }

            if (nonJoinCount == cubeInfoList.size()) {
                loader.getHandler().error(
                    "Virtual cube dimension must join to at least one cube: "
                    + "dimension '"
                    + xmlVirtualCubeDimension.name
                    + "' in cube '"
                    + xmlLegacyVirtualCube.name
                    + "'",
                    xmlVirtualCubeDimension,
                    null);
            }
        }

        xmlCube.children.holder(new MondrianDef.Dimensions())
            .list().addAll(xmlDimensionMap.values());

        // Create measures, looking up measures in existing cubes.
        final Set<String> measureNames = new HashSet<String>();
        for (Mondrian3Def.VirtualCubeMeasure xmlLegacyMeasure
            : xmlLegacyVirtualCube.measures)
        {
            convertVirtualCubeMeasure(
                xmlCube, measureNames, infoMap, xmlLegacyMeasure);
        }

        for (Info info : infoMap.values()) {
            convertMeasureLinks(
                info.xmlLegacyCube,
                info.xmlMeasureGroup,
                cubeInfoMap.get(info.cubeName).dimensionKeys);
        }

        final NamedList<MondrianDef.CalculatedMember> xmlCalcMembers =
            xmlCube.children.holder(new MondrianDef.CalculatedMembers()).list();
        final NamedList<MondrianDef.NamedSet> xmlNamedSets =
            xmlCube.children.holder(new MondrianDef.NamedSets()).list();

        // Convert calculated members and named sets defined in the virtual
        // cube.
        for (Mondrian3Def.CalculatedMember xmlLegacyCalcMember
            : xmlLegacyVirtualCube.calculatedMembers)
        {
            xmlCalcMembers.add(
                convertCalculatedMember(xmlLegacyCalcMember));
        }
        for (Mondrian3Def.NamedSet xmlLegacyNamedSet
            : xmlLegacyVirtualCube.namedSets)
        {
            xmlNamedSets.add(
                convertNamedSet(xmlLegacyNamedSet));
        }

        return xmlCube;
    }

    private void convertVirtualCubeMeasure(
        MondrianDef.Cube xmlCube,
        Set<String> calcMeasureNames,
        Map<String, Info> infoMap,
        Mondrian3Def.VirtualCubeMeasure xmlLegacyMeasure)
    {
        final Info info = infoMap.get(xmlLegacyMeasure.cubeName);
        assert info != null;

        // First look for a base measure.
        Mondrian3Def.Measure xmlLegacyBaseMeasure =
            lookupMeasure(info.xmlLegacyCube, xmlLegacyMeasure.name);
        if (xmlLegacyBaseMeasure != null) {
            final MondrianDef.Measure xmlMeasure =
                convertMeasure(
                    cubeInfoMap.get(info.cubeName).fact,
                    xmlLegacyBaseMeasure);

            // VirtualCubeMeasure.visible overrides underlying measure's
            // visibility
            xmlMeasure.visible =
                RolapSchemaLoader.toBoolean(xmlLegacyMeasure.visible, true);

            // VirtualCubeMeasure's annotations override underlying measure's
            // annotations (but if underlying measure has annotations with
            // different names, they will survive).
            convertAnnotations(
                xmlMeasure.children, xmlLegacyMeasure.annotations);
            info.xmlMeasureGroup.children.holder(new MondrianDef.Measures())
                .list()
                .add(xmlMeasure);
            return;
        }

        // Now look for a calculated measure.
        final Mondrian3Def.CalculatedMember xmlLegacyBaseCalcMeasure =
            lookupCalcMember(info.xmlLegacyCube, xmlLegacyMeasure.name);
        if (xmlLegacyBaseCalcMeasure != null) {
            final MondrianDef.CalculatedMember xmlCalcMember =
                convertCalculatedMember(
                    xmlLegacyBaseCalcMeasure);
            xmlCalcMember.visible =
                RolapSchemaLoader.toBoolean(xmlLegacyMeasure.visible, true);
            xmlCube.children.holder(new MondrianDef.CalculatedMembers())
                .list()
                .add(xmlCalcMember);
            calcMeasureNames.add(xmlLegacyBaseCalcMeasure.name);
            return;
        }

        throw Util.newInternal(
            "could not find measure '" + xmlLegacyMeasure.name
            + "' in cube '" + xmlLegacyMeasure.cubeName + "'");
    }

    private Mondrian3Def.CalculatedMember lookupCalcMember(
        Mondrian3Def.Cube xmlCube, String name)
    {
        for (Mondrian3Def.CalculatedMember xmlCalcMember
            : xmlCube.calculatedMembers)
        {
            String uniqueName =
                "["
                + xmlCalcMember.dimension + "].["
                + xmlCalcMember.name
                + "]";
            if (uniqueName.equals(name)) {
                return xmlCalcMember;
            }
        }
        return null;
    }

    private Mondrian3Def.Measure lookupMeasure(
        Mondrian3Def.Cube xmlCube,
        String name)
    {
        for (Mondrian3Def.Measure xmlMeasure : xmlCube.measures) {
            if (("[Measures].[" + xmlMeasure.name + "]").equals(name)) {
                return xmlMeasure;
            }
        }
        return null;
    }

    private CubeInfo lookupCube(String cubeName) {
        return cubeInfoMap.get(cubeName);
    }

    private Mondrian3Def.CubeDimension lookupDimension(
        Mondrian3Def.Cube xmlCube,
        String name)
    {
        for (Mondrian3Def.CubeDimension xmlDimension : xmlCube.dimensions) {
            if (xmlDimension.name.equals(name)) {
                return xmlDimension;
            }
        }
        return null;
    }

    private void convertAnnotations(
        MondrianDef.Children<?> children,
        Mondrian3Def.Annotations annotations)
    {
        if (annotations == null) {
            return;
        }
        List<MondrianDef.Annotation> xmlAnnotations =
            children.holder(
                new MondrianDef.Annotations()).list();
        for (Mondrian3Def.Annotation xmlLegacyAnnotation : annotations.array) {
            MondrianDef.Annotation xmlAnnotation = new MondrianDef.Annotation();
            xmlAnnotation.name = xmlLegacyAnnotation.name;
            xmlAnnotation.cdata = xmlLegacyAnnotation.cdata;
            xmlAnnotations.add(xmlAnnotation);
        }
    }

    private MondrianDef.Dimension convertSharedDimension(
        Mondrian3Def.Dimension xmlLegacyDimension,
        List<LevelInfo> levelList)
    {
        return convertDimension(
            null,
            new HashMap<String, MondrianDef.Dimension>(),
            null,
            xmlLegacyDimension,
            xmlLegacyDimension.name,
            xmlLegacyDimension.visible,
            xmlLegacyDimension.description,
            levelList);
    }

    /**
     * Converts a dimension from legacy format to new format.
     *
     * <p>For example,
     * <pre>
     * &lt;Dimension name="Product"&gt;
     *   &lt;Hierarchy hasAll="true" primaryKey="product_id"
     *          primaryKeyTable="product"&gt;
     *     &lt;Join leftKey="product_class_id"
     *           rightKey="product_class_id"&gt;
     *       &lt;Table schema="sales" name="product" alias="p"/&gt;
     *       &lt;Table schema="sales" name="product_class" alias="pc"/&gt;
     *     &lt;/Join&gt;
     *     &lt;Level name="Product Family" table="product_class"
     *               column="product_family" uniqueMembers="true"/&gt;
     *     &lt;Level name="Product Department" table="product_class"
     *                column="product_department" uniqueMembers="false"/&gt;
     *     &lt;Level name="Product Category" table="product_class"
     *                column="product_category" uniqueMembers="false"/&gt;
     *     &lt;Level name="Product Subcategory" table="product_class"
     *              column="product_subcategory" uniqueMembers="false"/&gt;
     *     &lt;Level name="Brand Name" table="product" column="brand_name"
     *                uniqueMembers="false"/&gt;
     *     &lt;Level name="Product Name" table="product"
     *        column="product_name" ordinal="sku" uniqueMembers="true"/&gt;
     *   &lt;/Hierarchy&gt;
     * &lt;/Dimension&gt;
     * </pre>
     *
     * becomes
     *
     * <pre>
     * &lt;PhysicalSchema&gt;
     *   &lt;Table schema="sales" name="product" alias="p"/&gt;
     *     &lt;Key&gt;
     *       &lt;Column name="product_id"/&gt;
     *     &lt;/Key&gt;
     *   &lt;/Table&gt;
     *   &lt;Table schema="sales" name="product_class" alias="pc"/&gt;
     *     &lt;Key&gt;
     *       &lt;Column name="product_class_id"/&gt;
     *     &lt;/Key&gt;
     *   &lt;/Table&gt;
     *   &lt;Link source="product_class" target="product"&gt;
     *     &lt;Key&gt;
     *       &lt;Column name="product_class_id"/&gt;
     *     &lt;/Key&gt;
     *   &lt;/Link&gt;
     * &lt;/PhysicalSchema&gt;
     * &lt;Dimension name="Product" key="Id"&gt;
     *   &lt;Attributes&gt;
     *     &lt;Attribute name="Product Family"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_family"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Attribute&gt;
     *     &lt;Attribute name="Product Department"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_family"/&gt;
     *         &lt;Column table="pc" column="product_department"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Attribute&gt;
     *     &lt;Attribute name="Product Category"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_family"/&gt;
     *         &lt;Column table="pc" column="product_department"/&gt;
     *         &lt;Column table="pc" column="product_category"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Attribute&gt;
     *     &lt;Attribute name="Product Subcategory"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_family"/&gt;
     *         &lt;Column table="pc" column="product_department"/&gt;
     *         &lt;Column table="pc" column="product_category"/&gt;
     *         &lt;Column table="pc" column="product_subcategory"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Attribute&gt;
     *     &lt;Attribute name="Brand Name"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_family"/&gt;
     *         &lt;Column table="pc" column="product_department"/&gt;
     *         &lt;Column table="pc" column="product_category"/&gt;
     *         &lt;Column table="pc" column="product_subcategory"/&gt;
     *         &lt;Column table="p" column="brand_name"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Attribute&gt;
     *     &lt;Attribute name="Product Name"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="p" column="product_name"/&gt;
     *       &lt;/KeyExpression&gt;
     *       &lt;OrdinalExpression&gt;
     *         &lt;Column table="p" column="sku"/&gt;
     *       &lt;/OrdinalExpression&gt;
     *     &lt;/Attribute&gt;
     *   &lt;/Attributes&gt;
     *   &lt;Hierarchies&gt;
     *     &lt;Hierarchy hasAll="true"&gt;
     *       &lt;Level attribute="Product Family"/&gt;
     *       &lt;Level name="Product Department"/&gt;
     *       &lt;Level name="Product Category"/&gt;
     *       &lt;Level name="Product Subcategory"/&gt;
     *       &lt;Level name="Brand Name/&gt;
     *       &lt;Level name="Product Name/&gt;
     *     &lt;/Hierarchy&gt;
     *   &lt;/Hierarchies&gt;
     * &lt;/Dimension&gt;
     * </pre>
     *
     *
     *
     *
     * @param links List of fact tables to link dimension to, or null if the
     *              dimension is shared and should not be linked to anything
     * @param xmlDimensionMap Holds names of dimensions already created
     * @param xmlLegacyFact XML definition of fact table
     * @param xmlLegacyDimension Dimension in legacy format
     * @param dimensionName Dimension name; equals xmlLegacyDimension.name
     *                      unless dimension is a DimensionUsage that has been
     *                      re-aliased
     * @param visible whether the new dimension should be visible
     * @param description the description
     * @return converted dimension
     */
    public MondrianDef.Dimension convertDimension(
        List<Link> links,
        Map<String, MondrianDef.Dimension> xmlDimensionMap,
        Mondrian3Def.RelationOrJoin xmlLegacyFact,
        Mondrian3Def.Dimension xmlLegacyDimension,
        String dimensionName,
        boolean visible,
        String description,
        List<LevelInfo> levelList)
    {
        assert dimensionName != null;
        final MondrianDef.Dimension xmlDimension =
            new MondrianDef.Dimension();
        xmlDimension.name = dimensionName;
        xmlDimension.visible = visible;
        final List<MondrianDef.Hierarchy> hierarchyList =
            xmlDimension.children.holder(
                new MondrianDef.Hierarchies()).list();
        xmlDimension.caption = xmlLegacyDimension.caption;
        xmlDimension.description = description;
        if (xmlLegacyDimension.highCardinality) {
            LOGGER.warn(
                "Removing unsupported highCardinality attribute "
                + "from " + dimensionName);
        }
        Util.discard(xmlLegacyDimension.highCardinality);
        xmlDimension.type =
            xmlLegacyDimension.type == null
            || xmlLegacyDimension.type.equalsIgnoreCase("StandardDimension")
            ? null
            : xmlLegacyDimension.type.equalsIgnoreCase("TimeDimension")
            ? "TIME"
            : xmlLegacyDimension.type;
        xmlDimension.hanger = false;
        Util.discard(xmlLegacyDimension.usagePrefix);

        // Create key attribute with first hierarchy. Require that all
        // hierarchies are based on the same table and have same primary key.
        // FIXME: check this
        MondrianDef.Attribute[] xmlKeyAttributes = {null};

        for (int i = 0; i < xmlLegacyDimension.hierarchies.length; i++) {
            hierarchyList.add(
                convertHierarchy(
                    links,
                    xmlLegacyFact,
                    xmlLegacyDimension.hierarchies[i],
                    xmlDimension,
                    xmlKeyAttributes,
                    levelList));
        }
        final MondrianDef.Attribute xmlKeyAttribute = xmlKeyAttributes[0];
        if (xmlKeyAttribute != null) {
            xmlDimension.key = xmlKeyAttribute.name;
        }
        if (xmlDimensionMap.containsKey(dimensionName)) {
            // If we've already translated another dimension with this name
            // for this cube -- which only occurs in virtual cubes, and
            // hopefully only with dimensions that will appear as a single
            // dimension, linked to multiple fact tables -- then return the
            // XML from the first time we translated it.
            return xmlDimensionMap.get(dimensionName);
        } else {
            xmlDimensionMap.put(dimensionName, xmlDimension);
            physSchemaConverter.legacyMap.put(xmlDimension, xmlLegacyDimension);
            return xmlDimension;
        }
    }

    private MondrianDef.Hierarchy convertHierarchy(
        List<Link> links,
        Mondrian3Def.RelationOrJoin xmlLegacyFact,
        Mondrian3Def.Hierarchy xmlLegacyHierarchy,
        MondrianDef.Dimension xmlDimension,
        MondrianDef.Attribute[] xmlKeyAttributes,
        List<LevelInfo> levelList)
    {
        final MondrianDef.Hierarchy xmlHierarchy =
            new MondrianDef.Hierarchy();
        xmlHierarchy.allLevelName = xmlLegacyHierarchy.allLevelName;
        xmlHierarchy.allMemberCaption = xmlLegacyHierarchy.allMemberCaption;
        xmlHierarchy.allMemberName = xmlLegacyHierarchy.allMemberName;
        xmlHierarchy.defaultMember = xmlLegacyHierarchy.defaultMember;
        xmlHierarchy.hasAll = xmlLegacyHierarchy.hasAll;
        xmlHierarchy.visible = xmlLegacyHierarchy.visible;
        if (xmlLegacyHierarchy.name == null) {
            // Inherit name, caption, description from dimension only if
            // hierarchy name is null.
            xmlHierarchy.name = xmlDimension.name;
            xmlHierarchy.caption =
                first(xmlLegacyHierarchy.caption, xmlDimension.caption);
            xmlHierarchy.description =
                first(xmlLegacyHierarchy.description, xmlDimension.description);
        } else {
            xmlHierarchy.name = xmlLegacyHierarchy.name;
            xmlHierarchy.caption = xmlLegacyHierarchy.caption;
            xmlHierarchy.description = xmlLegacyHierarchy.description;
        }
        Util.discard(xmlLegacyHierarchy.memberReaderClass); // obsolete
        Util.discard(xmlLegacyHierarchy.memberReaderParameters); // obsolete

        convertAnnotations(
            xmlHierarchy.children,
            xmlLegacyHierarchy.annotations);

        Map<String, RolapSchema.PhysRelation> relations =
            new HashMap<String, RolapSchema.PhysRelation>();
        final Mondrian3Def.RelationOrJoin xmlLegacyRelation =
            xmlLegacyHierarchy.relation != null
                ? xmlLegacyHierarchy.relation
                : xmlLegacyFact;
        if (xmlLegacyRelation == null) {
            throw physSchemaConverter.getHandler().fatal(
                "Hierarchy in legacy-style schema must include a relation",
                xmlLegacyHierarchy,
                null);
        }

        final List<String> tableNames = new ArrayList<String>();
        gatherTableNames(xmlLegacyRelation, tableNames);
        if (xmlLegacyHierarchy.primaryKeyTable != null
            && !tableNames.contains(xmlLegacyHierarchy.primaryKeyTable))
        {
            physSchemaConverter.getHandler().error(
                "Table '" + xmlLegacyHierarchy.primaryKeyTable
                + "' not found",
                xmlLegacyHierarchy,
                "primaryKeyTable");
            physSchemaConverter.legacyMap.put(xmlHierarchy, xmlLegacyHierarchy);
            return xmlHierarchy;
        }

        RolapSchema.PhysRelation soleRelation = null;
        if (links != null) {
            for (Link link : links) {
                registerRelation(
                    link.fact,
                    xmlLegacyRelation,
                    link.foreignKey,
                    link.fact.getAlias(),
                    xmlLegacyHierarchy.primaryKey,
                    xmlLegacyHierarchy.primaryKeyTable,
                    false,
                    relations);
            }
            if (relations.size() == 1) {
                soleRelation = relations.values().iterator().next();
            }
        } else {
            // Shared dimension. Not linked to any fact tables.
            soleRelation = registerRelation(
                null,
                xmlLegacyRelation,
                null,
                null,
                xmlLegacyHierarchy.primaryKey,
                xmlLegacyHierarchy.primaryKeyTable,
                false,
                relations);
        }

        for (int i = 0; i < xmlLegacyHierarchy.levels.length; i++) {
            xmlHierarchy.children.add(
                convertLevel(
                    xmlDimension,
                    xmlHierarchy,
                    xmlLegacyHierarchy,
                    i,
                    xmlLegacyHierarchy.levels[i],
                    relations,
                    soleRelation,
                    links,
                    levelList));
        }

        badKey:
        if (xmlKeyAttributes[0] == null) {
            final MondrianDef.Attribute xmlKeyAttribute;
            if (xmlLegacyHierarchy.primaryKey != null) {
                xmlKeyAttribute = new MondrianDef.Attribute();
                xmlKeyAttribute.name = "$Id";
                xmlKeyAttribute.keyColumn = xmlLegacyHierarchy.primaryKey;
                RolapSchema.PhysRelation firstRelation = soleRelation;
                if (xmlLegacyHierarchy.primaryKeyTable != null) {
                    for (RolapSchema.PhysRelation relation : relations.values())
                    {
                        if (relation.getAlias().equals(
                                xmlLegacyHierarchy.primaryKeyTable))
                        {
                            firstRelation = relation;
                        }
                    }
                }
                if (firstRelation == null) {
                    loader.getHandler().error(
                        "could not find table for hierarchy's key",
                        xmlLegacyHierarchy,
                        "primaryKey");
                    break badKey;
                }
                xmlKeyAttribute.table = firstRelation.getAlias();
                xmlKeyAttribute.levelType = "Regular";
                xmlKeyAttribute.hasHierarchy = false;
                xmlDimension.children
                    .holder(
                        new MondrianDef.Attributes())
                    .list()
                    .add(xmlKeyAttribute);

                MondrianDef.Relation xmlRelation =
                    physSchemaConverter.xmlTables.get(firstRelation.getAlias());
                addKey(xmlRelation, xmlKeyAttribute.keyColumn);

                firstRelation.lookupKey(
                    Collections.singletonList(
                        firstRelation.getColumn(
                            xmlKeyAttribute.keyColumn, true)),
                    true);
            } else {
                // Dimension is degenerate. Assume that the key of the lowest
                // level of the hierarchy is the dimension's key.
                xmlKeyAttribute =
                    xmlDimension.getAttributes().get(
                        xmlHierarchy.getLevels().get(0).attribute);
            }
            xmlKeyAttributes[0] = xmlKeyAttribute;
        }

        physSchemaConverter.legacyMap.put(xmlHierarchy, xmlLegacyHierarchy);
        return xmlHierarchy;
    }

    private void addKey(
        MondrianDef.Relation xmlRelation, String... keyColumns)
    {
        if (xmlRelation instanceof MondrianDef.Table) {
            MondrianDef.Table xmlTable = (MondrianDef.Table) xmlRelation;
            for (String keyColumn : keyColumns) {
                MondrianDef.Column xmlColumn = new MondrianDef.Column();
                xmlColumn.name = keyColumn;
                xmlTable.children
                    .holder(
                        new MondrianDef.Key())
                    .list()
                    .add(xmlColumn);
            }
        }
    }

    private void gatherTableNames(
        Mondrian3Def.RelationOrJoin xmlRelation,
        List<String> tableNames)
    {
        if (xmlRelation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) xmlRelation;
            gatherTableNames(join.left, tableNames);
            gatherTableNames(join.right, tableNames);
        } else {
            Mondrian3Def.Relation relation =
                (Mondrian3Def.Relation) xmlRelation;
            tableNames.add(relation.getAlias());
        }
    }

    private RolapSchema.PhysRelation registerRelation(
        RolapSchema.PhysRelation leftRelation,
        Mondrian3Def.RelationOrJoin relationOrJoin,
        String leftKeyColumnName,
        String leftAlias, // REVIEW: needed?
        String rightKeyColumnName,
        String rightAlias,
        boolean hardLink,
        Map<String, RolapSchema.PhysRelation> relations)
    {
        assert leftKeyColumnName == null || leftRelation != null;
        assert relationOrJoin != null;
        final RolapSchema.PhysRelation rightRelation;
        final RolapSchema.PhysRelation midRelation;
        if (relationOrJoin instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relationOrJoin;
            midRelation =
                registerRelation(
                    leftRelation,
                    join.left,
                    leftKeyColumnName,
                    leftAlias,
                    rightKeyColumnName,
                    rightAlias,
                    false,
                    relations);
            rightRelation =
                registerRelation(
                    midRelation,
                    join.right,
                    join.leftKey,
                    join.leftAlias,
                    join.rightKey,
                    join.rightAlias,
                    true,
                    relations);
        } else {
            Mondrian3Def.Relation relation =
                (Mondrian3Def.Relation) relationOrJoin;
            // TODO: fail if alias exists
            midRelation = rightRelation = toPhysRelation(relation);
            if (rightRelation instanceof RolapSchema.PhysTable) {
                RolapSchema.PhysTable physTable =
                    (RolapSchema.PhysTable) rightRelation;
                physTable.ensurePopulated(
                    loader,
                    relation);
            }

            RolapSchema.PhysKey rightKey;
            if (rightKeyColumnName != null) {
                final RolapSchema.PhysColumn rightColumn =
                    rightRelation.getColumn(rightKeyColumnName, false);
                if (rightColumn == null) {
                    // REVIEW: user error?
                    throw new IllegalArgumentException();
                }
                final List<RolapSchema.PhysColumn> keyColumnList =
                    Collections.singletonList(rightColumn);
                rightKey = rightRelation.lookupKey(keyColumnList, true);
                if (rightAlias != null && relation.getAlias() != null) {
                    if (!rightAlias.equals(relation.getAlias())) {
                        throw new IllegalArgumentException(
                            "right alias " + rightAlias + " != table alias "
                            + relation.getAlias());
                    }
                }
            } else {
                rightKey = null;
            }
            relations.put(rightRelation.getAlias(), rightRelation);
            if (leftKeyColumnName != null) {
                final RolapSchema.PhysColumn leftColumn =
                    leftRelation.getColumn(leftKeyColumnName, false);
                if (leftColumn == null) {
                    // This is correctly an internal error. Validation
                    // should have given a user error before this point.
                    throw Util.newInternal(
                        "Relation " + leftRelation
                        + " does not contain column " + leftKeyColumnName);
                }
                final List<RolapSchema.PhysColumn> leftKeyColumnList =
                    Collections.singletonList(leftColumn);
                assert rightKey != null;
                physSchemaConverter.physSchema.addLink(
                    rightKey,
                    leftRelation,
                    leftKeyColumnList,
                    hardLink);
            }
        }

        if (rightKeyColumnName != null) {
            // Make sure right relation's key is the one we think we're
            // joining to. If not, since we don't support alternate
            // keys, we're hosed.
            final RolapSchema.PhysColumn column =
                midRelation.getColumn(rightKeyColumnName, false);
            if (column == null) {
                throw new IllegalArgumentException(
                    "right relation=" + rightRelation + ", right key="
                    + rightKeyColumnName); // REVIEW: user error?
            }
            final List<RolapSchema.PhysColumn> keyColumnList =
                Collections.singletonList(column);
            final RolapSchema.PhysKey midKey =
                midRelation.lookupKey(keyColumnList, true);
            // TODO: also create link from leftRelation to midRelation???
        }
        return rightRelation;
    }

    /**
     * Converts an XML level to new format.
     *
     * @param xmlLegacyHierarchy XML hierarchy in original format
     * @param ordinal Level ordinal
     * @param xmlLegacyLevel XML level in original format
     * @param relations Map of relations by alias
     * @param relation Relation
     * @param links Required links from fact table to dimension
     * @return Converted level
     */
    private MondrianDef.Level convertLevel(
        MondrianDef.Dimension xmlDimension,
        MondrianDef.Hierarchy xmlHierarchy,
        Mondrian3Def.Hierarchy xmlLegacyHierarchy,
        int ordinal,
        Mondrian3Def.Level xmlLegacyLevel,
        Map<String, RolapSchema.PhysRelation> relations,
        RolapSchema.PhysRelation relation,
        List<Link> links,
        List<LevelInfo> levelList)
    {
        NamedList<MondrianDef.Attribute> attributeList =
            xmlDimension.children.holder(
                new MondrianDef.Attributes()).list();

        final MondrianDef.Attribute xmlAttribute = new MondrianDef.Attribute();
        attributeList.add(xmlAttribute);
        xmlAttribute.approxRowCount = xmlLegacyLevel.approxRowCount;
        xmlAttribute.caption = xmlLegacyLevel.caption;
        xmlAttribute.captionColumn = xmlLegacyLevel.captionColumn; // ??
        xmlAttribute.keyColumn = xmlLegacyLevel.column;
        xmlAttribute.name =
            Util.uniquify(
                xmlLegacyLevel.name,
                Integer.MAX_VALUE,
                new ArrayList<String>(names(attributeList)));

        convertMemberFormatter(
            xmlLegacyLevel.formatter,
            xmlLegacyLevel.memberFormatter,
            xmlAttribute.children);

        final MondrianDef.Level xmlLevel = new MondrianDef.Level();
        xmlLevel.caption = null; // attribute has caption
        if (xmlLegacyLevel.table != null) {
            relation =
                physSchemaConverter.getPhysRelation(xmlLegacyLevel.table, true);
        }

        xmlLevel.name = xmlLegacyLevel.name;
        xmlLevel.attribute = xmlAttribute.name;
        xmlLevel.visible = xmlLegacyLevel.visible;
        xmlAttribute.table = relation == null ? null : relation.getAlias();
        xmlLevel.hideMemberIf = xmlLegacyLevel.hideMemberIf;
        xmlLevel.description = xmlLegacyLevel.description;
        xmlLevel.caption = xmlLegacyLevel.caption;

        convertAnnotations(
            xmlLevel.children,
            xmlLegacyLevel.annotations);

        xmlAttribute.levelType = xmlLegacyLevel.levelType;
        xmlAttribute.hasHierarchy = false;

        final String levelUniqueName = null;

        // key
        final MondrianDef.Column keyColumn =
            convertColumnOrExpr(
                relation,
                xmlLegacyLevel.keyExp,
                xmlLegacyLevel.column,
                relations,
                levelUniqueName,
                MondrianDef.Key.class,
                xmlAttribute.children);
        xmlAttribute.keyColumn = null;

        levelList.add(
            new LevelInfo(
                xmlDimension.name,
                xmlHierarchy.name,
                xmlLevel.name,
                keyColumn.table,
                keyColumn.name));

        if (!xmlLegacyLevel.uniqueMembers
            && ordinal > 0)
        {
            MondrianDef.Attribute prevAttribute =
                attributeList.get(attributeList.size() - 2);
            xmlAttribute.children.holder(new MondrianDef.Key()).list().addAll(
                0, prevAttribute.getKey().list());
        }

        // name
        MondrianDef.Column n =
            convertColumnOrExpr(
                relation,
                xmlLegacyLevel.nameExp,
                xmlLegacyLevel.nameColumn,
                relations,
                levelUniqueName,
                MondrianDef.Name.class,
                xmlAttribute.children);
        xmlAttribute.nameColumn = null;

        // name is not optional if key is composite
        if (xmlAttribute.getName_() == null
            && xmlAttribute.getKey().list().size() > 1)
        {
            xmlAttribute.children.holder(new MondrianDef.Name()).list().add(
                Util.last(xmlAttribute.getKey().list()));
        }

        // caption
        convertColumnOrExpr(
            relation,
            xmlLegacyLevel.captionExp,
            xmlLegacyLevel.captionColumn,
            relations,
            levelUniqueName,
            MondrianDef.Caption.class,
            xmlAttribute.children);
        xmlAttribute.captionColumn = null;

        // ordinal
        MondrianDef.Column x =
            convertColumnOrExpr(
                relation,
                xmlLegacyLevel.ordinalExp,
                xmlLegacyLevel.ordinalColumn,
                relations,
                levelUniqueName,
                MondrianDef.OrderBy.class,
                xmlAttribute.children);
        xmlAttribute.orderByColumn = null;

        // In mondrian-3, ordinal defaulted to key, whereas
        // in mondrian-4, ordinal defaults to name. If name is not specified,
        // explicitly specify OrderBy to preserve mondrian-3 semantics.
        if (x == null && n != null) {
            xmlAttribute.children.holder(new MondrianDef.OrderBy())
                .list()
                .addAll(xmlAttribute.getKey().list());
        }

        // parent becomes a new attribute
        if (xmlLegacyLevel.parentColumn != null
            || xmlLegacyLevel.parentExp != null)
        {
            MondrianDef.Attribute xmlParentAttribute =
                new MondrianDef.Attribute();
            attributeList.add(attributeList.size() - 1, xmlParentAttribute);
            xmlParentAttribute.name = xmlAttribute.name + "$Parent";
            xmlParentAttribute.levelType = "Regular";
            convertColumnOrExpr(
                relation,
                xmlLegacyLevel.parentExp,
                xmlLegacyLevel.parentColumn,
                relations,
                levelUniqueName,
                MondrianDef.Key.class,
                xmlParentAttribute.children);
            xmlLevel.parentAttribute = xmlParentAttribute.name;
            xmlLevel.nullParentValue = xmlLegacyLevel.nullParentValue;
            xmlParentAttribute.hasHierarchy = false;

            // Register closure table in physical schema, and link to fact
            // table.
            if (xmlLegacyLevel.closure != null) {
                final RolapSchema.PhysRelation physClosureTable =
                    toPhysRelation2(
                        xmlLegacyLevel.closure.table);
                // Create a key for the closure table. This is a slight fib,
                // since this does columns not uniquely identify rows in the
                // table. But it is consistent with how we use keys in dimension
                // tables: the key is what we join to from the fact table.
                RolapSchema.PhysKey key =
                    physClosureTable.addKey(
                        "primary",
                        Collections.singletonList(
                            physClosureTable.getColumn(
                                xmlLegacyLevel.closure.childColumn, true)));
                // add link between closure and dim relation
                physSchemaConverter.physSchema.addLink(
                    relation.lookupKey(
                        Collections.singletonList(
                            relation.getColumn(keyColumn.name, false)), false),
                    physClosureTable,
                    Collections.singletonList(
                        physClosureTable.getColumn(
                            xmlLegacyLevel.closure.childColumn, true)), false);
                if (links != null) {
                    for (Link link : links) {
                        physSchemaConverter.physSchema.addLink(
                            key,
                            link.fact,
                            Collections.singletonList(
                                link.fact.getColumn(link.foreignKey, true)),
                            false);
                    }
                }

                // Convert the Closure element.
                MondrianDef.Closure closure =
                    new MondrianDef.Closure();
                closure.childColumn = xmlLegacyLevel.closure.childColumn;
                closure.parentColumn = xmlLegacyLevel.closure.parentColumn;
                closure.distanceColumn = "distance";
                closure.table = xmlLegacyLevel.closure.table.getAlias();

                physSchemaConverter
                    .legacyMap.put(closure, xmlLegacyLevel.closure);

                // Now add the closure to the level.
                xmlLevel.children.add(closure);
            }
        }

        for (int i = 0; i < xmlLegacyLevel.properties.length; i++) {
            Mondrian3Def.Property xmlLegacyProperty =
                xmlLegacyLevel.properties[i];

            MondrianDef.Attribute xmlPropertyAttribute =
                new MondrianDef.Attribute();
            xmlPropertyAttribute.name =
                xmlAttribute.name + "$" + xmlLegacyProperty.name;
            xmlPropertyAttribute.datatype = xmlLegacyProperty.type;
            xmlPropertyAttribute.levelType = "Regular";
            xmlPropertyAttribute.hasHierarchy = false;
            convertColumnOrExpr(
                relation,
                null,
                xmlLegacyProperty.column,
                relations,
                levelUniqueName,
                MondrianDef.Key.class,
                xmlPropertyAttribute.children);

            attributeList.add(xmlPropertyAttribute);

            final MondrianDef.Property xmlProperty =
                convertProperty(
                    xmlLegacyProperty,
                    xmlPropertyAttribute.name);

            xmlAttribute.children.add(xmlProperty);
        }

        xmlAttribute.datatype = xmlLegacyLevel.type;

        boolean uniqueMembers =
            xmlLegacyLevel.uniqueMembers == null
                ? (ordinal == 0)
                : xmlLegacyLevel.uniqueMembers;

        physSchemaConverter.legacyMap.put(xmlLevel, xmlLegacyLevel);
        return xmlLevel;
    }

    /** Returns the list of names in a list of named elements. */
    private <T extends MondrianDef.NamedElement> List<String> names(
        final NamedList<T> elementList)
    {
        return new AbstractList<String>() {
            public String get(int index) {
                return elementList.get(index).getNameAttribute();
            }

            public int size() {
                return elementList.size();
            }
        };
    }

    private void convertMemberFormatter(
        String formatter,
        Mondrian3Def.MemberFormatter xmlLegacyMemberFormatter,
        MondrianDef.Children<MondrianDef.AttributeElement> children)
    {
        if (formatter != null) {
            MondrianDef.MemberFormatter xmlMemberFormatter =
                new MondrianDef.MemberFormatter();
            xmlMemberFormatter.className = formatter;
            children.add(xmlMemberFormatter);
        } else if (xmlLegacyMemberFormatter != null) {
            MondrianDef.MemberFormatter xmlMemberFormatter =
                new MondrianDef.MemberFormatter();
            xmlMemberFormatter.className = xmlLegacyMemberFormatter.className;
            xmlMemberFormatter.script =
                convertScript(xmlLegacyMemberFormatter.script);
            children.add(xmlMemberFormatter);
        }
    }

    private MondrianDef.Property convertProperty(
        Mondrian3Def.Property xmlLegacyProperty,
        String sourceAttributeName)
    {
        final MondrianDef.Property xmlProperty =
            new MondrianDef.Property();
        xmlProperty.caption = xmlLegacyProperty.caption;
        xmlProperty.formatter = xmlLegacyProperty.formatter;
        if (xmlLegacyProperty.propertyFormatter != null) {
            xmlProperty.children.add(
                convertPropertyFormatter(xmlLegacyProperty.propertyFormatter));
        }
        xmlProperty.name = xmlLegacyProperty.name;
        xmlProperty.description = xmlLegacyProperty.description;
        xmlProperty.caption = xmlLegacyProperty.caption;
        xmlProperty.attribute = sourceAttributeName;
        return xmlProperty;
    }

    private MondrianDef.PropertyFormatter convertPropertyFormatter(
        Mondrian3Def.PropertyFormatter xmlLegacyPropertyFormatter)
    {
        if (xmlLegacyPropertyFormatter == null) {
            return null;
        }
        final MondrianDef.PropertyFormatter xmlPropertyFormatter =
            new MondrianDef.PropertyFormatter();
        xmlPropertyFormatter.className = xmlLegacyPropertyFormatter.className;
        xmlPropertyFormatter.script =
            convertScript(xmlLegacyPropertyFormatter.script);
        return xmlPropertyFormatter;
    }

    <T extends MondrianDef.Columns & MondrianDef.AttributeElement>
    MondrianDef.Column convertColumnOrExpr(
        RolapSchema.PhysRelation relation,
        Mondrian3Def.ExpressionView legacyExpression,
        final String columnName,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName,
        Class<T> keyClass,
        MondrianDef.Children<MondrianDef.AttributeElement> attributeChildren)
    {
        final MondrianDef.Column x =
            convertColumnOrExpr(
                relation,
                legacyExpression,
                columnName,
                relations,
                levelUniqueName);
        if (x != null) {
            try {
                T xmlColumns = keyClass.newInstance();
                xmlColumns.array = new MondrianDef.Column[] {x};
                attributeChildren.add(xmlColumns);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return x;
    }

    private MondrianDef.Column convertColumnOrExpr(
        RolapSchema.PhysRelation relation,
        Mondrian3Def.ExpressionView legacyExpression,
        String columnName,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName)
    {
        if (legacyExpression != null) {
            assert columnName == null;
            return convertExpr(
                relation,
                legacyExpression,
                relations,
                levelUniqueName);
        } else if (columnName != null) {
            return registerLevelColumn(
                relation,
                columnName,
                relations,
                levelUniqueName);
        } else {
            return null;
        }
    }

    private MondrianDef.Column convertExpr(
        RolapSchema.PhysRelation relation,
        Mondrian3Def.ExpressionView legacyExpression,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName)
    {
        final List<MondrianDef.SQL> xmlSqlList =
            new ArrayList<MondrianDef.SQL>();
        for (Mondrian3Def.SQL xmlLegacySql : legacyExpression.expressions) {
            List<NodeDef> list = asdasd(
                relation,
                relations,
                levelUniqueName,
                xmlLegacySql);
            if (list.size() == 1) {
                NodeDef def = list.get(0);
                if (def instanceof MondrianDef.Column) {
                    return (MondrianDef.Column) def;
                }
            }

            final MondrianDef.SQL xmlSql = new MondrianDef.SQL();
            xmlSql.children = list.toArray(new NodeDef[list.size()]);
            xmlSql.dialect = xmlLegacySql.dialect;
            xmlSqlList.add(xmlSql);
        }

        MondrianDef.ExpressionView xmlExpressionview =
            convert(
                relation,
                relations,
                levelUniqueName,
                legacyExpression);

        MondrianDef.Expression xmlExpression = optimize(xmlExpressionview);

        // Validate that the expression belongs to a unique relation.

        RolapSchema.PhysExpr measureExp =
            physSchemaConverter.toPhysExpr(relation, xmlExpression);

        RolapSchema.PhysColumn physColumn;
        if (measureExp instanceof RolapSchema.PhysColumn) {
            physColumn = (RolapSchema.PhysColumn) measureExp;
        } else {
            physColumn =
                physSchemaConverter.toPhysColumn(
                    measureExp, legacyExpression, xmlExpressionview, relation);
            if (physColumn == null) {
                // toPhysColumn could not convert, and posted error
                return null;
            }
        }

        if (physColumn instanceof RolapSchema.PhysCalcColumn) {
            MondrianDef.Table xmlTable =
                (MondrianDef.Table) physSchemaConverter.xmlTables.get(
                    physColumn.relation.getAlias());
            MondrianDef.CalculatedColumnDef xmlCalcColumnDef =
                new MondrianDef.CalculatedColumnDef();
            xmlCalcColumnDef.name = physColumn.name;
            xmlCalcColumnDef.expression = xmlExpression;
            xmlTable.children.holder(new MondrianDef.ColumnDefs()).list().add(
                xmlCalcColumnDef);
        }

        MondrianDef.Column xmlColumn = new MondrianDef.Column();
        xmlColumn.table = physColumn.relation.getAlias();
        xmlColumn.name = physColumn.name;
        return xmlColumn;
    }

    private MondrianDef.Expression optimize(
        MondrianDef.ExpressionView xmlExpressionView)
    {
        if (xmlExpressionView.expressions.length == 1) {
            final MondrianDef.SQL xmlExpression =
                xmlExpressionView.expressions[0];
            final NodeDef xmlChild = xmlExpression.children[0];
            if (xmlExpression.children.length == 1
                && xmlChild instanceof MondrianDef.Column)
            {
                return (MondrianDef.Column) xmlChild;
            }
        }
        return xmlExpressionView;
    }

    private MondrianDef.Column registerLevelColumn(
        RolapSchema.PhysRelation relation,
        final String columnName,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName)
    {
        assert relation != null;
        final MondrianDef.Column column = new MondrianDef.Column();
        column.name = columnName;
        column.table = relation.getAlias();
        return column;
    }

    private MondrianDef.Measure convertMeasure(
        RolapSchema.PhysRelation fact,
        Mondrian3Def.Measure xmlLegacyMeasure)
    {
        MondrianDef.Measure xmlMeasure = new MondrianDef.Measure();
        xmlMeasure.name = xmlLegacyMeasure.name;
        xmlMeasure.visible = xmlLegacyMeasure.visible;
        xmlMeasure.aggregator = xmlLegacyMeasure.aggregator;
        xmlMeasure.caption = xmlLegacyMeasure.caption;
        xmlMeasure.datatype = xmlLegacyMeasure.datatype;
        xmlMeasure.formatString = xmlLegacyMeasure.formatString;
        xmlMeasure.formatter = xmlLegacyMeasure.formatter;
        xmlMeasure.description = xmlLegacyMeasure.description;
        xmlMeasure.table = null;

        convertAnnotations(xmlMeasure.children, xmlLegacyMeasure.annotations);

        for (Mondrian3Def.CalculatedMemberProperty xmlLegacyMemberProperty
            : xmlLegacyMeasure.memberProperties)
        {
            // Skip MEMBER_ORDINAL. Clashes can occur when combining members
            // from different base cubes.
            if (!xmlLegacyMemberProperty.name.equals("MEMBER_ORDINAL")) {
                xmlMeasure.children.add(
                    convertMemberProperty(
                        xmlLegacyMemberProperty));
            }
        }

        if (xmlLegacyMeasure.cellFormatter != null) {
            xmlMeasure.children.add(
                convertCellFormatter(xmlLegacyMeasure.cellFormatter));
        }

        MondrianDef.Column column =
            convertColumnOrExpr(
                fact,
                xmlLegacyMeasure.measureExp,
                xmlLegacyMeasure.column,
                physSchemaConverter.physSchema.tablesByName,
                RolapSchemaLoader.MEASURES_LEVEL_NAME);
        if (column != null) {
            MondrianDef.Arguments xmlArguments = new MondrianDef.Arguments();
            xmlMeasure.children.add(xmlArguments);
            xmlArguments.array = new MondrianDef.Column[] {column};
        }
        xmlMeasure.column = null;
        return xmlMeasure;
    }

    private MondrianDef.CalculatedMember convertCalculatedMember(
        Mondrian3Def.CalculatedMember xmlLegacyCalcMember)
    {
        final MondrianDef.CalculatedMember xmlCalcMember =
            new MondrianDef.CalculatedMember();
        xmlCalcMember.caption = xmlLegacyCalcMember.caption;
        xmlCalcMember.description = xmlLegacyCalcMember.description;
        xmlCalcMember.formatString = xmlLegacyCalcMember.formatString;
        xmlCalcMember.formula = xmlLegacyCalcMember.formula;
        xmlCalcMember.dimension = xmlLegacyCalcMember.dimension;
        xmlCalcMember.hierarchy = xmlLegacyCalcMember.hierarchy;
        xmlCalcMember.parent = xmlLegacyCalcMember.parent;
        xmlCalcMember.name = xmlLegacyCalcMember.name;
        xmlCalcMember.visible = xmlLegacyCalcMember.visible;
        if (xmlLegacyCalcMember.formulaElement != null) {
            xmlCalcMember.children.add(
                convertFormula(xmlLegacyCalcMember.formulaElement));
        }
        if (xmlLegacyCalcMember.cellFormatter != null) {
            xmlCalcMember.children.add(
                convertCellFormatter(xmlLegacyCalcMember.cellFormatter));
        }
        convertAnnotations(
            xmlCalcMember.children,
            xmlLegacyCalcMember.annotations);
        convertCalcMemberProperties(
            xmlCalcMember.children,
            xmlLegacyCalcMember.memberProperties,
            true);
        return xmlCalcMember;
    }

    private void convertCalcMemberProperties(
        MondrianDef.Children<MondrianDef.CalculatedMemberElement> children,
        Mondrian3Def.CalculatedMemberProperty[] memberProperties,
        boolean skipOrdinal)
    {
        for (Mondrian3Def.CalculatedMemberProperty xmlLegacyMemberProperty
            : memberProperties)
        {
            if (!skipOrdinal
                || !xmlLegacyMemberProperty.name.equals("MEMBER_ORDINAL"))
            {
                children.add(
                    convertMemberProperty(
                        xmlLegacyMemberProperty));
            }
        }
    }

    private MondrianDef.CellFormatter convertCellFormatter(
        Mondrian3Def.CellFormatter xmlLegacyCellFormatter)
    {
        final MondrianDef.CellFormatter xmlCellFormatter =
            new MondrianDef.CellFormatter();
        xmlCellFormatter.className = xmlLegacyCellFormatter.className;
        xmlCellFormatter.script = convertScript(xmlLegacyCellFormatter.script);
        return xmlCellFormatter;
    }

    private MondrianDef.Script convertScript(
        Mondrian3Def.Script xmlLegacyScript)
    {
        if (xmlLegacyScript == null) {
            return null;
        }
        final MondrianDef.Script xmlScript = new MondrianDef.Script();
        xmlScript.cdata = xmlLegacyScript.cdata;
        xmlScript.language = xmlLegacyScript.language;
        return xmlScript;
    }

    private MondrianDef.CalculatedMemberElement convertFormula(
        Mondrian3Def.Formula xmlLegacyFormula)
    {
        final MondrianDef.Formula xmlFormula = new MondrianDef.Formula();
        xmlFormula.cdata = xmlLegacyFormula.cdata;
        return xmlFormula;
    }

    private MondrianDef.NamedSet convertNamedSet(
        Mondrian3Def.NamedSet xmlLegacyNamedSet)
    {
        final MondrianDef.NamedSet xmlNamedSet =
            new MondrianDef.NamedSet();
        xmlNamedSet.name = xmlLegacyNamedSet.name;
        xmlNamedSet.caption = xmlLegacyNamedSet.caption;
        xmlNamedSet.description = xmlLegacyNamedSet.description;
        xmlNamedSet.formula = xmlLegacyNamedSet.formula;
        if (xmlLegacyNamedSet.formulaElement != null) {
            xmlNamedSet.children.add(
                convertFormula(xmlLegacyNamedSet.formulaElement));
        }
        convertAnnotations(
            xmlNamedSet.children,
            xmlLegacyNamedSet.annotations);
        return xmlNamedSet;
    }

    private MondrianDef.CalculatedMemberProperty convertMemberProperty(
        Mondrian3Def.CalculatedMemberProperty xmlLegacyMemberProperty)
    {
        MondrianDef.CalculatedMemberProperty xmlCalcMemProp =
            new MondrianDef.CalculatedMemberProperty();
        xmlCalcMemProp.caption = xmlLegacyMemberProperty.caption;
        xmlCalcMemProp.description = xmlLegacyMemberProperty.description;
        xmlCalcMemProp.expression = xmlLegacyMemberProperty.expression;
        xmlCalcMemProp.name = xmlLegacyMemberProperty.name;
        xmlCalcMemProp.value = xmlLegacyMemberProperty.value;
        return xmlCalcMemProp;
    }

    private MondrianDef.Role convertRole(
        Mondrian3Def.Role xmlLegacyRole)
    {
        MondrianDef.Role xmlRole = new MondrianDef.Role();
        xmlRole.name = xmlLegacyRole.name;
        if (xmlLegacyRole.union != null) {
            for (Mondrian3Def.RoleUsage xmlLegacyRoleUsage
                : xmlLegacyRole.union.roleUsages)
            {
                xmlRole.children.holder(new MondrianDef.Union()).list().add(
                    convertRoleUsage(xmlLegacyRoleUsage));
            }
        }
        for (Mondrian3Def.SchemaGrant xmlLegacySchemaGrant
            : xmlLegacyRole.schemaGrants)
        {
            xmlRole.children.add(
                convertSchemaGrant(xmlLegacySchemaGrant));
        }
        convertAnnotations(
            xmlRole.children,
            xmlLegacyRole.annotations);
        return xmlRole;
    }

    private MondrianDef.RoleUsage convertRoleUsage(
        Mondrian3Def.RoleUsage xmlLegacyRoleUsage)
    {
        MondrianDef.RoleUsage xmlRoleUsage = new MondrianDef.RoleUsage();
        xmlRoleUsage.roleName = xmlLegacyRoleUsage.roleName;
        return xmlRoleUsage;
    }

    private MondrianDef.SchemaGrant convertSchemaGrant(
        Mondrian3Def.SchemaGrant xmlLegacySchemaGrant)
    {
        MondrianDef.SchemaGrant xmlSchemaGrant = new MondrianDef.SchemaGrant();
        xmlSchemaGrant.access = xmlLegacySchemaGrant.access;
        xmlSchemaGrant.cubeGrants =
            convertCubeGrants(xmlLegacySchemaGrant.cubeGrants);
        return xmlSchemaGrant;
    }

    private MondrianDef.CubeGrant[] convertCubeGrants(
        Mondrian3Def.CubeGrant[] xmlLegacyCubeGrants)
    {
        List<MondrianDef.CubeGrant> list =
            new ArrayList<MondrianDef.CubeGrant>();
        for (Mondrian3Def.CubeGrant xmlLegacyCubeGrant : xmlLegacyCubeGrants) {
            list.add(
                convertCubeGrant(
                    xmlLegacyCubeGrant));
        }
        return list.toArray(new MondrianDef.CubeGrant[list.size()]);
    }

    private MondrianDef.CubeGrant convertCubeGrant(
        Mondrian3Def.CubeGrant xmlLegacyCubeGrant)
    {
        MondrianDef.CubeGrant xmlCubeGrant = new MondrianDef.CubeGrant();
        xmlCubeGrant.cube = xmlLegacyCubeGrant.cube;
        xmlCubeGrant.access = xmlLegacyCubeGrant.access;
        xmlCubeGrant.dimensionGrants =
            convertDimensionGrants(xmlLegacyCubeGrant.dimensionGrants);
        xmlCubeGrant.hierarchyGrants =
            convertHierarchyGrants(xmlLegacyCubeGrant.hierarchyGrants);
        return xmlCubeGrant;
    }

    private MondrianDef.DimensionGrant[] convertDimensionGrants(
        Mondrian3Def.DimensionGrant[] xmlLegacyDimensionGrants)
    {
        List<MondrianDef.DimensionGrant> list =
            new ArrayList<MondrianDef.DimensionGrant>();
        for (Mondrian3Def.DimensionGrant xmlLegacyDimensionGrant
            : xmlLegacyDimensionGrants)
        {
            list.add(
                convertDimensionGrant(
                    xmlLegacyDimensionGrant));
        }
        return list.toArray(new MondrianDef.DimensionGrant[list.size()]);
    }

    private MondrianDef.DimensionGrant convertDimensionGrant(
        Mondrian3Def.DimensionGrant xmlLegacyDimensionGrant)
    {
        MondrianDef.DimensionGrant xmlDimensionGrant =
            new MondrianDef.DimensionGrant();
        xmlDimensionGrant.access = xmlLegacyDimensionGrant.access;
        xmlDimensionGrant.dimension = xmlLegacyDimensionGrant.dimension;
        return xmlDimensionGrant;
    }

    private MondrianDef.HierarchyGrant[] convertHierarchyGrants(
        Mondrian3Def.HierarchyGrant[] xmlLegacyHierarchyGrants)
    {
        List<MondrianDef.HierarchyGrant> list =
            new ArrayList<MondrianDef.HierarchyGrant>();
        for (Mondrian3Def.HierarchyGrant xmlLegacyHierarchyGrant
            : xmlLegacyHierarchyGrants)
        {
            list.add(
                convertHierarchyGrant(
                    xmlLegacyHierarchyGrant));
        }
        return list.toArray(new MondrianDef.HierarchyGrant[list.size()]);
    }

    private MondrianDef.HierarchyGrant convertHierarchyGrant(
        Mondrian3Def.HierarchyGrant xmlLegacyHierarchyGrant)
    {
        MondrianDef.HierarchyGrant xmlHierarchyGrant =
            new MondrianDef.HierarchyGrant();
        xmlHierarchyGrant.access = xmlLegacyHierarchyGrant.access;
        xmlHierarchyGrant.topLevel = xmlLegacyHierarchyGrant.topLevel;
        xmlHierarchyGrant.bottomLevel = xmlLegacyHierarchyGrant.bottomLevel;
        xmlHierarchyGrant.hierarchy = xmlLegacyHierarchyGrant.hierarchy;
        xmlHierarchyGrant.rollupPolicy = xmlLegacyHierarchyGrant.rollupPolicy;
        xmlHierarchyGrant.memberGrants =
            convertMemberGrants(xmlLegacyHierarchyGrant.memberGrants);
        return xmlHierarchyGrant;
    }

    private MondrianDef.MemberGrant[] convertMemberGrants(
        Mondrian3Def.MemberGrant[] xmlLegacyMemberGrants)
    {
        List<MondrianDef.MemberGrant> list =
            new ArrayList<MondrianDef.MemberGrant>();
        for (Mondrian3Def.MemberGrant xmlLegacyMemberGrant
            : xmlLegacyMemberGrants)
        {
            list.add(
                convertMemberGrant(
                    xmlLegacyMemberGrant));
        }
        return list.toArray(new MondrianDef.MemberGrant[list.size()]);
    }

    private MondrianDef.MemberGrant convertMemberGrant(
        Mondrian3Def.MemberGrant xmlLegacyMemberGrant)
    {
        MondrianDef.MemberGrant xmlMemberGrant = new MondrianDef.MemberGrant();
        xmlMemberGrant.access = xmlLegacyMemberGrant.access;
        xmlMemberGrant.member = xmlLegacyMemberGrant.member;
        return xmlMemberGrant;
    }

    /**
     * Association between a MondrianDef.Table with its associated
     * level's depth. This is used to rank tables in a snowflake so that
     * the table with the lowest rank, level depth, is furthest from
     * the base fact table in the RolapStar.
     */
    private static class RelNode {

        /**
         * Finds a RelNode by table name or, if that fails, by table alias
         * from a map of RelNodes.
         *
         * @param table Table
         * @param map Map
         */
        private static RelNode lookup(
            Mondrian3Def.Relation table,
            Map<String, RelNode> map)
        {
            RelNode relNode;
            if (table instanceof Mondrian3Def.Table) {
                relNode = map.get(((Mondrian3Def.Table) table).name);
                if (relNode != null) {
                    return relNode;
                }
            }
            return map.get(table.getAlias());
        }

        private int depth;
        private String alias;
        private Mondrian3Def.Relation table;

        RelNode(String alias, int depth) {
            this.alias = alias;
            this.depth = depth;
        }
    }

    /**
     * Link to be made between the bottom level in a hierarchy and a fact
     * table containing one or more measures.
     */
    static class Link {
        final RolapSchema.PhysRelation fact;
        final String foreignKey;

        /**
         * Creates a Link.
         *
         * <p>Fact table must not be null. Foreign key may be null if the
         * dimension lives in the fact table (i.e. is degenerate)
         *
         * @param fact Fact table
         * @param foreignKey Foreign key linking to fact table
         */
        Link(
            RolapSchema.PhysRelation fact,
            String foreignKey)
        {
            this.fact = fact;
            this.foreignKey = foreignKey;
        }
    }

    /**
     * Visitor that walks an MDX parse tree containing formulas
     * associated with calculated members defined in a base cube but
     * referenced from a virtual cube.  When walking the tree, look
     * for other calculated members as well as stored measures.  Keep
     * track of all stored measures found, and for the calculated members,
     * once the formula of that calculated member has been visited, resolve
     * the calculated member relative to the virtual cube.
     */
    private class MeasureFinder extends MdxVisitorImpl
    {
        private final Mondrian3Def.Schema xmlSchema;

        private final RolapSchema schema;

        /**
         * The virtual cube where the original calculated member was
         * referenced from
         */
        private RolapCube virtualCube;

        /**
         * The base cube where the original calculated member is defined
         */
        private RolapCube baseCube;

        /**
         * The measures level corresponding to the virtual cube
         */
        private RolapCubeLevel measuresLevel;

        /**
         * List of measures found
         */
        private List<RolapVirtualCubeMeasure> measuresFound;
        /**
         * List of calculated members found
         */
        private List<RolapCalculatedMember> calcMembersSeen;

        public MeasureFinder(
            RolapCube virtualCube,
            RolapCube baseCube,
            RolapCubeLevel measuresLevel)
        {
            this.xmlSchema = null; // TODO:
            this.schema = virtualCube.getSchema();
            this.virtualCube = virtualCube;
            this.baseCube = baseCube;
            this.measuresLevel = measuresLevel;
            this.measuresFound = new ArrayList<RolapVirtualCubeMeasure>();
            this.calcMembersSeen = new ArrayList<RolapCalculatedMember>();
        }

        public Object visit(MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            if (member instanceof RolapCalculatedMember) {
                // ignore the calculated member if we've already processed
                // it in another reference
                if (calcMembersSeen.contains(member)) {
                    return null;
                }
                RolapCalculatedMember calcMember =
                    (RolapCalculatedMember) member;
                Formula formula = calcMember.getFormula();
                formula.accept(this);
                calcMembersSeen.add(calcMember);

                // now that we've located all measures referenced in the
                // calculated member's formula, resolve the calculated
                // member relative to the virtual cube
                virtualCube.setMeasuresHierarchyMemberReader(
                    new CacheMemberReader(
                        new MeasureMemberSource(
                            virtualCube.getMeasuresHierarchy(),
                            Util.<RolapMember>cast(measuresFound))));

                Mondrian3Def.CalculatedMember xmlCalcMember =
                    lookupXmlCalculatedMember(
                        xmlSchema,
                        calcMember.getUniqueName(),
                        baseCube.getName());

//                ((RolapSchemaLoader) null).createCalcMembersAndNamedSets(
//                    Collections.singletonList(xmlCalcMember),
//                    Collections.<MondrianDef.NamedSet>emptyList(),
//                    new ArrayList<RolapMember>(),
//                    new ArrayList<Formula>(),
//                    virtualCube,
//                    false);
                return null;

            } else if (member instanceof RolapBaseCubeMeasure) {
                RolapBaseCubeMeasure baseMeasure =
                    (RolapBaseCubeMeasure) member;
                RolapMeasureGroup measureGroup = null;
                for (RolapMeasureGroup measureGroup2
                    : virtualCube.getMeasureGroups())
                {
                    if (measureGroup2.getName().equals(
                            baseMeasure.getCube().getName()))
                    {
                        measureGroup = measureGroup2;
                        break;
                    }
                }
                assert measureGroup != null;
                RolapVirtualCubeMeasure virtualCubeMeasure =
                    new RolapVirtualCubeMeasure(
                        measureGroup,
                        null,
                        measuresLevel,
                        baseMeasure,
                        Larders.EMPTY);
                if (!measuresFound.contains(virtualCubeMeasure)) {
                    measuresFound.add(virtualCubeMeasure);
                }
            }

            return null;
        }

        public List<RolapVirtualCubeMeasure> getMeasuresFound() {
            return measuresFound;
        }
    }

    private static class CubeComparator implements Comparator<RolapCube>
    {
        public int compare(RolapCube c1, RolapCube c2)
        {
            return c1.getName().compareTo(c2.getName());
        }
    }

    public static class RolapCubeUsages {
        private Mondrian3Def.CubeUsages cubeUsages;

        public RolapCubeUsages(Mondrian3Def.CubeUsages cubeUsage) {
            Util.deprecated("obsolete", false);
            this.cubeUsages = cubeUsage;
        }

        public boolean shouldIgnoreUnrelatedDimensions(
            RolapMeasureGroup measureGroup)
        {
            if (cubeUsages == null || cubeUsages.cubeUsages == null) {
                return false;
            }
            for (Mondrian3Def.CubeUsage usage : cubeUsages.cubeUsages) {
                if (usage.cubeName.equals(measureGroup.getName())
                    && Boolean.TRUE.equals(usage.ignoreUnrelatedDimensions))
                {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A <code>HierarchyUsage</code> is the usage of a hierarchy in the context
     * of a cube. Private hierarchies can only be used in their own
     * cube. Public hierarchies can be used in several cubes. The problem comes
     * when several cubes which the same public hierarchy are brought together
     * in one virtual cube. There are now several usages of the same public
     * hierarchy. Which one to use? It depends upon what measure we are
     * currently using. We should use the hierarchy usage for the fact table
     * which underlies the measure. That is what determines the foreign key to
     * join on.
     *
     * A <code>HierarchyUsage</code> is identified by
     * <code>(hierarchy.sharedHierarchy, factTable)</code> if the hierarchy is
     * shared, or <code>(hierarchy, factTable)</code> if it is private.
     */
    public static class HierarchyUsage {
        private static final Logger LOGGER =
            Logger.getLogger(HierarchyUsage.class);

        enum Kind {
            UNKNOWN,
            SHARED,
            VIRTUAL,
            PRIVATE
        }

        /**
         * Fact table (or relation) which this usage is joining to. This
         * identifies the usage, and determines which join conditions need to be
         * used.
         */
        protected final RolapSchema.PhysRelation fact;

        /**
         * This matches the hierarchy - may not be unique.
         * NOT NULL.
         */
        private final String hierarchyName;

        /**
         * not NULL for DimensionUsage
         * not NULL for Dimension
         */
        private final String name;

        /**
         * This is the name used to look up the hierachy usage. When the
         * dimension has only a single hierachy, then the fullName is simply the
         * CubeDimension name; there is no need to use the default dimension
         * name.  But, when the dimension has more than one hierachy, then the
         * fullName is the CubeDimension dotted with the dimension hierachy
         * name.
         *
         * <p>NOTE: jhyde, 2009/2/2: The only use of this field today is for
         * {@code RolapCube.getUsageByName}, which is used only for tracing.
         */
        private final String fullName;

        /**
         * The foreign key by which this {@link Hierarchy} is joined to
         * the {@link #fact} table.
         */
        private final String foreignKey;

        /**
         * not NULL for DimensionUsage
         * NULL for Dimension
         */
        private final String source;

        /**
         * May be null, this is the field that is used to disambiguate column
         * names in aggregate tables
         */
        private final String usagePrefix;

        // NOT USED
        private final String level;
        //final String type;
        //final String caption;

        /**
         * Dimension table which contains the primary key for the hierarchy.
         * (Usually the table of the lowest level of the hierarchy.)
         */
        private RolapSchema.PhysRelation joinTable;

        /**
         * The expression (usually a {@link mondrian.olap.MondrianDef.Column}) by
         * which the hierarchy which is joined to the fact table.
         */
        private RolapSchema.PhysExpr joinExp;

        private final Kind kind;

        /**
         * Creates a HierarchyUsage.
         *
         * @param cube Cube
         * @param hierarchy Hierarchy
         * @param cubeDim XML definition of a dimension which belongs to a cube
         */
        HierarchyUsage(
            RolapCube cube,
            RolapHierarchy hierarchy,
            Mondrian3Def.CubeDimension cubeDim)
        {
            Util.deprecated("remove HierarchyUsage", true);
            assert cubeDim != null : "precondition: cubeDim != null";

            this.fact = null; //cube.fact;

            // Attributes common to all Hierarchy kinds
            // name
            // foreignKey
            this.name = cubeDim.name;
            this.foreignKey = cubeDim.foreignKey;

            if (cubeDim instanceof Mondrian3Def.DimensionUsage) {
                this.kind = Kind.SHARED;


                // Shared Hierarchy attributes
                // source
                // level
                Mondrian3Def.DimensionUsage du =
                    (Mondrian3Def.DimensionUsage) cubeDim;

                this.hierarchyName = deriveHierarchyName(hierarchy);
                int index = this.hierarchyName.indexOf('.');
                if (index == -1) {
                    this.fullName = this.name;
                    this.source = du.source;
                } else {
                    String hname =
                        this.hierarchyName.substring(
                            index + 1, this.hierarchyName.length());

                    StringBuilder buf = new StringBuilder(32);
                    buf.append(this.name);
                    buf.append('.');
                    buf.append(hname);
                    this.fullName = buf.toString();

                    buf.setLength(0);
                    buf.append(du.source);
                    buf.append('.');
                    buf.append(hname);
                    this.source = buf.toString();
                }

                this.level = du.level;
                this.usagePrefix = du.usagePrefix;

                init(cube, hierarchy, du);

            } else if (cubeDim instanceof Mondrian3Def.Dimension) {
                this.kind = Kind.PRIVATE;

                // Private Hierarchy attributes
                // type
                // caption
                Mondrian3Def.Dimension d = (Mondrian3Def.Dimension) cubeDim;

                this.hierarchyName = deriveHierarchyName(hierarchy);
                this.fullName = this.name;

                this.source = null;
                this.usagePrefix = d.usagePrefix;
                this.level = null;

                init(cube, hierarchy, null);

            } else if (cubeDim instanceof Mondrian3Def.VirtualCubeDimension) {
                this.kind = Kind.VIRTUAL;

                // Virtual Hierarchy attributes
                Mondrian3Def.VirtualCubeDimension vd =
                    (Mondrian3Def.VirtualCubeDimension) cubeDim;

                this.hierarchyName = cubeDim.name;
                this.fullName = this.name;

                this.source = null;
                this.usagePrefix = null;
                this.level = null;

                init(cube, hierarchy, null);

            } else {
                getLogger().warn(
                    "HierarchyUsage<init>: Unknown cubeDim="
                    + cubeDim.getClass().getName());

                this.kind = Kind.UNKNOWN;

                this.hierarchyName = cubeDim.name;
                this.fullName = this.name;

                this.source = null;
                this.usagePrefix = null;
                this.level = null;

                init(cube, hierarchy, null);
            }
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(
                    toString()
                    + ", cubeDim="
                    + cubeDim.getClass().getName());
            }
        }

        private String deriveHierarchyName(RolapHierarchy hierarchy) {
            final String name = hierarchy.getName();
            final String dimensionName = hierarchy.getDimension().getName();
            if (name == null
                || name.equals("")
                || name.equals(dimensionName))
            {
                return name;
            } else {
                return dimensionName + '.' + name;
            }
        }

        protected Logger getLogger() {
            return LOGGER;
        }

        public String getHierarchyName() {
            return this.hierarchyName;
        }

        public String getFullName() {
            return this.fullName;
        }

        public String getName() {
            return this.name;
        }

        public String getForeignKey() {
            return this.foreignKey;
        }

        public String getSource() {
            return this.source;
        }

        public String getLevelName() {
            return this.level;
        }

        public String getUsagePrefix() {
            return this.usagePrefix;
        }

        public RolapSchema.PhysRelation getJoinTable() {
            return this.joinTable;
        }

        public RolapSchema.PhysExpr getJoinExp() {
            return this.joinExp;
        }

        public Kind getKind() {
            return this.kind;
        }

        public boolean isShared() {
            return this.kind == Kind.SHARED;
        }

        public boolean isVirtual() {
            return this.kind == Kind.VIRTUAL;
        }

        public boolean isPrivate() {
            return this.kind == Kind.PRIVATE;
        }

        public boolean equals(Object o) {
            if (o instanceof HierarchyUsage) {
                HierarchyUsage other = (HierarchyUsage) o;
                return (this.kind == other.kind)
                    && Util.equals(this.fact, other.fact)
                    && this.hierarchyName.equals(other.hierarchyName)
                    && Util.equalName(this.name, other.name)
                    && Util.equalName(this.source, other.source)
                    && Util.equalName(this.foreignKey, other.foreignKey);
            } else {
                return false;
            }
        }

        public int hashCode() {
            int h = fact.hashCode();
            h = Util.hash(h, hierarchyName);
            h = Util.hash(h, name);
            h = Util.hash(h, source);
            h = Util.hash(h, foreignKey);
            return h;
        }

        public String toString() {
            StringBuilder buf = new StringBuilder(100);
            buf.append("HierarchyUsage: ");
            buf.append("kind=");
            buf.append(this.kind.name());
            buf.append(", hierarchyName=");
            buf.append(this.hierarchyName);
            buf.append(", fullName=");
            buf.append(this.fullName);
            buf.append(", foreignKey=");
            buf.append(this.foreignKey);
            buf.append(", source=");
            buf.append(this.source);
            buf.append(", level=");
            buf.append(this.level);
            buf.append(", name=");
            buf.append(this.name);

            return buf.toString();
        }

        void init(
            RolapCube cube,
            RolapHierarchy hierarchy,
            Mondrian3Def.DimensionUsage cubeDim)
        {
            Util.deprecated("fix or remove", false);
        }
    }

    static class Info {
        final String cubeName;
        final Mondrian3Def.Cube xmlLegacyCube;
        MondrianDef.MeasureGroup xmlMeasureGroup;
        boolean ignoreUnrelatedDimensions;

        public Info(String cubeName, Mondrian3Def.Cube xmlLegacyCube) {
            this.cubeName = cubeName;
            this.xmlLegacyCube = xmlLegacyCube;
        }
    }

    static class CubeInfo {
        final String name;
        final RolapSchema.PhysRelation fact;
        final Mondrian3Def.Relation xmlFact;
        final Mondrian3Def.Cube xmlLegacyCube;
        final Map<String, String> dimensionKeys =
            new LinkedHashMap<String, String>();

        public CubeInfo(
            String name,
            RolapSchema.PhysRelation fact,
            Mondrian3Def.Relation xmlFact,
            Mondrian3Def.Cube xmlLegacyCube)
        {
            this.name = name;
            this.fact = fact;
            this.xmlFact = xmlFact;
            this.xmlLegacyCube = xmlLegacyCube;
        }
    }

    static class LevelInfo {
        final String dimension;
        final String hierarchy;
        final String level;
        final String table;
        final String column;

        public LevelInfo(
            String dimension,
            String hierarchy,
            String level,
            String table,
            String column)
        {
            this.dimension = dimension;
            this.hierarchy = hierarchy;
            this.level = level;
            this.table = table;
            this.column = column;
        }
    }
}

// End RolapSchemaUpgrader.java
