/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.spi.Dialect;
import mondrian.util.Pair;

import java.util.*;

/**
 * Converts a mondrian-3.x schema to a mondrian-4 schema.
 *
 * @author jhyde
 * @version $Id$
 */
public class RolapSchemaUpgrader {
    RolapSchemaUpgrader() {
    }

    // Upgrade old-style cube (which has a <Measures> element) to a
    // new-style cube (which has a <MeasureGroups> element).
    private MondrianDef.MeasureGroups upgrade(
        RolapSchema schema,
        Mondrian3Def.Cube xmlCube)
    {
        final Mondrian3Def.Relation fact = xmlCube.fact;
        if (fact == null) {
            schema.warning(
                "Cube '" + xmlCube.name + "' requires fact table",
                schema.locate(xmlCube, null),
                null);
            return null;
        }
        if (fact.getAlias() == null) {
            throw Util.newError(
                "Must specify alias for fact table of cube '" + xmlCube.name
                + "'");
        }

        MondrianDef.MeasureGroups measureGroups =
            new MondrianDef.MeasureGroups();
        final MondrianDef.MeasureGroup xmlMeasureGroup =
            new MondrianDef.MeasureGroup();
        measureGroups.array =
            new MondrianDef.MeasureGroup[] {xmlMeasureGroup};
        xmlMeasureGroup.name = xmlCube.name;
        xmlMeasureGroup.table = fact.getAlias();
        xmlMeasureGroup.dimensionLinks =
            new MondrianDef.DimensionLinks();
        List<MondrianDef.DimensionLink> dimensionLinks =
            new ArrayList<MondrianDef.DimensionLink>();
        for (Mondrian3Def.CubeDimension xmlDimension : xmlCube.dimensions) {
            final MondrianDef.DimensionLink xmlDimensionLink;
            final PhysSchemaBuilder.DimensionLink dimensionLink =
                physSchemaBuilder.dimensionLinks.get(xmlDimension.name);
            if (dimensionLink.degenerate) {
                final MondrianDef.FactDimensionLink xmlFactLink =
                    new MondrianDef.FactDimensionLink();
                xmlFactLink.dimension = xmlDimension.name;
                xmlDimensionLink = xmlFactLink;
            } else {
                MondrianDef.RegularDimensionLink xmlRegularLink =
                    new MondrianDef.RegularDimensionLink();
                xmlRegularLink.dimension = xmlDimension.name;
                xmlRegularLink.foreignKey = new MondrianDef.ForeignKey();
                xmlRegularLink.foreignKey.columns =
                    new MondrianDef.Column[] {
                        new MondrianDef.Column(
                            fact.getAlias(), dimensionLink.foreignKey)
                    };
                xmlRegularLink.key = new MondrianDef.Key();
                xmlRegularLink.key.columns =
                    new MondrianDef.Column[] {
                        new MondrianDef.Column(
                            dimensionLink.primaryKeyTable,
                            dimensionLink.primaryKey)
                    };
                xmlDimensionLink = xmlRegularLink;
            }
            dimensionLinks.add(xmlDimensionLink);
        }
        xmlMeasureGroup.dimensionLinks.array =
            dimensionLinks.toArray(
                new MondrianDef.DimensionLink[dimensionLinks.size()]);
        return measureGroups;
    }


    /**
     * Creates a <code>RolapCube</code> from a virtual cube.
     *
     * @param schema Schema cube belongs to
     * @param xmlSchema XML Schema element
     * @param syntheticPhysSchema Synthetic physical schema, if (and only if)
     *   the user's schema has no PhysicalSchema element
     * @param xmlVirtualCube XML element defining virtual cube
     */
    RolapCube(
        RolapSchema schema,
        MondrianDef.Schema xmlSchema,
        final RolapSchema.PhysSchema syntheticPhysSchema,
        MondrianDef.VirtualCube xmlVirtualCube)
    {
        this(
            schema,
            xmlSchema, xmlVirtualCube.name,
            xmlVirtualCube.caption,
            xmlVirtualCube.description, xmlVirtualCube.dimensions,
            RolapSchemaLoader.createAnnotationMap(xmlVirtualCube.annotations));

        // Since MondrianDef.Measure and MondrianDef.VirtualCubeMeasure cannot
        // be treated as the same, measure creation cannot be done in a common
        // constructor.
        RolapLevel measuresLevel = this.measuresHierarchy.newMeasuresLevel();

        // Recreate CalculatedMembers, as the original members point to
        // incorrect dimensional ordinals for the virtual cube.
        List<RolapVirtualCubeMeasure> origMeasureList =
            new ArrayList<RolapVirtualCubeMeasure>();
        List<MondrianDef.CalculatedMember> origCalcMeasureList =
            new ArrayList<MondrianDef.CalculatedMember>();
        CubeComparator cubeComparator = new CubeComparator();
        Map<RolapCube, List<MondrianDef.CalculatedMember>>
            calculatedMembersMap =
            new TreeMap<RolapCube, List<MondrianDef.CalculatedMember>>(
                cubeComparator);
        Member defaultMeasure = null;

        this.cubeUsages = new RolapCubeUsages(xmlVirtualCube.cubeUsage);

        // Must init the dimensions before dealing with calculated members.
        RolapSchemaLoader.initCube(this, physSchemaBuilder, xmlVirtualCube.dimensions);

        // For each base cube, create a measure group. Populate with measures
        // and link dimensions.
        final Map<String, RolapMeasureGroup> measureGroupsByName =
            new HashMap<String, RolapMeasureGroup>();
        for (String cubeName : getUsedCubeNames(xmlVirtualCube)) {
            final RolapCube baseCube = schema.lookupCube(cubeName);
            assert baseCube.measureGroupList.size() == 1;
            final RolapMeasureGroup baseMeasureGroup =
                baseCube.measureGroupList.get(0);
            final RolapMeasureGroup measureGroup =
                new RolapMeasureGroup(
                    this, cubeName,
                    xmlMeasureGroup.ignoreUnrelatedDimensions != null
                    && xmlMeasureGroup.ignoreUnrelatedDimensions,
                    baseMeasureGroup.getStar());
            measureGroupsByName.put(cubeName, measureGroup);
            measureGroupList.add(measureGroup);
            for (MondrianDef.VirtualCubeDimension xmlVirtualCubeDimension
                : xmlVirtualCube.dimensions)
            {
                if (xmlVirtualCubeDimension.cubeName != null
                    && !xmlVirtualCubeDimension.cubeName.equals(
                    cubeName))
                {
                    continue;
                }
                RolapDimension dimension =
                    baseCube.lookupDimension(
                        new Id.Segment(
                            xmlVirtualCubeDimension.name,
                            Id.Quoting.UNQUOTED));
                assert dimension != null;
                RolapDimension baseDimension;
                if (dimension instanceof RolapCubeDimension) {
                    baseDimension =
                        ((RolapCubeDimension) dimension).rolapDimension;
                } else {
                    Util.deprecated("does this ever happen", true);
                    baseDimension = dimension;
                }
                RolapSchema.PhysPath hop =
                    baseMeasureGroup.dimensionMap2.get(baseDimension);
                measureGroup.addLink(dimension, hop);
            }
        }

        // Create measures, looking up measures in existing cubes.
        for (MondrianDef.VirtualCubeMeasure xmlMeasure
            : xmlVirtualCube.measures)
        {
            RolapCube cube = schema.lookupCube(xmlMeasure.cubeName);
            if (cube == null) {
                throw Util.newError(
                    "Cube '" + xmlMeasure.cubeName + "' not found");
            }
            List<Member> cubeMeasures = cube.getMeasures();
            boolean found = false;
            for (Member cubeMeasure : cubeMeasures) {
                if (cubeMeasure.getUniqueName().equals(xmlMeasure.name)) {
                    if (cubeMeasure.getName().equalsIgnoreCase(
                        xmlVirtualCube.defaultMeasure))
                    {
                        defaultMeasure = cubeMeasure;
                    }
                    found = true;
                    if (cubeMeasure instanceof RolapCalculatedMember) {
                        // We have a calulated member!  Keep track of which
                        // base cube each calculated member is associated
                        // with, so we can resolve the calculated member
                        // relative to its base cube.  We're using a treeMap
                        // to store the mapping to ensure a deterministic
                        // order for the members.
                        MondrianDef.CalculatedMember calcMember =
                            schema.lookupXmlCalculatedMember(
                                xmlMeasure.name, xmlMeasure.cubeName);
                        if (calcMember == null) {
                            throw Util.newInternal(
                                "Could not find XML Calculated Member '"
                                + xmlMeasure.name + "' in XML cube '"
                                + xmlMeasure.cubeName + "'");
                        }
                        List<MondrianDef.CalculatedMember> memberList =
                            calculatedMembersMap.get(cube);
                        if (memberList == null) {
                            memberList =
                                new ArrayList<MondrianDef.CalculatedMember>();
                        }
                        memberList.add(calcMember);
                        origCalcMeasureList.add(calcMember);
                        calculatedMembersMap.put(cube, memberList);
                    } else {
                        // This is the a standard measure. (Don't know
                        // whether it will confuse things that this
                        // measure still points to its 'real' cube.)
                        final RolapMeasureGroup measureGroup =
                            measureGroupsByName.get(xmlMeasure.cubeName);
                        assert measureGroup != null;
                        RolapVirtualCubeMeasure virtualCubeMeasure =
                            new RolapVirtualCubeMeasure(
                                measureGroup,
                                null,
                                measuresLevel,
                                (RolapStoredMeasure) cubeMeasure,
                                RolapSchemaLoader.createAnnotationMap(
                                    xmlMeasure.annotations));

                        // Set member's visibility, default true.
                        Boolean visible = xmlMeasure.visible;
                        if (visible == null) {
                            visible = Boolean.TRUE;
                        }
                        virtualCubeMeasure.setProperty(
                            Property.VISIBLE.name,
                            visible);
                        // Inherit caption from the "real" measure
                        virtualCubeMeasure.setProperty(
                            Property.CAPTION.name,
                            cubeMeasure.getCaption());
                        origMeasureList.add(virtualCubeMeasure);
                    }
                    break;
                }
            }
            if (!found) {
                throw Util.newInternal(
                    "could not find measure '" + xmlMeasure.name
                    + "' in cube '" + xmlMeasure.cubeName + "'");
            }
        }

        // Loop through the base cubes containing calculated members
        // referenced by this virtual cube.  Resolve those members relative
        // to their base cubes first, then resolve them relative to this
        // cube so the correct dimension ordinals are used
        List<RolapVirtualCubeMeasure> modifiedMeasureList =
            new ArrayList<RolapVirtualCubeMeasure>(origMeasureList);
        for (Object o : calculatedMembersMap.keySet()) {
            RolapCube baseCube = (RolapCube) o;
            List<MondrianDef.CalculatedMember> xmlCalculatedMemberList =
                calculatedMembersMap.get(baseCube);
            Query queryExp =
                resolveCalcMembers(
                    xmlCalculatedMemberList,
                    Collections.<MondrianDef.NamedSet>emptyList(),
                    baseCube,
                    false);
            MeasureFinder measureFinder =
                new MeasureFinder(this, baseCube, measuresLevel);
            queryExp.accept(measureFinder);
            modifiedMeasureList.addAll(measureFinder.getMeasuresFound());
        }

        // Add the original calculated members from the base cubes to our
        // list of calculated members
        List<MondrianDef.CalculatedMember> xmlCalculatedMemberList =
            new ArrayList<MondrianDef.CalculatedMember>();
        for (Object o : calculatedMembersMap.keySet()) {
            RolapCube baseCube = (RolapCube) o;
            xmlCalculatedMemberList.addAll(
                calculatedMembersMap.get(baseCube));
        }
        xmlCalculatedMemberList.addAll(
            Arrays.asList(xmlVirtualCube.calculatedMembers));


        // Resolve all calculated members relative to this virtual cube,
        // whose measureHierarchy member reader now contains all base
        // measures referenced in those calculated members
        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(
                    this.measuresHierarchy,
                    Util.<RolapMember>cast(modifiedMeasureList))));

        createCalcMembersAndNamedSets(
            xmlCalculatedMemberList,
            Arrays.asList(xmlVirtualCube.namedSets),
            new ArrayList<RolapMember>(),
            new ArrayList<Formula>(),
            this,
            false);

        // reset the measureHierarchy member reader back to the list of
        // measures that are only defined on this virtual cube
        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(
                    this.measuresHierarchy,
                    Util.<RolapMember>cast(origMeasureList))));

        this.measuresHierarchy.setDefaultMember(defaultMeasure);

        List<MondrianDef.CalculatedMember> xmlVirtualCubeCalculatedMemberList =
                Arrays.asList(xmlVirtualCube.calculatedMembers);
        if (!vcHasAllCalcMembers(
            origCalcMeasureList, xmlVirtualCubeCalculatedMemberList))
        {
            // Remove from the calculated members array
            // those members that weren't originally defined
            // on this virtual cube.
            List<Formula> calculatedMemberListCopy =
                new ArrayList<Formula>(calculatedMemberList);
            calculatedMemberList.clear();
            for (Formula calculatedMember : calculatedMemberListCopy) {
                if (findOriginalMembers(
                    calculatedMember,
                    origCalcMeasureList,
                    calculatedMemberList))
                {
                    continue;
                }
                findOriginalMembers(
                    calculatedMember,
                    xmlVirtualCubeCalculatedMemberList,
                    calculatedMemberList);
            }
        }

        for (Formula calcMember : calculatedMemberList) {
            if (calcMember.getName().equalsIgnoreCase(
                xmlVirtualCube.defaultMeasure))
            {
                this.measuresHierarchy.setDefaultMember(
                    calcMember.getMdxMember());
                break;
            }
        }

        // Note: virtual cubes do not get aggregate
    }

    private boolean vcHasAllCalcMembers(
        List<MondrianDef.CalculatedMember> origCalcMeasureList,
        List<MondrianDef.CalculatedMember> xmlVirtualCubeCalculatedMemberList)
    {
        return calculatedMemberList.size()
            == (origCalcMeasureList.size()
            + xmlVirtualCubeCalculatedMemberList.size());
    }

    private boolean findOriginalMembers(
        Formula formula,
        List<MondrianDef.CalculatedMember> xmlCalcMemberList,
        List<Formula> calcMemberList)
    {
        for (MondrianDef.CalculatedMember xmlCalcMember : xmlCalcMemberList) {
            Dimension dimension =
                lookupDimension(
                    new Id.Segment(
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

    private MondrianDef.Dimension asdasd(
        RolapSchema.PhysRelation fact,
        Mondrian3Def.RelationOrJoin xmlFact,
        Mondrian3Def.CubeDimension xmlCubeDimension,
        RolapSchema schema,
        Mondrian3Def.Schema xmlSchema,
        Mondrian3Def.Dimension xmlDimension)
    {
        assert physSchemaBuilder != null;
        final List<Link> links;
        final String primaryKeyTable;
        final String primaryKey;
        if (xmlCubeDimension instanceof Mondrian3Def.VirtualCubeDimension) {
            final Mondrian3Def.VirtualCubeDimension xmlVirtualCubeDimension =
                (Mondrian3Def.VirtualCubeDimension) xmlCubeDimension;
            assert xmlFact == null
                : "VirtualCubeDimension only occurs within virtual cube, "
                + "which has no fact table";
            primaryKeyTable = null;
            primaryKey = null;
            if (xmlVirtualCubeDimension.cubeName == null) {
                // No cube specified. It's a shared dimension, and it is
                // implicitly joined to all cubes mentioned in other
                // VirtualCubeDimensions of this VirtualCube.
                links = new ArrayList<Link>();
                Mondrian3Def.VirtualCube xmlVirtualCube =
                    xmlSchema.getVirtualCube(name);
                for (String usedCubeName : getUsedCubeNames(xmlVirtualCube))
                {
                    // Usage of a cube. E.g. the [Warehouse and Sales]
                    // virtual cube uses real cubes [Sales] and [Warehouse]
                    final Mondrian3Def.Cube xmlCubeUsed =
                        xmlSchema.getCube(usedCubeName);
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
                                physSchemaBuilder.toPhysRelation(
                                    xmlCubeUsed.fact),
                                usageList.get(0).foreignKey));
                        break;
                    default:
                        Util.deprecated("test this", false);
                        schema.error(
                            "Shared cube dimension is ambiguous: more than "
                            + "one dimension in base cube "
                            + xmlCubeUsed.name
                            + " uses shared dimension "
                            + xmlVirtualCubeDimension.name,
                            xmlCubeDimension,
                            null);
                        break;
                    }
                }
                if (links.isEmpty()) {
                    schema.error(
                        "Virtual cube dimension must join to at least one "
                        + "cube: dimension '" + xmlVirtualCubeDimension.name
                        + "' in cube '" + name + "'",
                        xmlCubeDimension,
                        null);
                }
            } else {
                Mondrian3Def.Cube cube =
                    xmlSchema.getCube(xmlVirtualCubeDimension.cubeName);
                if (cube == null) {
                    Util.deprecated("use schema.error, and test", false);
                    throw Util.newError(
                        "Unknown cube '"
                        + xmlVirtualCubeDimension.cubeName + "'");
                }
                xmlFact = cube.fact;
                assert xmlFact != null;
                // REVIEW: is it safe to cast xmlFact to Relation? Is it
                // ever a join?
                assert xmlFact instanceof Mondrian3Def.Relation;
                fact = physSchemaBuilder.toPhysRelation(
                    (Mondrian3Def.Relation) xmlFact);
                links = Collections.singletonList(
                    new Link(
                        physSchemaBuilder.toPhysRelation(cube.fact),
                        xmlVirtualCubeDimension.foreignKey));
            }
        } else {
            final String foreignKey;
            if (xmlCubeDimension instanceof Mondrian3Def.DimensionUsage) {
                foreignKey =
                    ((Mondrian3Def.DimensionUsage) xmlCubeDimension)
                        .foreignKey;
            } else if (xmlCubeDimension instanceof Mondrian3Def.Dimension) {
                foreignKey = xmlDimension.foreignKey;
            } else {
                throw new AssertionError("unknown dimension type");
            }
            final boolean degenerate =
                isDegenerate(xmlCubeDimension, xmlSchema, xmlFact);
            if (foreignKey == null && !degenerate) {
                throw schema.fatal(
                    "Dimension or DimensionUsage must have foreignKey",
                    schema.locate(xmlCubeDimension, null),
                    null);
            }
            links =
                Collections.singletonList(
                    new Link(fact, foreignKey));
            final Pair<String, String> pair =
                getUniquePrimaryKey(xmlDimension);
            primaryKeyTable = pair.left;
            primaryKey = pair.right;
            physSchemaBuilder.dimensionLinks.put(
                xmlDimension.name,
                new PhysSchemaBuilder.DimensionLink(
                    this, xmlDimension.name, fact, foreignKey,
                    primaryKeyTable, primaryKey, degenerate));
        }
        // TODO: How does SSAS know which dimensions are linked to each
        // measure group? Is it explicit (and then validated) or implicit
        // (only dimensions with a join path are linked).
        xmlDimension =
            ((PhysSchemaConverter) physSchemaBuilder).convertDimension(
                links,
                xmlFact,
                xmlDimension);
        xmlDimension.tmpPrimaryKey = primaryKey;
        xmlDimension.tmpPrimaryKeyTable = primaryKeyTable;
        return xmlDimension;
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
        RolapStar star = getStar();

        for (RolapCubeHierarchy hierarchy
            : dimension.getRolapCubeHierarchyList())
        {
            Util.deprecated("obsolete method?", false);
            Mondrian3Def.RelationOrJoin relation = null;
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }
            List<RolapCubeLevel> levels = hierarchy.getRolapCubeLevelList();

            HierarchyUsage[] hierarchyUsages = getUsages(hierarchy);
            if (hierarchyUsages.length == 0) {
                if (getLogger().isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(64);
                    buf.append("RolapCube.registerDimension: ");
                    buf.append("hierarchyUsages == null for cube=\"");
                    buf.append(this.name);
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
                // an unsupported configuation and all bets are off.
                if (relation instanceof MondrianDef.Join) {
                    // RME
                    // take out after things seem to be working
                    MondrianDef.RelationOrJoin relationTmp1 = relation;

                    relation = reorder(relation, levels);

                    if (relation == null && getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.registerDimension: after reorder relation==null");
                        getLogger().debug(
                            "RolapCube.registerDimension: reorder relationTmp1="
                                + format(relationTmp1));
                    }
                }

                MondrianDef.RelationOrJoin relationTmp2 = relation;

                if (levelName != null) {
                    // When relation is a table, this does nothing. Otherwise
                    // it tries to arrange the joins so that the fact table
                    // in the RolapStar will be joining at the lowest level.
                    //

                    // Make sure the level exists
                    RolapLevel level =
                        RolapLevel.lookupLevel(levels, levelName);
                    if (level == null) {
                        StringBuilder buf = new StringBuilder(64);
                        buf.append("For cube \"");
                        buf.append(getName());
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
                    if (relation instanceof MondrianDef.Join) {
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
                                hierarchy.getName(), getName());
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
                                getName());
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
                        table, this, relation, joinCondition);
                }

                // The parent Column is used so that non-shared dimensions
                // which use the fact table (not a separate dimension table)
                // can keep a record of what other columns are in the
                // same set of levels.
                RolapStar.Column parentColumn = null;

                //RME
                // If the level name is not null, then we need only register
                // those columns for that level and above.
                if (levelName != null) {
                    for (RolapCubeLevel level : levels) {
                        if (null /* level.getKeyExp() */ != null) {
                            parentColumn =
                                makeColumns(
                                    table, level, parentColumn, usagePrefix);
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
                            parentColumn =
                                makeColumns(
                                    table, level, parentColumn, usagePrefix);
                        }
                    }
                }
            }
        }
    }

    // The following code deals with handling the DimensionUsage level attribute
    // and snowflake dimensions only.

    /**
     * Formats a {@link mondrian.olap.MondrianDef.RelationOrJoin}, indenting
     * joins for readability.
     *
     * @param relation
     */
    private static String format(MondrianDef.RelationOrJoin relation) {
        StringBuilder buf = new StringBuilder();
        format(relation, buf, "");
        return buf.toString();
    }

    private static void format(
        MondrianDef.RelationOrJoin relation,
        StringBuilder buf,
        String indent)
    {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;

            buf.append(indent);
            buf.append(table.name);
            if (table.alias != null) {
                buf.append('(');
                buf.append(table.alias);
                buf.append(')');
            }
            buf.append(Util.nl);
        } else {
            MondrianDef.Join join = (MondrianDef.Join) relation;
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

    /**
     * Attempts to transform a {@link mondrian.olap.MondrianDef.RelationOrJoin}
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
     * @param relation
     * @param levels
     */
    private static Mondrian3Def.RelationOrJoin reorder(
        Mondrian3Def.RelationOrJoin relation,
        RolapLevel[] levels)
    {
        // Need at least two levels, with only one level theres nothing to do.
        if (levels.length < 2) {
            return relation;
        }

        Map<String, RelNode> nodeMap = new HashMap<String, RelNode>();

        // Create RelNode in top down order (year -> day)
        for (int i = 0; i < levels.length; i++) {
            RolapLevel level = levels[i];

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
     * @param relation
     * @param map
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
     * @param relation
     * @param map
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
     * @param relation
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
     * @param relation
     */
    private static Mondrian3Def.RelationOrJoin copy(
        Mondrian3Def.RelationOrJoin relation)
    {
        if (relation instanceof Mondrian3Def.Table) {
            Mondrian3Def.Table table = (Mondrian3Def.Table) relation;
            return new Mondrian3Def.Table(table);

        } else if (relation instanceof Mondrian3Def.InlineTable) {
            Mondrian3Def.InlineTable table = (Mondrian3Def.InlineTable) relation;
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
     * @param relation
     * @param tableName
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
    static synchronized RolapStar.Table addJoin(
        RolapStar.Table table,
        RolapSchemaLoader.PhysSchemaBuilder physSchemaBuilder,
        Mondrian3Def.RelationOrJoin relationOrJoin,
        RolapStar.Condition joinCondition)
    {
        Util.deprecated("move this to PhysSchmaBuilder?", false);
        if (relationOrJoin instanceof Mondrian3Def.Relation) {
            final Mondrian3Def.Relation relation =
                (Mondrian3Def.Relation) relationOrJoin;
            final RolapSchema.PhysRelation physRelation =
                toPhysRelation(
                    physSchemaBuilder,
                    relation);
            RolapStar.Table starTable =
                table.findChild(physRelation, joinCondition, true);
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
            joinCondition =
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
     * @param physSchemaBuilder
     * @param xmlRelation XML relation
     * @return Physical table
     */
    static RolapSchema.PhysRelation toPhysRelation(
        RolapSchemaLoader.PhysSchemaBuilder physSchemaBuilder,
        final MondrianDef.Relation xmlRelation)
    {
        final String alias = xmlRelation.getAlias();
        RolapSchema.PhysRelation physRelation =
            physSchemaBuilder.physSchema.tablesByName.get(alias);
        if (physRelation == null) {
            if (xmlRelation instanceof Mondrian3Def.Table) {
                Mondrian3Def.Table xmlTable =
                    (Mondrian3Def.Table) xmlRelation;
                final RolapSchema.PhysTable physTable =
                    new RolapSchema.PhysTable(
                        physSchemaBuilder.physSchema,
                        xmlTable.schema,
                        xmlTable.name,
                        alias,
                        RolapSchemaLoader.buildHintMap(
                            xmlTable.tableHints));
                // Read columns from JDBC.
                physTable.ensurePopulated(
                    physSchemaBuilder.cube.getSchema(),
                    xmlTable);
                physRelation = physTable;
            } else if (xmlRelation instanceof Mondrian3Def.InlineTable) {
                final Mondrian3Def.InlineTable xmlInlineTable =
                    (Mondrian3Def.InlineTable) xmlRelation;
                RolapSchema.PhysInlineTable physInlineTable =
                    new RolapSchema.PhysInlineTable(
                        physSchemaBuilder.physSchema,
                        alias);
                for (Mondrian3Def.RealOrCalcColumnDef columnDef
                    : xmlInlineTable.columnDefs.array)
                {
                    physInlineTable.columnsByName.put(
                        columnDef.name,
                        new RolapSchema.PhysRealColumn(
                            physInlineTable,
                            columnDef.name,
                            Dialect.Datatype.valueOf(columnDef.type),
                            jdbcColumn.getColumnSize()));
                }
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
                            physInlineTable, physSchemaBuilder.physSchema.dialect);
                    physView.ensurePopulated(
                        physSchemaBuilder.cube.schema,
                        xmlInlineTable, null);
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
                        physSchemaBuilder.cube.schema.getDialect());
                final RolapSchema.PhysView physView =
                    new RolapSchema.PhysView(
                        alias,
                        physSchemaBuilder.physSchema, physSchemaBuilder.getText(sql));
                physView.ensurePopulated(
                    physSchemaBuilder.cube.schema,
                    xmlView);
                physRelation = physView;
            } else {
                throw Util.needToImplement(
                    "translate xml table to phys table for table type"
                        + xmlRelation.getClass());
            }
            physSchemaBuilder.physSchema.tablesByName.put(alias, physRelation);
        }
        assert physRelation.getSchema() == physSchemaBuilder.physSchema;
        return physRelation;
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
         * @param table
         * @param map
         */
        private static RelNode lookup(
            MondrianDef.Relation table,
            Map<String, RelNode> map)
        {
            RelNode relNode;
            if (table instanceof MondrianDef.Table) {
                relNode = map.get(((MondrianDef.Table) table).name);
                if (relNode != null) {
                    return relNode;
                }
            }
            return map.get(table.getAlias());
        }

        private int depth;
        private String alias;
        private MondrianDef.Relation table;

        RelNode(String alias, int depth) {
            this.alias = alias;
            this.depth = depth;
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
        private RolapLevel measuresLevel;

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
            RolapLevel measuresLevel)
        {
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
                            virtualCube.measuresHierarchy,
                            Util.<RolapMember>cast(measuresFound))));

                Mondrian3Def.CalculatedMember xmlCalcMember =
                    schema.lookupXmlCalculatedMember(
                        calcMember.getUniqueName(),
                        baseCube.name);
                createCalcMembersAndNamedSets(
                    Collections.singletonList(xmlCalcMember),
                    Collections.<MondrianDef.NamedSet>emptyList(),
                    new ArrayList<RolapMember>(),
                    new ArrayList<Formula>(),
                    virtualCube,
                    false);
                return null;

            } else if (member instanceof RolapBaseCubeMeasure) {
                RolapBaseCubeMeasure baseMeasure =
                    (RolapBaseCubeMeasure) member;
                RolapMeasureGroup measureGroup = null;
                for (RolapMeasureGroup measureGroup2
                    : virtualCube.measureGroupList)
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
                        Collections.<String, Annotation>emptyMap());
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
}

// End RolapSchemaUpgrader.java
