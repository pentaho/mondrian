/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.rolap.agg.AggregationManager;
import mondrian.xom.DOMWrapper;
import mondrian.xom.XOMException;
import mondrian.xom.XOMUtil;

import org.apache.log4j.Logger;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

/**
 * <code>RolapCube</code> implements {@link Cube} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapCube extends CubeBase
{
    private static final Logger LOGGER = Logger.getLogger(RolapCube.class);

    private final RolapSchema schema;
    private RolapHierarchy measuresHierarchy;
    /** For SQL generator. Fact table. */
    final MondrianDef.Relation fact;
    /** To access all measures stored in the fact table. */
    private CellReader cellReader;
    /**
     * Mapping such that
     * <code>localDimensionOrdinals[dimension.globalOrdinal]</code> is equal to
     * the ordinal of the dimension in this cube. See {@link
     * RolapDimension#topic_ordinals}
     */
    private int[] localDimensionOrdinals;
    /** Schema reader which can see this cube and nothing else. */
    private SchemaReader schemaReader;
    /** List of calculated members. */
    private Formula[] calculatedMembers = new Formula[0];

    /** Contains {@link HierarchyUsage}s for this cube */
    private final List hierarchyUsages;


    /** 
     * private constructor used by both normal cubes and virtual cubes. 
     * 
     * @param schema 
     * @param name 
     * @param fact 
     */
    private RolapCube(RolapSchema schema, 
                      String name, 
                      MondrianDef.Relation fact) {

        super(name);
        this.schema = schema;
        this.fact = fact;
        this.hierarchyUsages = new ArrayList();
    }

    RolapCube(
        RolapSchema schema, MondrianDef.Schema xmlSchema,
        MondrianDef.Cube xmlCube)
    {
        this(schema, xmlCube.name, xmlCube.fact);

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube<init>: cube=" +this.name);
        }

        schema.addCube(this);

        if (fact.getAlias() == null) {
            throw Util.newError(
                    "Must specify alias for fact table of cube " +
                    getUniqueName());
        }
        this.dimensions = new RolapDimension[xmlCube.dimensions.length + 1];

        RolapDimension measuresDimension = new RolapDimension(schema,
            Dimension.MEASURES_NAME, 0, DimensionType.StandardDimension);
        this.dimensions[0] = measuresDimension;
        this.measuresHierarchy = measuresDimension.newHierarchy(null, false);
        if (!Util.isEmpty(xmlSchema.measuresCaption)) {
            measuresDimension.setCaption(xmlSchema.measuresCaption);
            this.measuresHierarchy.setCaption(xmlSchema.measuresCaption);
        }
        RolapLevel measuresLevel = this.measuresHierarchy.newLevel("MeasuresLevel", 0);
        for (int i = 0; i < xmlCube.dimensions.length; i++) {
            MondrianDef.CubeDimension xmlCubeDimension = xmlCube.dimensions[i];
            // Look up usages of shared dimensions in the schema before
            // consulting the XML schema (which may be null).
            RolapDimension dimension =
                getOrCreateDimension(xmlCubeDimension, schema, xmlSchema);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("RolapCube<init>: dimension="+dimension.getName());
            }
            this.dimensions[i + 1] = dimension;

            RolapHierarchy[] hierarchies =
                (RolapHierarchy[]) dimension.getHierarchies();
            for (int j = 0; j < hierarchies.length; j++) {
                RolapHierarchy hierarchy = hierarchies[j];
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("RolapCube<init>: hierarchy="+hierarchy.getName());
                }
                createUsage(hierarchy, xmlCubeDimension);
            }

        }

        RolapStoredMeasure measures[] = new RolapStoredMeasure[
            xmlCube.measures.length];
        for (int i = 0; i < xmlCube.measures.length; i++) {
            MondrianDef.Measure xmlMeasure = xmlCube.measures[i];

            final RolapStoredMeasure measure =
                    new RolapStoredMeasure(
                            this, null, measuresLevel, xmlMeasure.name,
                            xmlMeasure.formatString, xmlMeasure.column,
                            xmlMeasure.aggregator);
            measures[i] = measure;

            if (!Util.isEmpty(xmlMeasure.formatter)) {
                // there is a special cell formatter class
                try {
                    Class clazz = Class.forName(xmlMeasure.formatter);
                    Constructor ctor = clazz.getConstructor(new Class[0]);
                    CellFormatter cellFormatter = (CellFormatter) ctor.newInstance(new Object[0]);
                    measure.setFormatter(cellFormatter);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Set member's caption, if present.
            if (!Util.isEmpty(xmlMeasure.caption)) {
                // there is a special caption string
                measure.setProperty(Property.PROPERTY_CAPTION, xmlMeasure.caption);
            }

            // Set member's visibility, default true.
            Boolean visible = xmlMeasure.visible;
            if (visible == null) {
                visible = Boolean.TRUE;
            }
            measure.setProperty(Property.PROPERTY_VISIBLE, visible);

            List propNames = new ArrayList();
            List propExprs = new ArrayList();
            validateMemberProps(xmlMeasure.memberProperties, propNames,
                    propExprs, xmlMeasure.name);
            for (int j = 0; j < propNames.size(); j++) {
                String propName = (String) propNames.get(j);
                final Object propExpr = propExprs.get(j);
                measure.setProperty(propName, propExpr);
            }
        }

        this.measuresHierarchy.memberReader = new CacheMemberReader(
                new MeasureMemberSource(measuresHierarchy, measures));
        init(xmlCube.dimensions);
        init(xmlCube);
    }

    /**
     * Creates a <code>RolapCube</code> from a virtual cube.
     **/
    RolapCube(
        RolapSchema schema, MondrianDef.Schema xmlSchema,
        MondrianDef.VirtualCube xmlVirtualCube)
    {
        this(schema, xmlVirtualCube.name, null);

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube<init>: virtual cube=" +this.name);
        }

        this.dimensions =
            new RolapDimension[xmlVirtualCube.dimensions.length + 1];
        RolapDimension measuresDimension = new RolapDimension(
            schema, Dimension.MEASURES_NAME, 0, DimensionType.StandardDimension);
        this.dimensions[0] = measuresDimension;
        this.measuresHierarchy = measuresDimension.newHierarchy(null, false);
        if (!Util.isEmpty(xmlSchema.measuresCaption)) {
            measuresDimension.setCaption(xmlSchema.measuresCaption);
            this.measuresHierarchy.setCaption(xmlSchema.measuresCaption);
        }

        this.measuresHierarchy.newLevel("MeasuresLevel", 0);
        for (int i = 0; i < xmlVirtualCube.dimensions.length; i++) {
            MondrianDef.VirtualCubeDimension xmlCubeDimension =
                xmlVirtualCube.dimensions[i];
            MondrianDef.Dimension xmlDimension = xmlCubeDimension.getDimension(
                xmlSchema);
            RolapDimension dimension = new RolapDimension(
                schema, this, xmlDimension, xmlCubeDimension);
            dimensions[i + 1] = dimension;
        }

        RolapMeasure measures[] = new RolapMeasure[
            xmlVirtualCube.measures.length];
        for (int i = 0; i < xmlVirtualCube.measures.length; i++) {
            // Lookup a measure in an existing cube. (Don't know whether it
            // will confuse things that this measure still points to its 'real'
            // cube.)
            MondrianDef.VirtualCubeMeasure xmlMeasure =
                xmlVirtualCube.measures[i];
            RolapCube cube = (RolapCube) schema.lookupCube(xmlMeasure.cubeName);
            Member[] cubeMeasures = cube.getMeasures();
            for (int j = 0; j < cubeMeasures.length; j++) {
                if (cubeMeasures[j].getUniqueName().equals(xmlMeasure.name)) {
                    measures[i] = (RolapMeasure) cubeMeasures[j];
                    break;
                }
            }
            if (measures[i] == null) {
                throw Util.newInternal(
                    "could not find measure '" + xmlMeasure.name +
                    "' in cube '" + xmlMeasure.cubeName + "'");
            }
        }
        this.measuresHierarchy.memberReader = new CacheMemberReader(
            new MeasureMemberSource(measuresHierarchy, measures));
        init(xmlVirtualCube.dimensions);
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * Creates a dimension from its XML definition. If the XML definition is
     * a &lt;DimensionUsage&gt;, and the shared dimension is cached in the
     * schema, returns that.
     *
     * @param xmlCubeDimension XML Dimension or DimensionUsage
     * @param schema Schema
     * @param xmlSchema XML Schema
     * @return A dimension
     */
    private RolapDimension getOrCreateDimension(
        MondrianDef.CubeDimension xmlCubeDimension,
        RolapSchema schema,
        MondrianDef.Schema xmlSchema)
    {
        if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            MondrianDef.DimensionUsage usage =
                (MondrianDef.DimensionUsage) xmlCubeDimension;
            final RolapHierarchy sharedHierarchy =
                schema.getSharedHierarchy(usage.source);
            if (sharedHierarchy != null) {
                return (RolapDimension) sharedHierarchy.getDimension();
            }
        }
        MondrianDef.Dimension xmlDimension =
            xmlCubeDimension.getDimension(xmlSchema);
        return new RolapDimension(schema, this, xmlDimension,
            xmlCubeDimension);
    }

    private Formula parseFormula(String formulaString,
        String memberUniqueName, List propNames, List propExprs)
    {
        assert memberUniqueName.startsWith("[");
        RolapConnection conn = schema.getInternalConnection();
        StringBuffer buf = new StringBuffer(256);
        buf.append("WITH MEMBER ").append(memberUniqueName).append(" AS ")
            .append(formulaString);
        assert propNames.size() == propExprs.size();
        for (int i = 0; i < propNames.size(); i++) {
            String name = (String) propNames.get(i);
            String expr = (String) propExprs.get(i);
            buf.append(", ").append(name).append(" = ").append(expr);
        }
        buf.append(" SELECT FROM ")
            .append(Util.quoteMdxIdentifier(getUniqueName()));
        final String queryString = buf.toString();
        final Query queryExp;
        try {
            queryExp = conn.parseQuery(queryString);
        } catch (Exception e) {
            throw MondrianResource.instance().newCalcMemberHasBadFormula(
                memberUniqueName, getUniqueName(), e);
        }
        queryExp.resolve();
        Util.assertTrue(queryExp.formulas.length == 1);
        final Formula formula = queryExp.formulas[0];
        return formula;
    }

    /**
     * Post-initialization, doing things which cannot be done in the
     * constructor.
     */
    void init(MondrianDef.Cube xmlCube) {
        // Load calculated members. (Cannot do this in the constructor, because
        // cannot parse the generated query, because the schema has not been
        // set in the cube at this point.)
        for (int i = 0; i < xmlCube.calculatedMembers.length; i++) {
            MondrianDef.CalculatedMember xmlCalculatedMember =
                xmlCube.calculatedMembers[i];
            final Member member = createCalculatedMember(xmlCalculatedMember);
            Util.discard(member);
        }
    }

    private Member createCalculatedMember(
        MondrianDef.CalculatedMember xmlCalcMember)
    {
        // Lookup dimension
        final Dimension dimension =
            (Dimension) lookupDimension(xmlCalcMember.dimension);
        if (dimension == null) {
            throw MondrianResource.instance().newCalcMemberHasBadDimension(
                xmlCalcMember.dimension, xmlCalcMember.name,
                getUniqueName());
        }
        // Check there isn't another calc member with the same name and
        // dimension.
        for (int i = 0; i < calculatedMembers.length; i++) {
            Formula formula = calculatedMembers[i];
            if (formula.getName().equals(xmlCalcMember.name) &&
                formula.getMdxMember().getDimension().getName() ==
                dimension.getName()) {
                throw MondrianResource.instance().newCalcMemberNotUnique(
                    Util.makeFqName(dimension, xmlCalcMember.name),
                    getUniqueName());
            }
        }
        final String memberUniqueName = Util.makeFqName(
            dimension.getUniqueName(), xmlCalcMember.name);
        final MondrianDef.CalculatedMemberProperty[] xmlProperties =
            xmlCalcMember.memberProperties;
        List propNames = new ArrayList();
        List propExprs = new ArrayList();
        validateMemberProps(xmlProperties, propNames, propExprs,
                xmlCalcMember.name);

        final Formula formula = parseFormula(xmlCalcMember.formula,
            memberUniqueName, propNames, propExprs);
        calculatedMembers = (Formula[])
            RolapUtil.addElement(calculatedMembers, formula);

        Member member = formula.getMdxMember();

        Boolean visible = xmlCalcMember.visible;
        if (visible == null) {
            visible = Boolean.TRUE;
        }
        member.setProperty(Property.PROPERTY_VISIBLE, visible);
        
        if (xmlCalcMember.caption != null && xmlCalcMember.caption.length() > 0)
            member.setProperty(Property.PROPERTY_MEMBER_CAPTION, xmlCalcMember.caption);

        return formula.getMdxMember();
    }

    /**
     * Validates an array of member properties, and populates a list of names
     * and expressions, one for each property.
     *
     * @param xmlProperties Array of property definitions.
     * @param propNames Output array of property names.
     * @param propExprs Output array of property expressions.
     * @param memberName Name of member which the properties belong to.
     */
    private void validateMemberProps(
            final MondrianDef.CalculatedMemberProperty[] xmlProperties,
            List propNames,
            List propExprs,
            String memberName) {
        MemberProperty[] properties = new MemberProperty[xmlProperties.length];
        for (int i = 0; i < properties.length; i++) {
            final MondrianDef.CalculatedMemberProperty xmlProperty =
                    xmlProperties[i];
            if (xmlProperty.expression == null &&
                xmlProperty.value == null) {
                throw MondrianResource.instance()
                    .newNeitherExprNorValueForCalcMemberProperty(
                        xmlProperty.name,
                        memberName,
                        getUniqueName());
            }
            if (xmlProperty.expression != null &&
                xmlProperty.value != null) {
                throw MondrianResource.instance()
                    .newExprAndValueForMemberProperty(
                        xmlProperty.name,
                        memberName,
                        getUniqueName());
            }
            propNames.add(xmlProperty.name);
            if (xmlProperty.expression != null) {
                propExprs.add(xmlProperty.expression);
            } else {
                propExprs.add(Util.quoteForMdx(xmlProperty.value));
            }
        }
    }


    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns the schema reader which enforces the appropriate access-control
     * context.
     *
     * @post return != null
     * @see #getSchemaReader(Role)
     */
    synchronized SchemaReader getSchemaReader() {
        if (schemaReader == null) {
            schemaReader = getSchemaReader(null);
        }
        return schemaReader;
    }

    public SchemaReader getSchemaReader(Role role) {
        if (role == null) {
            role = schema.defaultRole.makeMutableClone();
            role.grant(this, Access.ALL);
        }
        return new RolapCubeSchemaReader(role);
    }

    MondrianDef.CubeDimension lookup(MondrianDef.CubeDimension[] xmlDimensions,
                                     String name) {
        for (int i = 0; i < xmlDimensions.length; i++) {
            MondrianDef.CubeDimension cd = xmlDimensions[i];
            if (name.equals(cd.name)) {
                return cd;
            }
        }
        // TODO: this ought to be a fatal error.
        return null;
    }

    void init(MondrianDef.CubeDimension[] xmlDimensions)
    {
        int max = -1;
        for (int i = 0; i < dimensions.length; i++) {
            final RolapDimension dimension = (RolapDimension) dimensions[i];
            dimension.init(this, lookup(xmlDimensions, dimension.getName()));
            max = Math.max(max, dimension.getGlobalOrdinal());
        }
        this.localDimensionOrdinals = new int[max + 1];
        Arrays.fill(localDimensionOrdinals, -1);
        for (int i = 0; i < dimensions.length; i++) {
            final RolapDimension dimension = (RolapDimension) dimensions[i];
            final int globalOrdinal = dimension.getGlobalOrdinal();
/*
When the same Dimension is in two or more DimensionUsages, then this
assert is not true.
            Util.assertTrue(
                    localDimensionOrdinals[globalOrdinal] == -1,
                    "duplicate dimension globalOrdinal " + globalOrdinal);
*/
            localDimensionOrdinals[globalOrdinal] = i;
        }
        this.cellReader = AggregationManager.instance();
        register();
    }

    void register()
    {
        if (isVirtual()) {
            return;
        }
        List list = new ArrayList();
        Member[] measures = getMeasures();
        for (int i = 0; i < measures.length; i++) {
            if (measures[i] instanceof RolapStoredMeasure) {
                list.add(measures[i]);
            }
        }
        RolapStoredMeasure[] storedMeasures = (RolapStoredMeasure[])
                list.toArray(new RolapStoredMeasure[list.size()]);
        RolapStar star = getStar();

        // create measures (and stars for them, if necessary)
        for (int i = 0; i < storedMeasures.length; i++) {
            RolapStoredMeasure storedMeasure = storedMeasures[i];
            RolapStar.Measure measure = new RolapStar.Measure();
            measure.table = star.factTable;
            measure.expression = storedMeasure.expression;
            measure.aggregator = storedMeasure.aggregator;
            measure.isNumeric = true;
            storedMeasure.starMeasure = measure; // reverse mapping
            star.factTable.columns.add(measure);
            star.mapColumnToName.put(measure, storedMeasure.getName());
        }

        // create dimension tables
        Dimension[] dimensions = this.getDimensions();
        for (int j = 0; j < dimensions.length; j++) {
            registerDimension(dimensions[j]);
        }
    }

    int getOrdinal(int globalOrdinal) {
        return this.localDimensionOrdinals[globalOrdinal];
    }

    CellReader getCellReader() {
        return this.cellReader;
    }

    /**
     * Returns this cube's underlying star schema.
     */
    RolapStar getStar()
    {
        return schema.getRolapStarRegistry().getOrCreateStar(fact);
    }

    synchronized void createUsage(RolapHierarchy hierarchy,
                    MondrianDef.CubeDimension cubeDim) {
        HierarchyUsage usage = new HierarchyUsage(this, hierarchy, cubeDim);

        // Set the hierarchy caption- this is used to display the alias
        // name rather than the source name in jpivot
        hierarchy.setCaption(usage.getName());

        Iterator it = this.hierarchyUsages.iterator();
        while (it.hasNext()) {
            HierarchyUsage hierUsage = (HierarchyUsage) it.next();
            if (hierUsage.equals(usage)) {
                getLogger().warn("RolapCube.createUsage: duplicate " +hierUsage);
                return;
            }
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.createUsage: register " +usage);
        }
        this.hierarchyUsages.add(usage);
    }
    private synchronized HierarchyUsage getUsageByName(String name) {
        Iterator it = this.hierarchyUsages.iterator();
        while (it.hasNext()) {
            HierarchyUsage hierUsage = (HierarchyUsage) it.next();
            if (hierUsage.getFullName().equals(name)) {
                return hierUsage;
            }
        }
        return null;
    }
    /** 
     * A Hierarchy may have one or more HierarchyUsages. This method returns 
     * an array holding the one or more usages associated with a Hierarchy.
     * The HierarchyUsages hierarchyName attribute always equals the name
     * attribute of the Hierarchy.
     *      
     * @param hierarchy 
     * @return an HierarchyUsages array with 0 or more members.
     */     
    synchronized HierarchyUsage[] getUsages(Hierarchy hierarchy) {
        String name = hierarchy.getName();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.getUsages: name="+name);
        }
        
        HierarchyUsage hierUsage = null;
        List list = null;
                
        Iterator it = this.hierarchyUsages.iterator();
        while (it.hasNext()) {
            HierarchyUsage hu = (HierarchyUsage) it.next();
            if (hu.getHierarchyName().equals(name)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsages: add list HierarchyUsage.name="+hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList();
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsages: add list HierarchyUsage.name="+hierUsage.getName());
                        getLogger().debug("RolapCube.getUsages: add list HierarchyUsage.name="+hu.getName());
                    }
                    list.add(hierUsage);
                    list.add(hu);
                    hierUsage = null;
                }
            }
        }
        if (hierUsage != null) {
            return new HierarchyUsage[] { hierUsage };
        } else if (list != null) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("RolapCube.getUsages: return list");
            }
            return (HierarchyUsage[])
                list.toArray(new HierarchyUsage[list.size()]);
        } else {
            return new HierarchyUsage[0];
        }
    }
    /** 
     * Lookup all of the HierarchyUsages with the same "source" returning
     * an array of HierarchyUsage of length 0 or more. 
     * 
     * @param source 
     * @return array of HierarchyUsage (HierarchyUsage[]) - never null.
     */
    synchronized HierarchyUsage[] getUsagesBySource(String source) {
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.getUsagesBySource: source="+source);
        }

        HierarchyUsage hierUsage = null;
        List list = null;

        Iterator it = this.hierarchyUsages.iterator();
        while (it.hasNext()) {
            HierarchyUsage hu = (HierarchyUsage) it.next();
            String s = hu.getSource();
            if ((s != null) && s.equals(source)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsagesBySource: add list HierarchyUsage.name="+hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList();
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsagesBySource: add list HierarchyUsage.name="+hierUsage.getName());
                        getLogger().debug("RolapCube.getUsagesBySource: add list HierarchyUsage.name="+hu.getName());
                    }
                    list.add(hierUsage);
                    list.add(hu);
                    hierUsage = null;
                }
            }
        }
        if (hierUsage != null) {
            return new HierarchyUsage[] { hierUsage };
        } else if (list != null) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("RolapCube.getUsagesBySource: return list");
            }
            return (HierarchyUsage[])
                list.toArray(new HierarchyUsage[list.size()]);
        } else {
            return new HierarchyUsage[0];
        }
    }


    void registerDimension(Dimension dimension) {
        RolapStar star = getStar();
        Map mapLevelToColumn = star.getMapLevelToColumn(this);
        Map mapLevelToNameColumn = star.getMapLevelToNameColumn(this);

        Hierarchy[] hierarchies = dimension.getHierarchies();

        for (int k = 0; k < hierarchies.length; k++) {
            RolapHierarchy hierarchy = (RolapHierarchy) hierarchies[k];

            MondrianDef.Relation relation = hierarchy.getRelation();
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }

            HierarchyUsage[] hierarchyUsages = getUsages(hierarchy);
            if (hierarchyUsages.length == 0) {
                if (getLogger().isDebugEnabled()) {
                    StringBuffer buf = new StringBuffer(64);
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

            for (int j = 0; j < hierarchyUsages.length; j++) {
                HierarchyUsage hierarchyUsage = hierarchyUsages[j];

                RolapStar.Table table = star.factTable;
                if (!relation.equals(table.relation)) {
                    // HierarchyUsage should have checked this.
                    if (hierarchyUsage.getForeignKey() == null) {
                        throw MondrianResource.instance()
                                .newHierarchyMustHaveForeignKey(
                                        hierarchy.getName(), getName());
                    }
                    // jhyde: check is disabled until we handle <View> correctly
                    if (false &&
                        !star.factTable.containsColumn(hierarchyUsage.getForeignKey())) {
                        throw MondrianResource.instance()
                                .newHierarchyInvalidForeignKey(
                                        hierarchyUsage.getForeignKey(),
                                        hierarchy.getName(),
                                        getName());
                    }
                    RolapStar.Condition joinCondition = new RolapStar.Condition(
                            new MondrianDef.Column(table.getAlias(), 
                            hierarchyUsage.getForeignKey()),
                            hierarchyUsage.joinExp);
                    table = table.addJoin(relation, joinCondition);
                }
                RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
                for (int l = 0; l < levels.length; l++) {
                    RolapLevel level = levels[l];
                    if (level.keyExp == null) {
                        continue;
                    } else {
                        RolapStar.Column column = makeColumnForLevelExpr(level,
                            table, level.keyExp);
                        column.isNumeric = (level.flags & RolapLevel.NUMERIC) != 0;
                        table.columns.add(column);
                        mapLevelToColumn.put(level, column);
                        if (level.nameExp != null) {
                            final RolapStar.Column nameColumn =
                                makeColumnForLevelExpr(level, table, level.nameExp);
                            table.columns.add(nameColumn);
                            mapLevelToNameColumn.put(level, nameColumn);
                            star.mapColumnToName.put(nameColumn, level.getName());
                            star.mapColumnToName.put(column, level.getName() + " (Key)");
                        } else {
                            star.mapColumnToName.put(column, level.getName());
                        }
                    }
                }
            }
        }
    }

    private RolapStar.Column makeColumnForLevelExpr(
        RolapLevel level,
        RolapStar.Table table,
        MondrianDef.Expression xmlExpr)
    {
        RolapStar.Column column = new RolapStar.Column();
        if (xmlExpr instanceof MondrianDef.Column) {
            final MondrianDef.Column xmlColumn = (MondrianDef.Column) xmlExpr;
            String tableName = xmlColumn.table;
            column.table = table.findAncestor(tableName);
            if (column.table == null) {
                throw Util.newError(
                        "Level '" + level.getUniqueName() +
                        "' of cube '" + this +
                        "' is invalid: table '" + tableName +
                        "' is not found in current scope");
            }
            RolapStar.AliasReplacer aliasReplacer =
                    new RolapStar.AliasReplacer(tableName,
                            column.table.getAlias());
            xmlExpr = aliasReplacer.visit(xmlExpr);
        } else {
            column.table = table;
        }
        column.expression = xmlExpr;
        return column;
    }

    public Member[] getMembersForQuery(String query, List calcMembers) {
        throw new UnsupportedOperationException();
    }

    Member[] getMeasures() {
        Level measuresLevel = dimensions[0].getHierarchies()[0].getLevels()[0];
        return getSchemaReader().getLevelMembers(measuresLevel);
    }

    /**
     * Returns this cube's fact table, null if the cube is virtual.
     * @return
     */
    MondrianDef.Relation getFact() {
        return fact;
    }

    /**
     * Returns whether this cube is virtual. We use the fact that virtual cubes
     * do not have fact tables.
     **/
    boolean isVirtual() {
        return fact == null;
    }

    RolapDimension createDimension(MondrianDef.CubeDimension xmlCubeDimension) {
        MondrianDef.Dimension xmlDimension = null;
        final RolapDimension dimension;
        if (xmlCubeDimension instanceof MondrianDef.Dimension) {
            xmlDimension = (MondrianDef.Dimension) xmlCubeDimension;
            dimension = new RolapDimension(schema, this, xmlDimension,
                xmlCubeDimension);
        } else if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            final MondrianDef.DimensionUsage usage =
                (MondrianDef.DimensionUsage) xmlCubeDimension;
            final RolapHierarchy sharedHierarchy =
                ((RolapSchema) schema).getSharedHierarchy(usage.source);
            if (sharedHierarchy == null) {
                throw MondrianResource.instance().newInternal(
                    "todo: Shared hierarchy '" + usage.source +
                    "' not found");
            }

            final RolapDimension sharedDimension =
                (RolapDimension) sharedHierarchy.getDimension();
            dimension = sharedDimension.copy(this, usage.name,
                xmlCubeDimension);
        } else {
            throw Util.newInternal("Unexpected subtype, " + xmlCubeDimension);
        }

        dimension.init(this, xmlCubeDimension);
        // add to dimensions array
        final int localOrdinal = dimensions.length;
        dimensions = (DimensionBase[])
            RolapUtil.addElement(dimensions, dimension);

        RolapHierarchy hierarchy = (RolapHierarchy) dimension.getHierarchy();
        createUsage(hierarchy, xmlCubeDimension);


        // add to ordinals array
        final int globalOrdinal = dimension.getGlobalOrdinal();
        if (globalOrdinal >= localDimensionOrdinals.length) {
            int[] newLocalDimensionOrdinals = new int[globalOrdinal + 1];
            System.arraycopy(localDimensionOrdinals, 0,
                    newLocalDimensionOrdinals, 0, localDimensionOrdinals.length);
            Arrays.fill(newLocalDimensionOrdinals,
                    localDimensionOrdinals.length,
                    newLocalDimensionOrdinals.length, -1);
            this.localDimensionOrdinals = newLocalDimensionOrdinals;
        }
        Util.assertTrue(localDimensionOrdinals[globalOrdinal] == -1);
        localDimensionOrdinals[globalOrdinal] = localOrdinal;
        registerDimension(dimension);
        return dimension;
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String s)
    {
        OlapElement oe = null;
        String status = null;
        if (s.equals("Measures")) {
            // A Measure is never aliased, so just get it
            // Note if one calls getUsageByName with "Measures" as the value
            // it will return null so one must either do this or check for
            // a null HierarchyUsage below and disambiguate.
            oe = super.lookupChild(schemaReader, s);

        } else {
            // At this point everything ought to have a HierarchyUsage.
            //
            // If one does not exist (null), then odds are one is using
            // the base/source hierarchy name in a calculated measure
            // for a shared hierarchy - which is not legal.
            // But there are cases where its OK here not to have a
            // usage, i.e., the child is a measure, member or level
            // (not a RolapDimension), or the cube is virtual.
            // (Are those the only cases??)
            //
            // On the other hand, if the HierarchyUsage is shared, then
            // use the source name. 
            //
            // Lastly if the HierarchyUsage is not shared, then there is
            // no aliasing so just use the value.

            HierarchyUsage hierUsage = getUsageByName(s);

            if (hierUsage == null) {
                oe = super.lookupChild(schemaReader, s);
                status = "hierUsage == null";

                // Let us see if one is using the source name of a
                // usage rather than the alias name.
                if (oe instanceof RolapDimension) {
                    HierarchyUsage[] usages = getUsagesBySource(s);

                    if (usages.length > 0) {
                        StringBuffer buf = new StringBuffer(64);
                        buf.append("RolapCube.lookupChild: ");
                        buf.append("In cube \"");
                        buf.append(getName());
                        buf.append("\" use of unaliased Dimension name \"");
                        buf.append(s);

                        if (usages.length == 1) {
                            // ERROR: this will work but is bad coding
                            buf.append("\" rather than the alias name ");
                            buf.append("\"");
                            buf.append(usages[0].getName());
                            buf.append("\" ");
                            getLogger().error(buf.toString());
                            throw new MondrianException(buf.toString());
                        } else {
                            // ERROR: this is not allowed 
                            buf.append("\" rather than one of the alias names "); 
                            for (int i = 0; i < usages.length; i++) {
                                buf.append("\"");
                                buf.append(usages[i].getName());
                                buf.append("\" ");
                            }
                            getLogger().error(buf.toString());
                            throw new MondrianException(buf.toString());
                        }
                    }
                }
            } else if (hierUsage.isShared()) {
                status = "hierUsage == shared";
                // Shared, use source
                String source = hierUsage.getSource();
                oe = super.lookupChild(schemaReader, source);

            } else {
                status = "hierUsage == not shared";
                // Not shared, cool man
                oe = super.lookupChild(schemaReader, s);
            }
        }
        if (getLogger().isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("RolapCube.lookupChild: ");
            buf.append("name=");
            buf.append(getName());
            buf.append(", childname=");
            buf.append(s);
            if (status != null) {
                buf.append(", status=");
                buf.append(status);
            }
            if (oe == null) {
                buf.append(" returning null");
            } else {
                buf.append(" returning elementname="+oe.getName());
            }
            getLogger().debug(buf.toString());
        }

        return oe;
    }

    // implement NameResolver
    public OlapElement lookupChild(OlapElement parent, String s) {
        // use OlapElement's virtual lookup
System.out.println("RolapCube.lookupChild OlapElement: DOES THIS EVER GET CALLED s="+s);
        return parent.lookupChild(getSchemaReader(), s);
    }

    /**
     * Returns the the measures hierarchy.
     */
    public Hierarchy getMeasuresHierarchy(){
        return measuresHierarchy;
    }

    public Member createCalculatedMember(String xml) {
        MondrianDef.CalculatedMember xmlCalcMember;
        try {
            final mondrian.xom.Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("CalculatedMember")) {
                xmlCalcMember = new MondrianDef.CalculatedMember(def);
            } else {
                throw new XOMException("Got <" + tagName +
                    "> when expecting <CalculatedMember>");
            }
        } catch (XOMException e) {
            throw Util.newError(e,
                "Error while creating calculated member from XML [" +
                xml + "]");
        }
        return createCalculatedMember(xmlCalcMember);
    }

    /**
     * Schema reader which works from the perspective of a particular cube
     * (and hence includes calculated members defined in that cube) and also
     * applies the access-rights of a given role.
     */
    private class RolapCubeSchemaReader extends RolapSchemaReader {
        public RolapCubeSchemaReader(Role role) {
            super(role, schema);
            assert role != null : "precondition: role != null";
        }

        public Member[] getLevelMembers(Level level) {
            Member[] members = super.getLevelMembers(level);
            List calcMembers = getCalculatedMembers(level.getHierarchy());
            for (int i = 0; i < calcMembers.size(); i++) {
                Member member = (Member) calcMembers.get(i);
                if (member.getLevel().equals(level)) {
                    members = (Member[]) RolapUtil.addElement(members, member);
                }
            }
            return members;
        }

        public Member getCalculatedMember(String[] nameParts) {
            final String uniqueName = Util.implode(nameParts);
            for (int i = 0; i < calculatedMembers.length; i++) {
                Formula formula = calculatedMembers[i];
                final String formulaUniqueName =
                    formula.getMdxMember().getUniqueName();
                if (formulaUniqueName.equals(uniqueName)) {
                    return formula.getMdxMember();
                }
            }
            return null;
        }

        public List getCalculatedMembers(Hierarchy hierarchy) {
            ArrayList list = new ArrayList();
            for (int i = 0; i < calculatedMembers.length; i++) {
                Formula formula = calculatedMembers[i];
                if (formula.getMdxMember().getHierarchy().equals(hierarchy)) {
                    list.add(formula.getMdxMember());
                }
            }
            return list;
        }

        public Member getMemberByUniqueName(
            String[] uniqueNameParts,
            boolean failIfNotFound)
        {
            return (Member) lookupCompound(RolapCube.this, uniqueNameParts,
                failIfNotFound, Category.Member);
        }

        public Cube getCube() {
            return RolapCube.this;
        }
    }
}

// End RolapCube.java
