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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
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
    RolapSchema schema;
    RolapHierarchy measuresHierarchy;
    /** For SQL generator. Fact table. */
    MondrianDef.Relation fact;
    /** To access all measures stored in the fact table. */
    CellReader cellReader;
    /**
     * Mapping such that
     * <code>localDimensionOrdinals[dimension.globalOrdinal]</code> is equal to
     * the ordinal of the dimension in this cube. See {@link
     * RolapDimension#topic_ordinals}
     */
    int[] localDimensionOrdinals;
    /** Schema reader which can see this cube and nothing else. */
    private SchemaReader schemaReader;
    /** List of calculated members. */
    private Formula[] calculatedMembers = new Formula[0];

    /** The hierarchies of this cube, in order of local ordinal. */
    private RolapHierarchy[] hierarchies;

    RolapCube(
        RolapSchema schema, MondrianDef.Schema xmlSchema,
        MondrianDef.Cube xmlCube)
    {
        this.schema = schema;
        this.name = xmlCube.name;
        this.fact = xmlCube.fact;
        if (fact.getAlias() == null) {
            throw Util.newError(
                    "Must specify alias for fact table of cube " +
                    getUniqueName());
        }
        this.dimensions = new RolapDimension[xmlCube.dimensions.length + 1];
        this.hierarchies = new RolapHierarchy[xmlCube.dimensions.length + 1];
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
            dimensions[i + 1] =
                getOrCreateDimension(xmlCubeDimension, schema, xmlSchema);
            hierarchies[i + 1] =
                (RolapHierarchy) dimensions[i + 1].getHierarchy();
        }
        RolapStoredMeasure measures[] = new RolapStoredMeasure[
            xmlCube.measures.length];
        for (int i = 0; i < xmlCube.measures.length; i++) {
            MondrianDef.Measure xmlMeasure = xmlCube.measures[i];
            final RolapStoredMeasure measure =
                    measures[i] =
                    new RolapStoredMeasure(
                            this, null, measuresLevel, xmlMeasure.name,
                            xmlMeasure.formatString, xmlMeasure.column,
                            xmlMeasure.aggregator);
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

            ArrayList propNames = new ArrayList();
            ArrayList propExprs = new ArrayList();
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
        init();
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
        String memberUniqueName, ArrayList propNames, ArrayList propExprs)
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
        ArrayList propNames = new ArrayList(),
            propExprs = new ArrayList();
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
            ArrayList propNames,
            ArrayList propExprs,
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

    /**
     * Creates a <code>RolapCube</code> from a virtual cube.
     **/
    RolapCube(
        RolapSchema schema, MondrianDef.Schema xmlSchema,
        MondrianDef.VirtualCube xmlVirtualCube)
    {
        this.schema = schema;
        this.name = xmlVirtualCube.name;
        this.fact = null;
        this.dimensions =
            new RolapDimension[xmlVirtualCube.dimensions.length + 1];
        this.hierarchies =
            new RolapHierarchy[xmlVirtualCube.dimensions.length + 1];
        RolapDimension measuresDimension = new RolapDimension(
            schema, Dimension.MEASURES_NAME, 0, DimensionType.StandardDimension);
        this.dimensions[0] = measuresDimension;
        this.measuresHierarchy = measuresDimension.newHierarchy(null, false);
        this.hierarchies[0] = measuresHierarchy;
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
            dimensions[i + 1] = new RolapDimension(
                schema, this, xmlDimension, xmlCubeDimension);
            hierarchies[i + 1] =
                (RolapHierarchy) dimensions[i + 1].getHierarchy();
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
        init();
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

    void init()
    {
        int max = -1;
        for (int i = 0; i < dimensions.length; i++) {
            final RolapDimension dimension = (RolapDimension) dimensions[i];
            dimension.init(this);
            max = Math.max(max, dimension.getGlobalOrdinal());
        }
        this.localDimensionOrdinals = new int[max + 1];
        Arrays.fill(localDimensionOrdinals, -1);
        for (int i = 0; i < dimensions.length; i++) {
            final RolapDimension dimension = (RolapDimension) dimensions[i];
            final int globalOrdinal = dimension.getGlobalOrdinal();
            Util.assertTrue(
                    localDimensionOrdinals[globalOrdinal] == -1,
                    "duplicate dimension globalOrdinal " + globalOrdinal);
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
        ArrayList list = new ArrayList();
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
        RolapDimension[] dimensions = (RolapDimension[]) this.getDimensions();
        for (int j = 0; j < dimensions.length; j++) {
            registerDimension(dimensions[j]);
        }
    }

    /**
     * Returns this cube's underlying star schema.
     */
    RolapStar getStar()
    {
        return schema.getRolapStarRegistry().getOrCreateStar(fact);
    }

    void registerDimension(RolapDimension dimension) {
        RolapStar star = getStar();
        RolapHierarchy[] hierarchies = (RolapHierarchy[])
                dimension.getHierarchies();
        HashMap mapLevelToColumn = (HashMap)
            star.mapCubeToMapLevelToColumn.get(this);
        if (mapLevelToColumn == null) {
            mapLevelToColumn = new HashMap();
            star.mapCubeToMapLevelToColumn.put(this, mapLevelToColumn);
        }
        HashMap mapLevelToNameColumn = (HashMap)
            star.mapCubeToMapLevelToNameColumn.get(this);
        if (mapLevelToNameColumn == null) {
            mapLevelToNameColumn = new HashMap();
            star.mapCubeToMapLevelToNameColumn.put(this, mapLevelToNameColumn);
        }
        for (int k = 0; k < hierarchies.length; k++) {
            RolapHierarchy hierarchy = hierarchies[k];
            HierarchyUsage hierarchyUsage = schema.getUsage(hierarchy,this);
            MondrianDef.Relation relation = hierarchy.getRelation();
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }
            RolapStar.Table table = star.factTable;
            if (!relation.equals(table.relation)) {
                // HierarchyUsage should have checked this.
                if (hierarchyUsage.foreignKey == null) {
                    throw MondrianResource.instance()
                            .newHierarchyMustHaveForeignKey(
                                    hierarchy.getName(), getName());
                }
                // jhyde: check is disabled until we handle <View> correctly
                if (false &&
                    !star.factTable.containsColumn(hierarchyUsage.foreignKey)) {
                    throw MondrianResource.instance()
                            .newHierarchyInvalidForeignKey(
                                    hierarchyUsage.foreignKey,
                                    hierarchy.getName(),
                                    getName());
                }
                RolapStar.Condition joinCondition = new RolapStar.Condition(
                        new MondrianDef.Column(table.getAlias(), hierarchyUsage.foreignKey),
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

        dimension.init(this);
        // add to dimensions array
        final int localOrdinal = dimensions.length;
        dimensions = (DimensionBase[])
            RolapUtil.addElement(dimensions, dimension);
        hierarchies = (RolapHierarchy[])
            RolapUtil.addElement(hierarchies, dimension.getHierarchy());

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

    // implement NameResolver
    public OlapElement lookupChild(OlapElement parent, String s) {
        // use OlapElement's virtual lookup
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
