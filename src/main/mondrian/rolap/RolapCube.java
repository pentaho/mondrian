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
import mondrian.rolap.aggmatcher.ExplicitRules;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;
import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Set;

/**
 * <code>RolapCube</code> implements {@link Cube} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public class RolapCube extends CubeBase {

    private static final Logger LOGGER = Logger.getLogger(RolapCube.class);

    private final RolapSchema schema;
    private final RolapHierarchy measuresHierarchy;
    /** For SQL generator. Fact table. */
    final MondrianDef.Relation fact;
    /** To access all measures stored in the fact table. */
    private final CellReader cellReader;
    /**
     * Mapping such that
     * <code>localDimensionOrdinals[dimension.globalOrdinal]</code> is equal to
     * the ordinal of the dimension in this cube. See {@link
     * RolapDimension#topic_ordinals}
     */
    private int[] localDimensionOrdinals;
    /** Schema reader which can see this cube and nothing else. */
    private SchemaReader schemaReader;
    /**
     * List of calculated members.
     */
    private Formula[] calculatedMembers;

    /**
     * List of named sets.
     */
    private Formula[] namedSets;

    /** Contains {@link HierarchyUsage}s for this cube */
    private final List hierarchyUsages;

    private RolapStar star;
    private ExplicitRules.Group aggGroup;

    /**
     * private constructor used by both normal cubes and virtual cubes.
     *
     * @param schema
     * @param name
     * @param fact
     */
    private RolapCube(RolapSchema schema,
                      MondrianDef.Schema xmlSchema,
                      String name,
                      boolean cache,
                      MondrianDef.Relation fact,
                      MondrianDef.CubeDimension[] dimensions) {
        super(name, new RolapDimension[dimensions.length + 1]);

        this.schema = schema;
        this.fact = fact;
        this.hierarchyUsages = new ArrayList();
        this.cellReader = AggregationManager.instance();
        this.calculatedMembers = new Formula[0];
        this.namedSets = new Formula[0];

        if (! isVirtual()) {
            this.star = schema.getRolapStarRegistry().getOrCreateStar(fact);
            // only set if different from default (so that if two cubes share
            // the same fact table, either can turn off caching and both are
            // effected).
            if (! cache) {
                star.setCacheAggregations(cache);
            }
        }

        if (getLogger().isDebugEnabled()) {
            if (isVirtual()) {
                getLogger().debug("RolapCube<init>: virtual cube=" +this.name);
            } else {
                getLogger().debug("RolapCube<init>: cube=" +this.name);
            }
        }

        RolapDimension measuresDimension = new RolapDimension(
                schema,
                Dimension.MEASURES_NAME,
                0,
                DimensionType.StandardDimension);

        this.dimensions[0] = measuresDimension;

        this.measuresHierarchy = measuresDimension.newHierarchy(null, false);

        if (!Util.isEmpty(xmlSchema.measuresCaption)) {
            measuresDimension.setCaption(xmlSchema.measuresCaption);
            this.measuresHierarchy.setCaption(xmlSchema.measuresCaption);
        }

        for (int i = 0; i < dimensions.length; i++) {
            MondrianDef.CubeDimension xmlCubeDimension = dimensions[i];
            // Look up usages of shared dimensions in the schema before
            // consulting the XML schema (which may be null).
            RolapDimension dimension =
                getOrCreateDimension(xmlCubeDimension, schema, xmlSchema);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("RolapCube<init>: dimension="
                    +dimension.getName());
            }
            this.dimensions[i + 1] = dimension;

            if (! isVirtual()) {
                createUsages(dimension, xmlCubeDimension);
            }

        }

        schema.addCube(this);
    }

    /**
     * Creates a <code>RolapCube</code> from a regular cube.
     **/
    RolapCube(RolapSchema schema,
              MondrianDef.Schema xmlSchema,
              MondrianDef.Cube xmlCube) {
        this(schema, xmlSchema, xmlCube.name, xmlCube.cache.booleanValue(),
            xmlCube.fact, xmlCube.dimensions);

        if (fact.getAlias() == null) {
            throw Util.newError(
                    "Must specify alias for fact table of cube " +
                    getUniqueName());
        }


        // since MondrianDef.Measure and MondrianDef.VirtualCubeMeasure
        // can not be treated as the same, measure creation can not be
        // done in a common constructor.
        RolapLevel measuresLevel =
            this.measuresHierarchy.newLevel("MeasuresLevel", 0);

        RolapStoredMeasure measures[] = new RolapStoredMeasure[
            xmlCube.measures.length];
        for (int i = 0; i < xmlCube.measures.length; i++) {
            MondrianDef.Measure xmlMeasure = xmlCube.measures[i];

            final RolapStoredMeasure measure = new RolapStoredMeasure(
                            this, null, measuresLevel, xmlMeasure.name,
                            xmlMeasure.formatString, xmlMeasure.column,
                            xmlMeasure.aggregator);
            measures[i] = measure;

            if (!Util.isEmpty(xmlMeasure.formatter)) {
                // there is a special cell formatter class
                try {
                    Class clazz = Class.forName(xmlMeasure.formatter);
                    Constructor ctor = clazz.getConstructor(new Class[0]);
                    CellFormatter cellFormatter =
                        (CellFormatter) ctor.newInstance(new Object[0]);
                    measure.setFormatter(cellFormatter);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Set member's caption, if present.
            if (!Util.isEmpty(xmlMeasure.caption)) {
                // there is a special caption string
                measure.setProperty(
                        Property.CAPTION.name,
                        xmlMeasure.caption);
            }

            // Set member's visibility, default true.
            Boolean visible = xmlMeasure.visible;
            if (visible == null) {
                visible = Boolean.TRUE;
            }
            measure.setProperty(Property.VISIBLE.name, visible);

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
                new MeasureMemberSource(this.measuresHierarchy, measures));
        init(xmlCube.dimensions);
        init(xmlCube);

        loadAggGroup(xmlCube);
    }

    /**
     * Creates a <code>RolapCube</code> from a virtual cube.
     **/
    RolapCube(RolapSchema schema,
              MondrianDef.Schema xmlSchema,
              MondrianDef.VirtualCube xmlVirtualCube) {
        this(schema, xmlSchema, xmlVirtualCube.name, true,
            null, xmlVirtualCube.dimensions);


        // since MondrianDef.Measure and MondrianDef.VirtualCubeMeasure
        // can not be treated as the same, measure creation can not be
        // done in a common constructor.
        this.measuresHierarchy.newLevel("MeasuresLevel", 0);

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
            new MeasureMemberSource(this.measuresHierarchy, measures));
        init(xmlVirtualCube.dimensions);

        // Note: virtual cubes do not get aggregate
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public boolean hasAggGroup() {
        return (aggGroup != null);
    }
    public ExplicitRules.Group getAggGroup() {
        return aggGroup;
    }
    void loadAggGroup(MondrianDef.Cube xmlCube) {
        aggGroup = ExplicitRules.Group.make(this, xmlCube);
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
        MondrianDef.Schema xmlSchema) {

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

    /**
     * Post-initialization, doing things which cannot be done in the
     * constructor.
     */
    private void init(MondrianDef.Cube xmlCube) {
        // Load calculated members and named sets.
        // (We cannot do this in the constructor, because
        // cannot parse the generated query, because the schema has not been
        // set in the cube at this point.)
        List memberList = new ArrayList();
        List formulaList = new ArrayList();
        createCalcMembersAndNamedSets(
                xmlCube.calculatedMembers, xmlCube.namedSets,
                memberList, formulaList);
    }

    /**
     * Adds a collection of calculated members and named sets to this cube.
     * The members and sets can refer to each other.
     *
     * @param xmlCalcMembers XML objects representing members
     * @param xmlNamedSets Array of XML definition of named set
     * @param memberList Output list of {@link Member} objects
     * @param formulaList Output list of {@link Formula} objects
     */
    private void createCalcMembersAndNamedSets(
            MondrianDef.CalculatedMember[] xmlCalcMembers,
            MondrianDef.NamedSet[] xmlNamedSets,
            List memberList,
            List formulaList) {
        // If there are no objects to create, our generated SQL will so silly
        // the parser will laugh.
        if (xmlCalcMembers.length == 0 &&
                xmlNamedSets.length == 0) {
            return;
        }

        StringBuffer buf = new StringBuffer(256);
        buf.append("WITH").append(Util.nl);

        // Check the members individually, and generate SQL.
        for (int i = 0; i < xmlCalcMembers.length; i++) {
            preCalcMember(xmlCalcMembers, i, buf);
        }

        // Check the named sets individually (for uniqueness) and generate SQL.
        Set nameSet = new HashSet();
        for (int i = 0; i < namedSets.length; i++) {
            Formula namedSet = namedSets[i];
            nameSet.add(namedSet.getName());
        }
        for (int i = 0; i < xmlNamedSets.length; i++) {
            preNamedSet(xmlNamedSets[i], nameSet, buf);
        }

        buf.append("SELECT FROM ")
            .append(Util.quoteMdxIdentifier(getUniqueName()));

        // Parse and validate this huge MDX query we've created.
        final String queryString = buf.toString();
        final Query queryExp;
        try {
            RolapConnection conn = schema.getInternalConnection();
            queryExp = conn.parseQuery(queryString);
        } catch (Exception e) {
            throw MondrianResource.instance().UnknownNamedSetHasBadFormula.ex(
                getUniqueName(), e);
        }
        queryExp.resolve();

        // Now pick through the formulas.
        Util.assertTrue(queryExp.formulas.length ==
                xmlCalcMembers.length + xmlNamedSets.length);
        for (int i = 0; i < xmlCalcMembers.length; i++) {
            postCalcMember(xmlCalcMembers, i, queryExp, memberList);
        }
        for (int i = 0; i < xmlNamedSets.length; i++) {
            postNamedSet(xmlNamedSets, xmlCalcMembers.length, i, queryExp, formulaList);
        }
    }

    private void postNamedSet(
            MondrianDef.NamedSet[] xmlNamedSets,
            final int offset, int i,
            final Query queryExp,
            List formulaList) {
        MondrianDef.NamedSet xmlNamedSet = xmlNamedSets[i];
        Util.discard(xmlNamedSet);
        Formula formula = queryExp.formulas[offset + i];
        namedSets = (Formula[]) RolapUtil.addElement(namedSets, formula);
        formulaList.add(formula);
    }

    private void preNamedSet(
            MondrianDef.NamedSet xmlNamedSet,
            Set nameSet,
            StringBuffer buf) {
        if (!nameSet.add(xmlNamedSet.name)) {
            throw MondrianResource.instance().NamedSetNotUnique.ex(
                    xmlNamedSet.name, getUniqueName());
        }

        buf.append("SET ")
                .append(Util.makeFqName(xmlNamedSet.name))
                .append(Util.nl)
                .append(" AS '")
                .append(xmlNamedSet.getFormula())
                .append("'")
                .append(Util.nl);
    }

    private void postCalcMember(
            MondrianDef.CalculatedMember[] xmlCalcMembers,
            int i,
            final Query queryExp,
            List memberList) {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers[i];
        final Formula formula = queryExp.formulas[i];

        calculatedMembers = (Formula[])
                RolapUtil.addElement(calculatedMembers, formula);

        Member member = formula.getMdxMember();

        Boolean visible = xmlCalcMember.visible;
        if (visible == null) {
            visible = Boolean.TRUE;
        }
        member.setProperty(Property.VISIBLE.name, visible);

        if ((xmlCalcMember.caption != null) &&
                xmlCalcMember.caption.length() > 0) {
            member.setProperty(
                    Property.CAPTION.name,
                    xmlCalcMember.caption);
        }

        memberList.add(formula.getMdxMember());
    }

    private void preCalcMember(
            MondrianDef.CalculatedMember[] xmlCalcMembers,
            int j,
            StringBuffer buf) {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers[j];

        // Lookup dimension
        final Dimension dimension =
                (Dimension) lookupDimension(xmlCalcMember.dimension);
        if (dimension == null) {
            throw MondrianResource.instance().CalcMemberHasBadDimension.ex(
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

                throw MondrianResource.instance().CalcMemberNotUnique.ex(
                        Util.makeFqName(dimension, xmlCalcMember.name),
                        getUniqueName());
            }
        }

        // Check this calc member doesn't clash with one earlier in this
        // batch.
        for (int k = 0; k < j; k++) {
            MondrianDef.CalculatedMember xmlCalcMember2 = xmlCalcMembers[k];
            if (xmlCalcMember2.name.equals(xmlCalcMember.name) &&
                    xmlCalcMember2.dimension.equals(xmlCalcMember.dimension)) {
                throw MondrianResource.instance().CalcMemberNotUnique.ex(
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

        // Generate SQL.
        assert memberUniqueName.startsWith("[");
        buf.append("MEMBER ").append(memberUniqueName)
                .append(Util.nl)
                .append("  AS ").append(xmlCalcMember.getFormula());

        assert propNames.size() == propExprs.size();

        for (int i = 0; i < propNames.size(); i++) {
            String name = (String) propNames.get(i);
            String expr = (String) propExprs.get(i);
            buf.append(",").append(Util.nl)
                    .append(name).append(" = ").append(expr);
        }
        buf.append(Util.nl);
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
                    .NeitherExprNorValueForCalcMemberProperty.ex(
                        xmlProperty.name,
                        memberName,
                        getUniqueName());
            }
            if (xmlProperty.expression != null &&
                xmlProperty.value != null) {

                throw MondrianResource.instance()
                    .ExprAndValueForMemberProperty.ex(
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
    public synchronized SchemaReader getSchemaReader() {
        if (schemaReader == null) {
            schemaReader = getSchemaReader(null);
        }
        return schemaReader;
    }

    public SchemaReader getSchemaReader(Role role) {
        if (role == null) {
            role = schema.getDefaultRole().makeMutableClone();
            role.grant(this, Access.ALL);
        }
        return new RolapCubeSchemaReader(role);
    }

    MondrianDef.CubeDimension lookup(
            MondrianDef.CubeDimension[] xmlDimensions,
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

    private void init(MondrianDef.CubeDimension[] xmlDimensions) {
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
        register();
    }

    private void register() {
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
        RolapStar.Table table = star.getFactTable();

        // create measures (and stars for them, if necessary)
        for (int i = 0; i < storedMeasures.length; i++) {
            RolapStoredMeasure storedMeasure = storedMeasures[i];
            table.makeMeasure(storedMeasure);
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

    public boolean isCache() {
        return (isVirtual()) ? true : star.isCacheAggregations();
    }

    public void setCache(boolean cache) {
        if (! isVirtual()) {
            star.setCacheAggregations(cache);
        }
    }

    public void clearCache() {
        if (isVirtual()) {
            // Currently a virtual cube does not keep a list of all of its
            // base cubes, so we must just flush all of them.
            schema.flushRolapStarCaches();
        } else {
            star.clearCache();
        }
    }


    /**
     * Returns this cube's underlying star schema.
     */
    public RolapStar getStar() {
        return star;
    }

    private void createUsages(RolapDimension dimension,
            MondrianDef.CubeDimension xmlCubeDimension) {
        // RME level may not be in all hierarchies
        // If one uses the DimensionUsage attribute "level", which level
        // in a hierarchy to join on, and there is more than one hierarchy,
        // then a HierarchyUsage can not be created for the hierarchies
        // that do not have the level defined.
        RolapHierarchy[] hierarchies =
            (RolapHierarchy[]) dimension.getHierarchies();

        if (hierarchies.length == 1) {
            // Only one, so let lower level error checking handle problems
            createUsage(hierarchies[0], xmlCubeDimension);

        } else if ((xmlCubeDimension instanceof MondrianDef.DimensionUsage) &&
            (((MondrianDef.DimensionUsage) xmlCubeDimension).level != null)) {
            // More than one, make sure if we are joining by level, that
            // at least one hierarchy can and those that can not are
            // not registered
            MondrianDef.DimensionUsage du =
                (MondrianDef.DimensionUsage) xmlCubeDimension;

            int cnt = 0;

            for (int j = 0; j < hierarchies.length; j++) {
                RolapHierarchy hierarchy = hierarchies[j];
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("RolapCube<init>: hierarchy="
                        +hierarchy.getName());
                }
                RolapLevel joinLevel = (RolapLevel)
                                Util.lookupHierarchyLevel(hierarchy, du.level);
                if (joinLevel == null) {
                    continue;
                }
                createUsage(hierarchy, xmlCubeDimension);
                cnt++;
            }

            if (cnt == 0) {
                // None of the hierarchies had the level, let lower level
                // detect and throw error
                createUsage(hierarchies[0], xmlCubeDimension);
            }

        } else {
            // just do it
            for (int j = 0; j < hierarchies.length; j++) {
                RolapHierarchy hierarchy = hierarchies[j];
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("RolapCube<init>: hierarchy="
                        +hierarchy.getName());
                }
                createUsage(hierarchy, xmlCubeDimension);
            }
        }
    }

    synchronized void createUsage(
            RolapHierarchy hierarchy,
            MondrianDef.CubeDimension cubeDim) {
        HierarchyUsage usage = new HierarchyUsage(this, hierarchy, cubeDim);

        for (Iterator it = hierarchyUsages.iterator(); it.hasNext(); ) {
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
        for (Iterator it = hierarchyUsages.iterator(); it.hasNext(); ) {
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
    public synchronized HierarchyUsage[] getUsages(Hierarchy hierarchy) {
        String name = hierarchy.getName();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.getUsages: name="+name);
        }

        HierarchyUsage hierUsage = null;
        List list = null;

        for (Iterator it = hierarchyUsages.iterator(); it.hasNext(); ) {
            HierarchyUsage hu = (HierarchyUsage) it.next();
            if (hu.getHierarchyName().equals(name)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsages: "
                            +"add list HierarchyUsage.name="+hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList();
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsages: "
                            + "add list hierUsage.name="
                            + hierUsage.getName()
                            + ", hu.name="
                            + hu.getName());
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
     * Looks up all of the HierarchyUsages with the same "source" returning
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

        for (Iterator it = hierarchyUsages.iterator(); it.hasNext(); ) {
            HierarchyUsage hu = (HierarchyUsage) it.next();
            String s = hu.getSource();
            if ((s != null) && s.equals(source)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsagesBySource: "
                            + "add list HierarchyUsage.name="
                            + hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList();
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsagesBySource: "
                            + "add list hierUsage.name="
                            + hierUsage.getName()
                            + ", hu.name="
                            + hu.getName());
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


    /**
     * Understand this and you are no longer a novice.
     *
     * @param dimension
     */
    void registerDimension(Dimension dimension) {
        RolapStar star = getStar();

        Hierarchy[] hierarchies = dimension.getHierarchies();

        for (int k = 0; k < hierarchies.length; k++) {
            RolapHierarchy hierarchy = (RolapHierarchy) hierarchies[k];

            MondrianDef.Relation relation = hierarchy.getRelation();
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }
            RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();

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
                    MondrianDef.Relation relationTmp1 = relation;

                    relation = reorder(relation, levels);

                    if (relation == null && getLogger().isDebugEnabled()) {
                    	getLogger().debug("RolapCube.registerDimension: after reorder relation==null");
                    	getLogger().debug("RolapCube.registerDimension: reorder relationTmp1="
                    						+format(relationTmp1));
                    }
                }

                MondrianDef.Relation relationTmp2 = relation;

                if (levelName != null) {
                    //System.out.println("RolapCube.registerDimension: levelName=" +levelName);
                    // When relation is a table, this does nothing. Otherwise
                    // it tries to arrange the joins so that the fact table
                    // in the RolapStar will be joining at the lowest level.
                    //

                    // Make sure the level exists
                    RolapLevel level =
                        RolapLevel.lookupLevel(levels, levelName);
                    if (level == null) {
                        StringBuffer buf = new StringBuffer(64);
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
                        RolapLevel childLevel = (RolapLevel) level.getChildLevel();
                        if (childLevel != null) {
                            String tableName = childLevel.getTableName();
                            if (tableName != null) {
                                relation = snip(relation, tableName);

                                if (relation == null && getLogger().isDebugEnabled()) {
                                    getLogger().debug("RolapCube.registerDimension: after snip relation==null");
                                    getLogger().debug("RolapCube.registerDimension: snip relationTmp2="
                                            +format(relationTmp2));
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
                    if (false &&
                        !star.getFactTable().containsColumn(hierarchyUsage.getForeignKey())) {
                        throw MondrianResource.instance()
                                .HierarchyInvalidForeignKey.ex(
                                        hierarchyUsage.getForeignKey(),
                                        hierarchy.getName(),
                                        getName());
                    }
                    // parameters:
                    //   fact table,
                    //   fact table foreign key,
                    MondrianDef.Column column =
                        new MondrianDef.Column(table.getAlias(),
                                               hierarchyUsage.getForeignKey());
                    // parameters:
                    //   left column
                    //   right column
                    RolapStar.Condition joinCondition =
                        new RolapStar.Condition(column,
                                                hierarchyUsage.getJoinExp());

                    table = table.addJoin(relation, joinCondition);
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
                    for (int l = 0; l < levels.length; l++) {
                        RolapLevel level = levels[l];
                        if (level.getKeyExp() != null) {
                            parentColumn = makeColumns(table,
                                            level, parentColumn, usagePrefix);
                        }
                        if (levelName.equals(level.getName())) {
                            break;
                        }
                    }
                } else {
                    // This is the normal case, no level attribute so register
                    // all columns.
                    for (int l = 0; l < levels.length; l++) {
                        RolapLevel level = levels[l];
                        if (level.getKeyExp() != null) {
                            parentColumn = makeColumns(table,
                                            level, parentColumn, usagePrefix);
                        }
                    }
                }
            }
        }
    }

    /**
     * Adds a column to the appropriate table in the {@link RolapStar}.
     * Note that if the RolapLevel has a table attribute, then the associated
     * column needs to be associated with that table.
     *
     * @param table
     * @param level
     * @param parentColumn
     * @param usagePrefix
     * @return
     */
    protected RolapStar.Column makeColumns(
            RolapStar.Table table,
            RolapLevel level,
            RolapStar.Column parentColumn,
            String usagePrefix) {

        // If there is a table name, then first see if the table name is the
        // table parameter's name or alias and, if so, simply add the column
        // to that table. On the other hand, find the ancestor of the table
        // parameter and if found, then associate the new column with
        // that table.
        // Lastly, if the ancestor can not be found, i.e., there is no table
        // with the level's table name, what to do.  Here we simply punt and
        // associated the new column with the table parameter which might
        // be an error. We do issue a warning in any case.
        String tableName = level.getTableName();
        if (tableName != null) {
            if (table.getAlias().equals(tableName)) {
                parentColumn = table.makeColumns(this, level,
                                            parentColumn, usagePrefix);
            } else if (table.equalsTableName(tableName)) {
                parentColumn = table.makeColumns(this, level,
                                            parentColumn, usagePrefix);
            } else {
                RolapStar.Table t = table.findAncestor(tableName);
                if (t != null) {
                    parentColumn = t.makeColumns(this, level,
                                            parentColumn, usagePrefix);
                } else {
                    // Issue warning and keep going.
                    StringBuffer buf = new StringBuffer(64);
                    buf.append("RolapCube.makeColumns: for cube \"");
                    buf.append(getName());
                    buf.append("\" the Level \"");
                    buf.append(level.getName());
                    buf.append("\" has a table name attribute \"");
                    buf.append(tableName);
                    buf.append("\" but the associated RolapStar does not");
                    buf.append(" have a table with that name.");
                    getLogger().warn(buf.toString());

                    parentColumn = table.makeColumns(this, level,
                                            parentColumn, usagePrefix);
                }
            }
        } else {
            // level's expr is not a MondrianDef.Column (this is used by tests)
            // or there is no table name defined
            parentColumn = table.makeColumns(this, level,
                                            parentColumn, usagePrefix);
        }

        return parentColumn;
    }

    ///////////////////////////////////////////////////////////////////////////
    //
    // The following code deals with handling the DimensionUsage level attribute
    // and snowflake dimensions only.
    //

    /**
     * Formats a {@link MondrianDef.Relation} indenting joins for
     * readability.
     *
     * @param relation
     * @return
     */
    private static String format(MondrianDef.Relation relation) {
        StringBuffer buf = new StringBuffer();
        format(relation, buf, "");
        return buf.toString();
    }

    private static void format(
            MondrianDef.Relation relation,
            StringBuffer buf, String indent) {
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
     * This class is used to associate a MondrianDef.Table with its associated
     * level's depth. This is used to rank tables in a snowflake so that
     * the table with the lowest rank, level depth, is furthest from
     * the base fact table in the RolapStar.
     *
     */
    private static class RelNode {

        /**
         * Find a RelNode by table name or, if that fails, by table alias
         * from a map of RelNodes.
         *
         * @param table
         * @param map
         * @return
         */
        private static RelNode lookup(MondrianDef.Table table, Map map) {
            RelNode relNode = (RelNode) map.get(table.name);
            if ((relNode == null) && (table.alias != null)) {
                relNode = (RelNode) map.get(table.alias);
            }
            return relNode;
        }

        private int depth;
        private String alias;
        private MondrianDef.Table table;
        RelNode(String alias, int depth) {
            this.alias = alias;
            this.depth = depth;
        }

    }

    /**
     * Attempts to transform a {@link MondrianDef.Relation}
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
     * @return
     */
    private static MondrianDef.Relation reorder(
            MondrianDef.Relation relation,
            RolapLevel[] levels) {
        // Need at least two levels, with only one level theres nothing to do.
        if (levels.length < 2) {
            return relation;
        }

        Map nodeMap = new HashMap();

        // Create RelNode in top down order (year -> day)
        for (int i = 0; i < levels.length; i++) {
            RolapLevel level = levels[i];

            if (level.isAll()) {
                continue;
            }

            // this is the table alias
            String tableName = level.getTableName();
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
     * @return
     */
    private static boolean validateNodes(MondrianDef.Relation relation, Map map) {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;

            RelNode relNode = RelNode.lookup(table, map);
            return (relNode != null);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            return validateNodes(join.left, map) &&
                validateNodes(join.right, map);

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
     * @return
     */
    private static int leftToRight(MondrianDef.Relation relation, Map map) {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;

            RelNode relNode = RelNode.lookup(table, map);
            // Associate the table with its RelNode!!!! This is where this
            // happens.
            relNode.table = table;

            return relNode.depth;

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            int leftDepth = leftToRight(join.left, map);
            int rightDepth = leftToRight(join.right, map);

            // we want the right side to be less than the left
            if (rightDepth > leftDepth) {
                // switch
                String leftAlias = join.leftAlias;
                String leftKey = join.leftKey;
                MondrianDef.Relation left = join.left;
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
    private static void topToBottom(MondrianDef.Relation relation) {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            // nothing

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            while (join.left instanceof MondrianDef.Join) {
                MondrianDef.Join jleft = (MondrianDef.Join) join.left;
                MondrianDef.Relation right = join.right;

                join.right = new MondrianDef.Join(
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
     * Copies a {@link MondrianDef.Relation}.
     *
     * @param relation
     * @return
     */
    private static MondrianDef.Relation copy(MondrianDef.Relation relation) {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            return new MondrianDef.Table(table);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            MondrianDef.Relation left = copy(join.left);
            MondrianDef.Relation right = copy(join.right);

            return new MondrianDef.Join(join.leftAlias, join.leftKey, left,
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
     * @return
     */
    private static MondrianDef.Relation snip(MondrianDef.Relation relation,
                String tableName) {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            // Return null if the table's name or alias matches tableName
            return ((table.alias != null) && table.alias.equals(tableName))
                ? null
                : (table.name.equals(tableName) ? null : table);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            // snip left
            MondrianDef.Relation left = snip(join.left, tableName);
            if (left == null) {
                // left got snipped so return the right
                // (the join is no longer a join).
                return join.right;

            } else {
                // whatever happened on the left, save it
                join.left = left;

                // snip right
                MondrianDef.Relation right = snip(join.right, tableName);
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
    //
    ///////////////////////////////////////////////////////////////////////////



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
    public boolean isVirtual() {
        return (fact == null);
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
                throw Util.newInternal("todo: Shared hierarchy '" + usage.source +
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
        this.dimensions = (DimensionBase[])
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

    public OlapElement lookupChild(SchemaReader schemaReader, String s) {
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

    /**
     * Returns the the measures hierarchy.
     */
    public Hierarchy getMeasuresHierarchy(){
        return measuresHierarchy;
    }

    public Member createCalculatedMember(String xml) {
        MondrianDef.CalculatedMember xmlCalcMember;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
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

        final ArrayList memberList = new ArrayList();
        createCalcMembersAndNamedSets(
                new MondrianDef.CalculatedMember[] {xmlCalcMember},
                new MondrianDef.NamedSet[0],
                memberList,
                new ArrayList());
        assert memberList.size() == 1;
        return (Member) memberList.get(0);
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
                if (formulaUniqueName.equals(uniqueName) && getRole().canAccess(formula.getMdxMember())) {
                    return formula.getMdxMember();
                }
            }
            return null;
        }

        public NamedSet getNamedSet(String[] nameParts) {
            if (nameParts.length == 1) {
                String name = nameParts[0];
                for (int i = 0; i < namedSets.length; i++) {
                    Formula namedSet = namedSets[i];
                    if (namedSet.getName().equals(name)) {
                        return namedSet.getNamedSet();
                    }
                }
            }
            return super.getNamedSet(nameParts);
        }

        public List getCalculatedMembers(Hierarchy hierarchy) {
            ArrayList list = new ArrayList();

            if (getRole().getAccess(hierarchy) == Access.NONE) {
                return list;
            }

            for (int i = 0; i < calculatedMembers.length; i++) {
                Formula formula = calculatedMembers[i];
                Member member = formula.getMdxMember(); 
                if (member.getHierarchy().equals(hierarchy) && getRole().canAccess(member)) {
                    list.add(member);
                }
            }
            return list;
        }

        public List getCalculatedMembers(Level level) {
            ArrayList list = new ArrayList();

            if (getRole().getAccess(level) == Access.NONE) {
                return list;
            }

            List hierarchyList = getCalculatedMembers(level.getHierarchy());
            
            Iterator it = hierarchyList.iterator();
            
            while (it.hasNext()) {
                Member member = (Member) it.next(); 
                if (member.getLevel().equals(level) && getRole().canAccess(member)) {
                    list.add(member);
                }
            }
            return list;
        }

        public List getCalculatedMembers() {
            ArrayList list = new ArrayList();
            for (int i = 0; i < calculatedMembers.length; i++) {
                Formula formula = calculatedMembers[i];
                Member member = formula.getMdxMember(); 
                if (getRole().canAccess(member)) {
                    list.add(member);
                }
            }
            return list;
        }

        public Member getMemberByUniqueName(
                String[] uniqueNameParts, boolean failIfNotFound) {
        	Member member = (Member) lookupCompound(
					                    RolapCube.this, uniqueNameParts,
					                    failIfNotFound, Category.Member);
        	if (getRole().canAccess(member)) {
        		return member;
        	} else {
        		return null;
        	}
        }

        public Cube getCube() {
            return RolapCube.this;
        }
    }
}

// End RolapCube.java
