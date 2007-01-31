/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
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
import mondrian.mdx.*;

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
     * the ordinal of the dimension in this cube. See
     * {@link RolapDimension#topic_ordinals}
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
    private final List<HierarchyUsage> hierarchyUsages;

    private RolapStar star;
    private ExplicitRules.Group aggGroup;

    /**
     * True if the cube is being created while loading the schema
     */
    private boolean load;

    private final Map<Hierarchy, HierarchyUsage> firstUsageMap =
        new HashMap<Hierarchy, HierarchyUsage>();

    /**
     * Private constructor used by both normal cubes and virtual cubes.
     *
     * @param schema
     * @param name
     * @param fact
     */
    private RolapCube(RolapSchema schema,
                      MondrianDef.Schema xmlSchema,
                      String name,
                      boolean isCache,
                      MondrianDef.Relation fact,
                      MondrianDef.CubeDimension[] dimensions,
                      boolean load) {
        super(name, new RolapDimension[dimensions.length + 1]);

        this.schema = schema;
        this.fact = fact;
        this.hierarchyUsages = new ArrayList<HierarchyUsage>();
        this.cellReader = AggregationManager.instance();
        this.calculatedMembers = new Formula[0];
        this.namedSets = new Formula[0];
        this.load = load;

        if (! isVirtual()) {
            this.star = schema.getRolapStarRegistry().getOrCreateStar(fact);
            // only set if different from default (so that if two cubes share
            // the same fact table, either can turn off caching and both are
            // effected).
            if (! isCache) {
                star.setCacheAggregations(isCache);
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
     */
    RolapCube(RolapSchema schema,
              MondrianDef.Schema xmlSchema,
              MondrianDef.Cube xmlCube,
              boolean load) {
        this(schema, xmlSchema, xmlCube.name, xmlCube.cache,
            xmlCube.fact, xmlCube.dimensions, load);

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

        RolapMember measures[] = new RolapMember[xmlCube.measures.length];
        for (int i = 0; i < xmlCube.measures.length; i++) {
            MondrianDef.Measure xmlMeasure = xmlCube.measures[i];
            MondrianDef.Expression measureExp;
            if (xmlMeasure.column != null) {
                if (xmlMeasure.measureExp != null) {
                    throw MondrianResource.instance().
                    BadMeasureSource.ex(
                        xmlCube.name, xmlMeasure.name);
                }
                measureExp = new MondrianDef.Column(
                    fact.getAlias(), xmlMeasure.column);
            } else if (xmlMeasure.measureExp != null) {
                measureExp = xmlMeasure.measureExp;
            } else {
                throw MondrianResource.instance().
                BadMeasureSource.ex(
                        xmlCube.name, xmlMeasure.name);
            }
            final RolapBaseCubeMeasure measure = new RolapBaseCubeMeasure(
                    this, null, measuresLevel, xmlMeasure.name,
                    xmlMeasure.formatString, measureExp,
                    xmlMeasure.aggregator, xmlMeasure.datatype);
            measures[i] = measure;

            if (!Util.isEmpty(xmlMeasure.formatter)) {
                // there is a special cell formatter class
                try {
                    Class<CellFormatter> clazz =
                        (Class<CellFormatter>)
                            Class.forName(xmlMeasure.formatter);
                    Constructor<CellFormatter> ctor = clazz.getConstructor();
                    CellFormatter cellFormatter = ctor.newInstance();
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

            List<String> propNames = new ArrayList<String>();
            List<String> propExprs = new ArrayList<String>();
            validateMemberProps(xmlMeasure.memberProperties, propNames,
                    propExprs, xmlMeasure.name);
            for (int j = 0; j < propNames.size(); j++) {
                String propName = propNames.get(j);
                final Object propExpr = propExprs.get(j);
                measure.setProperty(propName, propExpr);
            }
        }

        this.measuresHierarchy.setMemberReader(new CacheMemberReader(
                new MeasureMemberSource(this.measuresHierarchy, measures)));
        init(xmlCube.dimensions);
        init(xmlCube);

        checkOrdinals(xmlCube.name, measures, xmlCube.calculatedMembers);
        loadAggGroup(xmlCube);
    }

    /**
     * Creates a <code>RolapCube</code> from a virtual cube.
     */
    RolapCube(RolapSchema schema,
              MondrianDef.Schema xmlSchema,
              MondrianDef.VirtualCube xmlVirtualCube,
              boolean load) {
        this(schema, xmlSchema, xmlVirtualCube.name, true,
            null, xmlVirtualCube.dimensions, load);


        // Since MondrianDef.Measure and MondrianDef.VirtualCubeMeasure cannot
        // be treated as the same, measure creation cannot be done in a common
        // constructor.
        RolapLevel measuresLevel =
            this.measuresHierarchy.newLevel("MeasuresLevel", 0);

        // Recreate CalculatedMembers, as the original members point to
        // incorrect dimensional ordinals for the virtual cube.
        List<RolapVirtualCubeMeasure> origMeasureList =
            new ArrayList<RolapVirtualCubeMeasure>();
        List<MondrianDef.CalculatedMember> origCalcMeasureList =
            new ArrayList<MondrianDef.CalculatedMember>();
        CubeComparator cubeComparator = new CubeComparator();
        Map<RolapCube, List<MondrianDef.CalculatedMember>> calculatedMembersMap =
            new TreeMap<RolapCube, List<MondrianDef.CalculatedMember>>(
                cubeComparator);
        for (MondrianDef.VirtualCubeMeasure xmlMeasure : xmlVirtualCube.measures) {
            // Lookup a measure in an existing cube.
            RolapCube cube = schema.lookupCube(xmlMeasure.cubeName);
            Member[] cubeMeasures = cube.getMeasures();
            boolean found = false;
            for (Member cubeMeasure : cubeMeasures) {
                if (cubeMeasure.getUniqueName().equals(xmlMeasure.name)) {
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
                                "Could not find XML Calculated Member '" +
                                    xmlMeasure.name + "' in XML cube '" +
                                    xmlMeasure.cubeName + "'");
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
                        RolapVirtualCubeMeasure virtualCubeMeasure =
                            new RolapVirtualCubeMeasure(
                                null,
                                measuresLevel,
                                (RolapStoredMeasure) cubeMeasure);

                        // Set member's visibility, default true.
                        Boolean visible = xmlMeasure.visible;
                        if (visible == null) {
                            visible = Boolean.TRUE;
                        }
                        virtualCubeMeasure.setProperty(Property.VISIBLE.name,
                            visible);
                        origMeasureList.add(virtualCubeMeasure);
                    }
                    break;
                }
            }
            if (!found) {
                throw Util.newInternal(
                    "could not find measure '" + xmlMeasure.name +
                        "' in cube '" + xmlMeasure.cubeName + "'");
            }
        }

        // Must init the dimensions before dealing with calculated members
        init(xmlVirtualCube.dimensions);
        
        // Loop through the base cubes containing calculated members
        // referenced by this virtual cube.  Resolve those members relative
        // to their base cubes first, then resolve them relative to this
        // cube so the correct dimension ordinals are used
        List<RolapVirtualCubeMeasure> modifiedMeasureList = new ArrayList<RolapVirtualCubeMeasure>(origMeasureList);
        for (Object o : calculatedMembersMap.keySet()) {
            RolapCube baseCube = (RolapCube) o;
            List<MondrianDef.CalculatedMember> calculatedMemberList =
                calculatedMembersMap.get(baseCube);
            Query queryExp = resolveCalcMembers(
                calculatedMemberList.toArray(
                    new MondrianDef.CalculatedMember[
                        calculatedMemberList.size()]),
                new MondrianDef.NamedSet[0],
                baseCube,
                false);
            MeasureFinder measureFinder =
                new MeasureFinder(this, baseCube, measuresLevel);
            queryExp.accept(measureFinder);
            modifiedMeasureList.addAll(measureFinder.getMeasuresFound());
        }
        
        // Add the original calculated members from the base cubes to our
        // list of calculated members
        List<MondrianDef.CalculatedMember> calculatedMemberList = new ArrayList<MondrianDef.CalculatedMember>();
        for (Object o : calculatedMembersMap.keySet()) {
            RolapCube baseCube = (RolapCube) o;
            calculatedMemberList.addAll(
                calculatedMembersMap.get(baseCube));
        }
        calculatedMemberList.addAll(
            Arrays.asList(xmlVirtualCube.calculatedMembers));         
        
        // Resolve all calculated members relative to this virtual cube,
        // whose measureHierarchy member reader now contains all base
        // measures referenced in those calculated members
        RolapMember[] measures =
            modifiedMeasureList.toArray(
                new RolapMember[modifiedMeasureList.size()]);
        this.measuresHierarchy.setMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(this.measuresHierarchy, measures)));
        createCalcMembersAndNamedSets(
            calculatedMemberList.toArray(
                new MondrianDef.CalculatedMember[
                    calculatedMemberList.size()]),
                new MondrianDef.NamedSet[0],
                new ArrayList<Member>(),
                new ArrayList<Formula>(),
                this,
                false);
        
        // reset the measureHierarchy member reader back to the list of
        // measures that are only defined on this virtual cube
        measures =
            origMeasureList.toArray(
                new RolapMember[origMeasureList.size()]);
        this.measuresHierarchy.setMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(this.measuresHierarchy, measures)));
        
        // remove from the calculated members array those members that weren't
        // originally defined on this virtual cube
        List<Formula> finalCalcMemberList = new ArrayList<Formula>();
        for (Formula calculatedMember : calculatedMembers) {
            if (findOriginalMembers(
                calculatedMember,
                origCalcMeasureList,
                finalCalcMemberList)) {
                continue;
            }
            findOriginalMembers(
                calculatedMember,
                Arrays.asList(xmlVirtualCube.calculatedMembers),
                finalCalcMemberList);
        }
        calculatedMembers =
            finalCalcMemberList.toArray(
                new Formula[finalCalcMemberList.size()]);

        // Note: virtual cubes do not get aggregate
    }
    
    private boolean findOriginalMembers(
        Formula formula,
        List<MondrianDef.CalculatedMember> calcMemberList,
        List<Formula> finalCalcMemberList)
    {
        for (MondrianDef.CalculatedMember xmlCalcMember : calcMemberList) {
            Dimension dimension =
                (Dimension) lookupDimension(xmlCalcMember.dimension);
            if (formula.getName().equals(xmlCalcMember.name) &&
                formula.getMdxMember().getDimension().getName().equals(
                    dimension.getName())) {
                finalCalcMemberList.add(formula);
                return true;
            }
        }
        return false;
    }

    protected void validate() {
        // Make sure that tables referenced in dimensions in this cube have
        // distinct aliases.
        Map<String, List<Object>> aliases = new HashMap<String, List<Object>>();
        List<Object> joinPath = new ArrayList<Object>();
        joinPath.add(this);
        for (Dimension dimension : dimensions) {
            final Hierarchy[] hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy : hierarchies) {
                MondrianDef.Relation relation =
                    ((RolapHierarchy) hierarchy).getRelation();
                if (relation != null) {
                    final HierarchyUsage hierarchyUsage =
                        getFirstUsage(hierarchy);
                    joinPath.add(hierarchyUsage.getForeignKey());
                    validateRelation(
                        aliases,
                        relation,
                        joinPath);
                    joinPath.remove(joinPath.size() - 1);
                }
            }
        }
    }

    private void validateRelation(
        Map<String, List<Object>> aliases,
        MondrianDef.Relation relation,
        List<Object> joinPath) {
        if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            validateRelation(aliases, join.left, joinPath);

            int saveSize = joinPath.size();
            flatten(relation, joinPath);
            joinPath.add(join.leftKey);
            joinPath.add(join.rightKey);
            validateRelation(aliases, join.right, joinPath);
            while (joinPath.size() > saveSize) {
                joinPath.remove(joinPath.size() - 1);
            }
        } else {
            String alias;
            if (relation instanceof MondrianDef.Table) {
                MondrianDef.Table table = (MondrianDef.Table) relation;
                alias = table.alias;
                if (alias == null) {
                    alias = table.name;
                }
            } else if (relation instanceof MondrianDef.InlineTable) {
                MondrianDef.InlineTable table = (MondrianDef.InlineTable) relation;
                alias = table.alias;
            } else {
                final MondrianDef.View view = (MondrianDef.View) relation;
                alias = view.alias;
            }
            joinPath.add(relation);
            joinPath.add(alias);
            List joinPath2 = aliases.get(alias);
            if (joinPath2 == null) {
                aliases.put(alias, new ArrayList<Object>(joinPath));
            } else {
                if (!joinPath.equals(joinPath2)) {
                    throw MondrianResource.instance().DuplicateAliasInCube.ex(
                        alias, this.getUniqueName());
                }
            }
            joinPath.remove(joinPath.size() - 1);
            joinPath.remove(joinPath.size() - 1);
        }
    }

    private void flatten(MondrianDef.Relation relation, List<Object> joinPath) {
        if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            flatten(join.left, joinPath);
            flatten(join.right, joinPath);
        } else {
            joinPath.add(relation);
        }
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
/*
System.out.println("RolapCube.getOrCreateDimension: " +
" cube="+getName()+
" sharedHierarchy.dimension=" + sharedHierarchy.getDimension().getName());
*/
                return (RolapDimension) sharedHierarchy.getDimension();
            }
        }
        MondrianDef.Dimension xmlDimension =
            xmlCubeDimension.getDimension(xmlSchema);
/*
        RolapDimension dim = new RolapDimension(schema, this, xmlDimension,
            xmlCubeDimension);
System.out.println("RolapCube.getOrCreateDimension: " +
" cube="+getName()+
" new dimension=" + dim.getName());
return dim;
*/
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
        List<Member> memberList = new ArrayList<Member>();
        List<Formula> formulaList = new ArrayList<Formula>();
        createCalcMembersAndNamedSets(
                xmlCube.calculatedMembers, xmlCube.namedSets,
                memberList, formulaList, this, true);
    }


    /**
     * Checks whether the ordinals of Measures and calculated measures are
     * unique.
     *
     * @param cubeName        the name of the cube (required for error-messages only)
     * @param measures        the stored measures
     * @param xmlCalcMembers  the calculate members (only members of dimension Measures are checked)
     */
    private void checkOrdinals(
            String cubeName,
            RolapMember measures[],
            MondrianDef.CalculatedMember[] xmlCalcMembers)
    {
        Map<Integer, String> ordinals = new HashMap<Integer, String>();

        // step 1: check the stored measures
        for (RolapMember measure : measures) {
            Integer ordinal = measure.getOrdinal();
            if (!ordinals.containsKey(ordinal)) {
                ordinals.put(ordinal, measure.getUniqueName());
            } else {
                throw MondrianResource.instance().MeasureOrdinalsNotUnique.ex(
                    cubeName,
                    ordinal.toString(),
                    ordinals.get(ordinal),
                    measure.getUniqueName());
            }
        }

        // step 2: check the calculated measures
        for (MondrianDef.CalculatedMember xmlCalcMember : xmlCalcMembers) {
            if (xmlCalcMember.dimension.equalsIgnoreCase("Measures")) {
                MondrianDef.CalculatedMemberProperty[] properties =
                    xmlCalcMember.memberProperties;

                for (int j = 0; j < properties.length; j++) {
                    if (properties[j].name.equals(
                        Property.MEMBER_ORDINAL.getName())) {
                        Integer ordinal = new Integer(properties[j].value);

                        final String uname =
                            "[Measures].[" + xmlCalcMember.name + "]";
                        if (!ordinals.containsKey(ordinal)) {
                            ordinals.put(ordinal, uname);
                        } else {
                            throw MondrianResource.instance().
                                MeasureOrdinalsNotUnique.ex(
                                cubeName,
                                ordinal.toString(),
                                ordinals.get(ordinal),
                                uname);
                        }
                    }
                }
            }
        }
    }

    /**
     * Adds a collection of calculated members and named sets to this cube.
     * The members and sets can refer to each other.
     *
     * @param xmlCalcMembers XML objects representing members
     * @param xmlNamedSets Array of XML definition of named set
     * @param memberList Output list of {@link Member} objects
     * @param formulaList Output list of {@link Formula} objects
     * @param cube the cube that the calculated members originate from
     * @param errOnDups throws an error if a duplicate member is found
     */
    private void createCalcMembersAndNamedSets(
            MondrianDef.CalculatedMember[] xmlCalcMembers,
            MondrianDef.NamedSet[] xmlNamedSets,
            List<Member> memberList,
            List<Formula> formulaList,
            RolapCube cube,
            boolean errOnDups) {
        
        final Query queryExp =
            resolveCalcMembers(
                xmlCalcMembers,
                xmlNamedSets,
                cube,
                errOnDups);
        if (queryExp == null) {
            return;
        }

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
    
    private Query resolveCalcMembers(
        MondrianDef.CalculatedMember[] xmlCalcMembers,
        MondrianDef.NamedSet[] xmlNamedSets,
        RolapCube cube,
        boolean errOnDups)
    {
        // If there are no objects to create, our generated SQL will be so
        // silly, the parser will laugh.
        if (xmlCalcMembers.length == 0 && xmlNamedSets.length == 0) {
            return null;
        }

        StringBuilder buf = new StringBuilder(256);
        buf.append("WITH").append(Util.nl);

        // Check the members individually, and generate SQL.
        for (int i = 0; i < xmlCalcMembers.length; i++) {
            preCalcMember(xmlCalcMembers, i, buf, cube, errOnDups);
        }

        // Check the named sets individually (for uniqueness) and generate SQL.
        Set<String> nameSet = new HashSet<String>();
        for (Formula namedSet : namedSets) {
            nameSet.add(namedSet.getName());
        }
        for (MondrianDef.NamedSet xmlNamedSet : xmlNamedSets) {
            preNamedSet(xmlNamedSet, nameSet, buf);
        }

        buf.append("SELECT FROM ")
            .append(Util.quoteMdxIdentifier(cube.getUniqueName()));

        // Parse and validate this huge MDX query we've created.
        final String queryString = buf.toString();
        final Query queryExp;
        try {
            RolapConnection conn = schema.getInternalConnection();
            queryExp = conn.parseQuery(queryString, load);
        } catch (Exception e) {
            throw MondrianResource.instance().UnknownNamedSetHasBadFormula.ex(
                getUniqueName(), e);
        }
        queryExp.resolve();
        return queryExp;
    }

    private void postNamedSet(
            MondrianDef.NamedSet[] xmlNamedSets,
            final int offset, int i,
            final Query queryExp,
            List<Formula> formulaList) {
        MondrianDef.NamedSet xmlNamedSet = xmlNamedSets[i];
        Util.discard(xmlNamedSet);
        Formula formula = queryExp.formulas[offset + i];
        namedSets = (Formula[]) RolapUtil.addElement(namedSets, formula);
        formulaList.add(formula);
    }

    private void preNamedSet(
            MondrianDef.NamedSet xmlNamedSet,
            Set<String> nameSet,
            StringBuilder buf) {
        if (!nameSet.add(xmlNamedSet.name)) {
            throw MondrianResource.instance().NamedSetNotUnique.ex(
                    xmlNamedSet.name, getUniqueName());
        }

        buf.append("SET ")
                .append(Util.makeFqName(xmlNamedSet.name))
                .append(Util.nl)
                .append(" AS ");
        Util.singleQuoteString(xmlNamedSet.getFormula(), buf);
        buf.append(Util.nl);
    }

    private void postCalcMember(
            MondrianDef.CalculatedMember[] xmlCalcMembers,
            int i,
            final Query queryExp,
            List<Member> memberList) {
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
            StringBuilder buf,
            RolapCube cube,
            boolean errOnDup) {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers[j];

        // Lookup dimension
        final Dimension dimension =
                (Dimension) lookupDimension(xmlCalcMember.dimension);
        if (dimension == null) {
            throw MondrianResource.instance().CalcMemberHasBadDimension.ex(
                    xmlCalcMember.dimension, xmlCalcMember.name,
                    getUniqueName());
        }

        // If we're processing a virtual cube, it's possible that we've
        // already processed this calculated member because it's
        // referenced in another measure; in that case, remove it from the
        // list, since we'll add it back in later; otherwise, in the
        // non-virtual cube case, throw an exception
        List<Formula> newCalcMemberList = new ArrayList<Formula>();
        for (Formula formula : calculatedMembers) {
            if (formula.getName().equals(xmlCalcMember.name) &&
                formula.getMdxMember().getDimension().getName().equals(
                    dimension.getName())) {
                if (errOnDup) {
                    throw MondrianResource.instance().CalcMemberNotUnique.ex(
                        Util.makeFqName(dimension, xmlCalcMember.name),
                        getUniqueName());
                }
                continue;
            } else {
                newCalcMemberList.add(formula);
            }
        }
        calculatedMembers =
            newCalcMemberList.toArray(new Formula[newCalcMemberList.size()]);

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
        List<String> propNames = new ArrayList<String>();
        List<String> propExprs = new ArrayList<String>();
        validateMemberProps(xmlProperties, propNames, propExprs,
                xmlCalcMember.name);

        final int measureCount =
                cube.measuresHierarchy.getMemberReader().getMemberCount();

        // Generate SQL.
        assert memberUniqueName.startsWith("[");
        buf.append("MEMBER ").append(memberUniqueName)
                .append(Util.nl)
                .append("  AS ");
        Util.singleQuoteString(xmlCalcMember.getFormula(), buf);

        assert propNames.size() == propExprs.size();
        processFormatStringAttribute(xmlCalcMember, buf);

        for (int i = 0; i < propNames.size(); i++) {
            String name = propNames.get(i);
            String expr = propExprs.get(i);
            buf.append(",").append(Util.nl)
                    .append(name).append(" = ").append(expr);
        }
        // Flag that the calc members are defined against a cube; will
        // determine the value of Member.isCalculatedInQuery
        buf.append(",").append(Util.nl).
                append(Util.quoteMdxIdentifier(Property.MEMBER_SCOPE.name)).
                append(" = 'CUBE'");

        // Assign the member an ordinal higher than all of the stored measures.
        if (!propNames.contains(Property.MEMBER_ORDINAL.getName())) {
            buf.append(",").append(Util.nl).
                    append(Property.MEMBER_ORDINAL).append(" = ").
                    append(measureCount + j);
        }
        buf.append(Util.nl);
    }

    void processFormatStringAttribute(MondrianDef.CalculatedMember xmlCalcMember, StringBuilder buf) {
        if (xmlCalcMember.formatString != null) {
            buf.append(",").append(Util.nl)
                    .append(Property.FORMAT_STRING.name).append(" = ").append(Util.quoteForMdx(xmlCalcMember.formatString));
        }
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
            List<String> propNames,
            List<String> propExprs,
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
        for (MondrianDef.CubeDimension cd : xmlDimensions) {
            if (name.equals(cd.name)) {
                return cd;
            }
        }
        // TODO: this ought to be a fatal error.
        return null;
    }

    private void init(MondrianDef.CubeDimension[] xmlDimensions) {
        int max = -1;
        for (Dimension dimension1 : dimensions) {
            final RolapDimension dimension = (RolapDimension) dimension1;
/*
System.out.println("RolapCube.init: "+
"cube=" +getName() +
", dimension=" +dimension.getName());
*/
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
        List<Member> list = new ArrayList<Member>();
        Member[] measures = getMeasures();
        for (Member measure : measures) {
            if (measure instanceof RolapBaseCubeMeasure) {
                list.add(measure);
            }
        }
        RolapBaseCubeMeasure[] storedMeasures =
            list.toArray(new RolapBaseCubeMeasure[list.size()]);

        RolapStar star = getStar();
        RolapStar.Table table = star.getFactTable();

        // create measures (and stars for them, if necessary)
        for (RolapBaseCubeMeasure storedMeasure : storedMeasures) {
            table.makeMeasure(storedMeasure);
        }

        // create dimension tables
        Dimension[] dimensions = this.getDimensions();
        for (Dimension dimension : dimensions) {
            registerDimension(dimension);
        }
    }

    int getOrdinal(int globalOrdinal) {
        return this.localDimensionOrdinals[globalOrdinal];
    }

    CellReader getCellReader() {
        return this.cellReader;
    }

    /**
     * Returns true if this Cube is either virtual or if the Cube's
     * RolapStar is caching aggregates.
     *
     * @return Whether this Cube's RolapStar should cache aggregations
     */
    public boolean isCacheAggregations() {
        return isVirtual() || star.isCacheAggregations();
    }

    /**
     * Set if this (non-virtual) Cube's RolapStar should cache
     * aggregations.
     *
     * @param cache Whether this Cube's RolapStar should cache aggregations
     */
    public void setCacheAggregations(boolean cache) {
        if (! isVirtual()) {
            star.setCacheAggregations(cache);
        }
    }

    /**
     * Clear the in memory aggregate cache associated with this Cube, but
     * only if Disabling Caching has been enabled.
     */
    public void clearCachedAggregations() {
        if (isVirtual()) {
            // TODO:
            // Currently a virtual cube does not keep a list of all of its
            // base cubes, so we need to iterate through each and flush
            // the ones that should be flushed
            schema.flushRolapStarCaches(false);
        } else {
            star.clearCachedAggregations(false);
        }
    }
    
    /**
     * Check if there are modifications in the aggregations cache
     */
    public void checkAggregateModifications() {
        if (isVirtual()) {
            // TODO:
            // Currently a virtual cube does not keep a list of all of its
            // base cubes, so we need to iterate through each and flush
            // the ones that should be flushed
            schema.checkAggregateModifications();
        } else {
            star.checkAggregateModifications();
        }        
    }
    /**
     * Push all modifications of the aggregations to global cache,
     * so other queries can start using the new cache
     */
    public void pushAggregateModificationsToGlobalCache() {
        if (isVirtual()) {
            // TODO:
            // Currently a virtual cube does not keep a list of all of its
            // base cubes, so we need to iterate through each and flush
            // the ones that should be flushed
            schema.pushAggregateModificationsToGlobalCache();
        } else {
            star.pushAggregateModificationsToGlobalCache();
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

            for (RolapHierarchy hierarchy : hierarchies) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("RolapCube<init>: hierarchy="
                        + hierarchy.getName());
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
            for (RolapHierarchy hierarchy : hierarchies) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("RolapCube<init>: hierarchy="
                        + hierarchy.getName());
                }
                createUsage(hierarchy, xmlCubeDimension);
            }
        }
    }

    synchronized void createUsage(
            RolapHierarchy hierarchy,
            MondrianDef.CubeDimension cubeDim) {

        HierarchyUsage usage = new HierarchyUsage(this, hierarchy, cubeDim);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("RolapCube.createUsage: "+
                "cube=" +getName()+
                ", hierarchy=" +hierarchy.getName() +
                ", usage=" +usage);
        }
        for (HierarchyUsage hierUsage : hierarchyUsages) {
            if (hierUsage.equals(usage)) {
                getLogger().warn(
                    "RolapCube.createUsage: duplicate " + hierUsage);
                return;
            }
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.createUsage: register " +usage);
        }
        this.hierarchyUsages.add(usage);
    }

    private synchronized HierarchyUsage getUsageByName(String name) {
        for (HierarchyUsage hierUsage : hierarchyUsages) {
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
     * @param hierarchy Hierarchy
     * @return an HierarchyUsages array with 0 or more members.
     */
    public synchronized HierarchyUsage[] getUsages(Hierarchy hierarchy) {
        String name = hierarchy.getName();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.getUsages: name="+name);
        }

        HierarchyUsage hierUsage = null;
        List<HierarchyUsage> list = null;

        for (HierarchyUsage hu : hierarchyUsages) {
            if (hu.getHierarchyName().equals(name)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("RolapCube.getUsages: "
                            + "add list HierarchyUsage.name=" + hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList<HierarchyUsage>();
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
            return list.toArray(new HierarchyUsage[list.size()]);
        } else {
            return new HierarchyUsage[0];
        }
    }

    synchronized HierarchyUsage getFirstUsage(Hierarchy hier) {
        HierarchyUsage hierarchyUsage = firstUsageMap.get(hier);
        if (hierarchyUsage == null) {
            HierarchyUsage[] hierarchyUsages = getUsages(hier);
            if (hierarchyUsages.length != 0) {
                hierarchyUsage = hierarchyUsages[0];
                firstUsageMap.put(hier, hierarchyUsage);
            }
        }
        return hierarchyUsage;
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
        List<HierarchyUsage> list = null;

        for (HierarchyUsage hu : hierarchyUsages) {
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
                    list = new ArrayList<HierarchyUsage>();
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
            return list.toArray(new HierarchyUsage[list.size()]);
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

        for (Hierarchy hierarchy1 : hierarchies) {
            RolapHierarchy hierarchy = (RolapHierarchy) hierarchy1;

            MondrianDef.Relation relation = hierarchy.getRelation();
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }
            RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();

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
                    MondrianDef.Relation relationTmp1 = relation;

                    relation = reorder(relation, levels);

                    if (relation == null && getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.registerDimension: after reorder relation==null");
                        getLogger().debug(
                            "RolapCube.registerDimension: reorder relationTmp1="
                                + format(relationTmp1));
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
                        RolapLevel childLevel =
                            (RolapLevel) level.getChildLevel();
                        if (childLevel != null) {
                            String tableName = childLevel.getTableName();
                            if (tableName != null) {
                                relation = snip(relation, tableName);

                                if (relation == null &&
                                    getLogger().isDebugEnabled()) {
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
                    if (false &&
                        !star.getFactTable()
                            .containsColumn(hierarchyUsage.getForeignKey())) {
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
                    for (RolapLevel level : levels) {
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
                    for (RolapLevel level : levels) {
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
                    StringBuilder buf = new StringBuilder(64);
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
     */
    private static String format(MondrianDef.Relation relation) {
        StringBuilder buf = new StringBuilder();
        format(relation, buf, "");
        return buf.toString();
    }

    private static void format(
            MondrianDef.Relation relation,
            StringBuilder buf, String indent) {
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
         */
        private static RelNode lookup(MondrianDef.Table table, Map<String, RelNode> map) {
            RelNode relNode = map.get(table.name);
            if ((relNode == null) && (table.alias != null)) {
                relNode = map.get(table.alias);
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
     */
    private static MondrianDef.Relation reorder(
            MondrianDef.Relation relation,
            RolapLevel[] levels) {
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
     */
    private static boolean validateNodes(
        MondrianDef.Relation relation,
        Map<String, RelNode> map)
    {
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
     */
    private static int leftToRight(
        MondrianDef.Relation relation,
        Map<String, RelNode> map)
    {
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
            // nothing

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            while (join.left instanceof MondrianDef.Join) {
                MondrianDef.Join jleft = (MondrianDef.Join) join.left;

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
     */
    private static MondrianDef.Relation snip(
            MondrianDef.Relation relation,
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



    public Member[] getMembersForQuery(String query, List<Member> calcMembers) {
        throw new UnsupportedOperationException();
    }

    Member[] getMeasures() {
        Level measuresLevel = dimensions[0].getHierarchies()[0].getLevels()[0];
        return getSchemaReader().getLevelMembers(measuresLevel, true);
    }

    /**
     * Returns this cube's fact table, null if the cube is virtual.
     */
    MondrianDef.Relation getFact() {
        return fact;
    }

    /**
     * Returns whether this cube is virtual. We use the fact that virtual cubes
     * do not have fact tables.
     */
    public boolean isVirtual() {
        return (fact == null);
    }

    RolapDimension createDimension(MondrianDef.CubeDimension xmlCubeDimension) {
        MondrianDef.Dimension xmlDimension;
        final RolapDimension dimension;
        if (xmlCubeDimension instanceof MondrianDef.Dimension) {
            xmlDimension = (MondrianDef.Dimension) xmlCubeDimension;
            dimension = new RolapDimension(schema, this, xmlDimension,
                xmlCubeDimension);
        } else if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            final MondrianDef.DimensionUsage usage =
                (MondrianDef.DimensionUsage) xmlCubeDimension;
            final RolapHierarchy sharedHierarchy =
                schema.getSharedHierarchy(usage.source);
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
        return lookupChild(schemaReader, s, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, String s, MatchType matchType)
    {
        // Note that non-exact matches aren't supported at this level,
        // so the matchType is ignored
        OlapElement oe;
        String status = null;
        if (s.equals("Measures")) {
            // A Measure is never aliased, so just get it
            // Note if one calls getUsageByName with "Measures" as the value
            // it will return null so one must either do this or check for
            // a null HierarchyUsage below and disambiguate.
            oe = super.lookupChild(schemaReader, s, MatchType.EXACT);

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
                oe = super.lookupChild(schemaReader, s, MatchType.EXACT);
                status = "hierUsage == null";

                // Let us see if one is using the source name of a
                // usage rather than the alias name.
                if (oe instanceof RolapDimension) {
                    HierarchyUsage[] usages = getUsagesBySource(s);

                    if (usages.length > 0) {
                        StringBuilder buf = new StringBuilder(64);
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
                            for (HierarchyUsage usage : usages) {
                                buf.append("\"");
                                buf.append(usage.getName());
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
                oe = super.lookupChild(schemaReader, source, MatchType.EXACT);

            } else {
                status = "hierUsage == not shared";
                // Not shared, cool man
                oe = super.lookupChild(schemaReader, s, MatchType.EXACT);
            }
        }
        if (getLogger().isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(64);
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
                buf.append(" returning elementname=").append(oe.getName());
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

    // RME
    public RolapMember[] getMeasuresMembers(){
        return measuresHierarchy.getMemberReader().getMembers();
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

        final ArrayList<Member> memberList = new ArrayList<Member>();
        createCalcMembersAndNamedSets(
                new MondrianDef.CalculatedMember[] {xmlCalcMember},
                new MondrianDef.NamedSet[0],
                memberList,
                new ArrayList<Formula>(),
                this,
                true);
        assert memberList.size() == 1;
        return memberList.get(0);
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

        public Member[] getLevelMembers(
                Level level, boolean includeCalculated) {
            Member[] members = super.getLevelMembers(level, false);
            if (includeCalculated) {
                members = Util.addLevelCalculatedMembers(this, level, members);
            }
            return members;
        }


        public Member getCalculatedMember(String[] nameParts) {
            final String uniqueName = Util.implode(nameParts);
            for (Formula formula : calculatedMembers) {
                final String formulaUniqueName =
                    formula.getMdxMember().getUniqueName();
                if (formulaUniqueName.equals(uniqueName) &&
                    getRole().canAccess(formula.getMdxMember()))
                {
                    return formula.getMdxMember();
                }
            }
            return null;
        }

        public NamedSet getNamedSet(String[] nameParts) {
            if (nameParts.length == 1) {
                String name = nameParts[0];
                for (Formula namedSet : namedSets) {
                    if (namedSet.getName().equals(name)) {
                        return namedSet.getNamedSet();
                    }
                }
            }
            return super.getNamedSet(nameParts);
        }

        public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
            ArrayList<Member> list = new ArrayList<Member>();

            if (getRole().getAccess(hierarchy) == Access.NONE) {
                return list;
            }

            for (Formula formula : calculatedMembers) {
                Member member = formula.getMdxMember();
                if (member.getHierarchy().equals(hierarchy) &&
                    getRole().canAccess(member)) {
                    list.add(member);
                }
            }
            return list;
        }

        public List<Member> getCalculatedMembers(Level level) {
            List<Member> list = new ArrayList<Member>();

            if (getRole().getAccess(level) == Access.NONE) {
                return list;
            }

            for (Member member : getCalculatedMembers(level.getHierarchy())) {
                if (member.getLevel().equals(level) &&
                    getRole().canAccess(member)) {
                    list.add(member);
                }
            }
            return list;
        }

        public List<Member> getCalculatedMembers() {
            List<Member> list = new ArrayList<Member>();
            for (Formula formula : calculatedMembers) {
                Member member = formula.getMdxMember();
                if (getRole().canAccess(member)) {
                    list.add(member);
                }
            }
            return list;
        }

        public Member getMemberByUniqueName(
            String[] uniqueNameParts, boolean failIfNotFound)
        {
            return getMemberByUniqueName(
                uniqueNameParts, failIfNotFound, MatchType.EXACT);
        }

        public Member getMemberByUniqueName(
                String[] uniqueNameParts, boolean failIfNotFound,
                MatchType matchType)
        {
            Member member = (Member) lookupCompound(
                                        RolapCube.this, uniqueNameParts,
                                        failIfNotFound, Category.Member,
                                        matchType);
            if (!failIfNotFound && member == null) {
                return null;
            }
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
            this.virtualCube = virtualCube;
            this.baseCube = baseCube;
            this.measuresLevel = measuresLevel;
            this.measuresFound = new ArrayList<RolapVirtualCubeMeasure>();
            this.calcMembersSeen = new ArrayList<RolapCalculatedMember>();
        }
        
        public Object visit(MemberExpr memberExpr)
        {
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
                RolapMember[] measures = (RolapMember[])
                    measuresFound.toArray(
                        new RolapMember[measuresFound.size()]);
                virtualCube.measuresHierarchy.setMemberReader(
                    new CacheMemberReader(
                        new MeasureMemberSource(
                            virtualCube.measuresHierarchy, measures)));
                MondrianDef.CalculatedMember xmlCalcMember =
                    schema.lookupXmlCalculatedMember(
                        calcMember.getUniqueName(),
                        baseCube.name);
                createCalcMembersAndNamedSets(
                    new MondrianDef.CalculatedMember [] { xmlCalcMember },
                    new MondrianDef.NamedSet[0],
                    new ArrayList<Member>(),
                    new ArrayList<Formula>(),
                    virtualCube,
                    false);
                return null;
                
            } else if (member instanceof RolapBaseCubeMeasure) {
                RolapBaseCubeMeasure baseMeasure =
                    (RolapBaseCubeMeasure) member;
                RolapVirtualCubeMeasure virtualCubeMeasure =
                    new RolapVirtualCubeMeasure(
                        null,
                        measuresLevel,
                        baseMeasure);
                if (!measuresFound.contains(virtualCubeMeasure)) {
                    measuresFound.add(virtualCubeMeasure);
                }
            }
            
            return null;
        }

        public List<RolapVirtualCubeMeasure> getMeasuresFound()
        {
            return measuresFound;
        }
    }
    
    private class CubeComparator implements Comparator<RolapCube>
    {
        public int compare(RolapCube c1, RolapCube c2)
        {
            return c1.getName().compareTo(c2.getName());
         }
    }
}

// End RolapCube.java
