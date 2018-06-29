/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.fun.FunDefBase;
import mondrian.resource.MondrianResource;
import mondrian.rolap.aggmatcher.ExplicitRules;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.format.FormatterCreateContext;
import mondrian.rolap.format.FormatterFactory;
import mondrian.server.Locus;
import mondrian.server.Statement;
import mondrian.spi.CellFormatter;

import org.apache.log4j.Logger;

import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;

import java.util.*;

/**
 * <code>RolapCube</code> implements {@link Cube} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapCube extends CubeBase {

    private static final Logger LOGGER = Logger.getLogger(RolapCube.class);

    private final RolapSchema schema;
    private final Map<String, Annotation> annotationMap;
    private final RolapHierarchy measuresHierarchy;

    /** For SQL generator. Fact table. */
    final MondrianDef.Relation fact;

    /** Schema reader which can see this cube and nothing else. */
    private SchemaReader schemaReader;

    /**
     * List of calculated members.
     */
    private final List<Formula> calculatedMemberList = new ArrayList<Formula>();

    /**
     * Role-based cache of calculated members
     */
    private final SoftSmartCache<Role, List<Member>>
        roleToAccessibleCalculatedMembers =
        new SoftSmartCache<Role, List<Member>>();

    /**
     * List of named sets.
     */
    private final List<Formula> namedSetList = new ArrayList<Formula>();

    /** Contains {@link HierarchyUsage}s for this cube */
    private final List<HierarchyUsage> hierarchyUsages;

    private RolapStar star;
    private ExplicitRules.Group aggGroup;

    private final Map<Hierarchy, HierarchyUsage> firstUsageMap =
        new HashMap<Hierarchy, HierarchyUsage>();

    /**
     * Refers {@link RolapCubeUsages} if this is a virtual cube
     */
    private RolapCubeUsages cubeUsages;

    RolapBaseCubeMeasure factCountMeasure;

    final List<RolapHierarchy> hierarchyList =
        new ArrayList<RolapHierarchy>();

    /**
     * Set to true when a cube is being modified after creation.
     *
     * @see #isLoadInProgress()
     */
    private boolean loadInProgress = false;

    private Map<RolapLevel, RolapCubeLevel> virtualToBaseMap =
        new HashMap<RolapLevel, RolapCubeLevel>();

    final BitKey closureColumnBitKey;

    /**
     * Used for virtual cubes.
     * Contains a list of all base cubes related to a virtual cube
     */
    private List<RolapCube> baseCubes;

    /**
     * Private constructor used by both normal cubes and virtual cubes.
     *
     * @param schema Schema cube belongs to
     * @param name Name of cube
     * @param caption Caption
     * @param description Description
     * @param fact Definition of fact table
     * @param load Whether cube is being created while loading the schema
     * @param annotationMap Annotations
     */
    private RolapCube(
        RolapSchema schema,
        MondrianDef.Schema xmlSchema,
        String name,
        boolean visible,
        String caption,
        String description,
        boolean isCache,
        MondrianDef.Relation fact,
        MondrianDef.CubeDimension[] dimensions,
        boolean load,
        Map<String, Annotation> annotationMap)
    {
        super(
            name,
            caption,
            visible,
            description,
            new RolapDimension[dimensions.length + 1]);

        assert annotationMap != null;
        this.schema = schema;
        this.annotationMap = annotationMap;
        this.caption = caption;
        this.fact = fact;
        this.hierarchyUsages = new ArrayList<HierarchyUsage>();

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
                getLogger().debug(
                    "RolapCube<init>: virtual cube="  + this.name);
            } else {
                getLogger().debug("RolapCube<init>: cube="  + this.name);
            }
        }

        RolapDimension measuresDimension =
            new RolapDimension(
                schema,
                Dimension.MEASURES_NAME,
                null,
                true,
                null,
                DimensionType.MeasuresDimension,
                false,
                Collections.<String, Annotation>emptyMap());

        this.dimensions[0] = measuresDimension;

        this.measuresHierarchy =
            measuresDimension.newHierarchy(null, false, null);
        hierarchyList.add(measuresHierarchy);

        if (!Util.isEmpty(xmlSchema.measuresCaption)) {
            measuresDimension.setCaption(xmlSchema.measuresCaption);
            this.measuresHierarchy.setCaption(xmlSchema.measuresCaption);
        }

        for (int i = 0; i < dimensions.length; i++) {
            MondrianDef.CubeDimension xmlCubeDimension = dimensions[i];
            if (xmlCubeDimension.highCardinality) {
                LOGGER.warn(
                    MondrianResource.instance()
                        .HighCardinalityInDimension.str(
                            xmlCubeDimension.getName()));
            }
            // Look up usages of shared dimensions in the schema before
            // consulting the XML schema (which may be null).
            RolapCubeDimension dimension =
                getOrCreateDimension(
                    xmlCubeDimension, schema, xmlSchema, i + 1, hierarchyList);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(
                    "RolapCube<init>: dimension=" + dimension.getName());
            }
            this.dimensions[i + 1] = dimension;

            if (! isVirtual()) {
                createUsages(dimension, xmlCubeDimension);
            }

            // the register Dimension call was moved here
            // to keep the RolapStar in sync with the realiasing
            // within the RolapCubeHierarchy objects.
            registerDimension(dimension);
        }

        // Initialize closure bit key only when we know how many columns are in
        // the star.
        if (! isVirtual()) {
            closureColumnBitKey =
                BitKey.Factory.makeBitKey(star.getColumnCount());
        } else {
            closureColumnBitKey = null;
        }

        schema.addCube(this);
    }

    /**
     * Creates a <code>RolapCube</code> from a regular cube.
     */
    RolapCube(
        RolapSchema schema,
        MondrianDef.Schema xmlSchema,
        MondrianDef.Cube xmlCube,
        boolean load)
    {
        this(
            schema,
            xmlSchema,
            xmlCube.name,
            xmlCube.visible,
            xmlCube.caption,
            xmlCube.description,
            xmlCube.cache,
            xmlCube.fact,
            xmlCube.dimensions,
            load,
            RolapHierarchy.createAnnotationMap(xmlCube.annotations));

        if (fact == null) {
            throw Util.newError(
                "Must specify fact table of cube '" + getName() + "'");
        }

        if (fact.getAlias() == null) {
            throw Util.newError(
                "Must specify alias for fact table of cube '" + getName()
                + "'");
        }

        // since MondrianDef.Measure and MondrianDef.VirtualCubeMeasure
        // can not be treated as the same, measure creation can not be
        // done in a common constructor.
        RolapLevel measuresLevel = this.measuresHierarchy.newMeasuresLevel();

        List<RolapMember> measureList =
            new ArrayList<RolapMember>(xmlCube.measures.length);
        Member defaultMeasure = null;
        for (int i = 0; i < xmlCube.measures.length; i++) {
            RolapBaseCubeMeasure measure =
                createMeasure(xmlCube, measuresLevel, i, xmlCube.measures[i]);
            measureList.add(measure);

            // Is this the default measure?
            if (Util.equalName(measure.getName(), xmlCube.defaultMeasure)) {
                defaultMeasure = measure;
            }

            if (measure.getAggregator() == RolapAggregator.Count) {
                factCountMeasure = measure;
            }
        }

        boolean writebackEnabled = false;
        for (RolapHierarchy hierarchy : hierarchyList) {
            if (ScenarioImpl.isScenario(hierarchy)) {
                writebackEnabled = true;
            }
        }

        // Ensure that cube has an atomic cell count
        // measure even if the schema does not contain one.
        if (factCountMeasure == null) {
            final MondrianDef.Measure xmlMeasure = new MondrianDef.Measure();
            xmlMeasure.aggregator = "count";
            xmlMeasure.name = "Fact Count";
            xmlMeasure.visible = false;

            MondrianDef.Annotation internalUsage = new MondrianDef.Annotation();
            internalUsage.name = "Internal Use";
            internalUsage.cdata = "For internal use";
            MondrianDef.Annotations annotations = new MondrianDef.Annotations();
            annotations.array = new MondrianDef.Annotation[1];
            annotations.array[0] = internalUsage;
            xmlMeasure.annotations = annotations;

            factCountMeasure =
                createMeasure(
                    xmlCube, measuresLevel, measureList.size(), xmlMeasure);
            measureList.add(factCountMeasure);
        }

        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(this.measuresHierarchy, measureList)));

        this.measuresHierarchy.setDefaultMember(defaultMeasure);
        init(xmlCube.dimensions);
        init(xmlCube, measureList);

        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(this.measuresHierarchy, measureList)));

        checkOrdinals(xmlCube.name, measureList);
        loadAggGroup(xmlCube);
    }

    /**
     * Creates a measure.
     *
     * @param xmlCube XML cube
     * @param measuresLevel Member that all measures belong to
     * @param ordinal Ordinal of measure
     * @param xmlMeasure XML measure
     * @return Measure
     */
    private RolapBaseCubeMeasure createMeasure(
        MondrianDef.Cube xmlCube,
        RolapLevel measuresLevel,
        int ordinal,
        final MondrianDef.Measure xmlMeasure)
    {
        MondrianDef.Expression measureExp;
        if (xmlMeasure.column != null) {
            if (xmlMeasure.measureExp != null) {
                throw MondrianResource.instance().BadMeasureSource.ex(
                    xmlCube.name, xmlMeasure.name);
            }
            measureExp = new MondrianDef.Column(
                fact.getAlias(), xmlMeasure.column);
        } else if (xmlMeasure.measureExp != null) {
            measureExp = xmlMeasure.measureExp;
        } else if (xmlMeasure.aggregator.equals("count")) {
            // it's ok if count has no expression; it means 'count(*)'
            measureExp = null;
        } else {
            throw MondrianResource.instance().BadMeasureSource.ex(
                xmlCube.name, xmlMeasure.name);
        }

        // Validate aggregator name. Substitute deprecated "distinct count"
        // with modern "distinct-count".
        String aggregator = xmlMeasure.aggregator;
        if (aggregator.equals("distinct count")) {
            aggregator = RolapAggregator.DistinctCount.getName();
        }
        final RolapBaseCubeMeasure measure =
            new RolapBaseCubeMeasure(
                this, null, measuresLevel, xmlMeasure.name,
                xmlMeasure.caption, xmlMeasure.description,
                xmlMeasure.formatString, measureExp,
                aggregator, xmlMeasure.datatype,
                RolapHierarchy.createAnnotationMap(xmlMeasure.annotations));

        FormatterCreateContext formatterContext =
                new FormatterCreateContext.Builder(measure.getUniqueName())
                    .formatterDef(xmlMeasure.cellFormatter)
                    .formatterAttr(xmlMeasure.formatter)
                    .build();
        CellFormatter cellFormatter =
            FormatterFactory.instance()
                .createCellFormatter(formatterContext);
        if (cellFormatter != null) {
            measure.setFormatter(cellFormatter);
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
        validateMemberProps(
            xmlMeasure.memberProperties, propNames, propExprs, xmlMeasure.name);
        for (int j = 0; j < propNames.size(); j++) {
            String propName = propNames.get(j);
            final Object propExpr = propExprs.get(j);
            measure.setProperty(propName, propExpr);
            if (propName.equals(Property.MEMBER_ORDINAL.name)
                && propExpr instanceof String)
            {
                final String expr = (String) propExpr;
                if (expr.startsWith("\"")
                    && expr.endsWith("\""))
                {
                    try {
                        ordinal =
                            Integer.valueOf(
                                expr.substring(1, expr.length() - 1));
                    } catch (NumberFormatException e) {
                        Util.discard(e);
                    }
                }
            }
        }
        measure.setOrdinal(ordinal);
        return measure;
    }

    /**
     * Makes sure that the schemaReader cache is invalidated.
     * Problems can occur if the measure hierarchy member reader is out
     * of sync with the cache.
     *
     * @param memberReader new member reader for measures hierarchy
     */
    private void setMeasuresHierarchyMemberReader(MemberReader memberReader) {
        this.measuresHierarchy.setMemberReader(memberReader);
        // this invalidates any cached schema reader
        this.schemaReader = null;
    }

    /**
     * Creates a <code>RolapCube</code> from a virtual cube.
     */
    RolapCube(
        RolapSchema schema,
        MondrianDef.Schema xmlSchema,
        MondrianDef.VirtualCube xmlVirtualCube,
        boolean load)
    {
        this(
            schema,
            xmlSchema,
            xmlVirtualCube.name,
            xmlVirtualCube.visible,
            xmlVirtualCube.caption,
            xmlVirtualCube.description,
            true,
            null,
            xmlVirtualCube.dimensions,
            load,
            RolapHierarchy.createAnnotationMap(xmlVirtualCube.annotations));

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

        for (MondrianDef.VirtualCubeMeasure xmlMeasure
            : xmlVirtualCube.measures)
        {
            // Lookup a measure in an existing cube.
            RolapCube cube = schema.lookupCube(xmlMeasure.cubeName);
            if (cube == null) {
                throw Util.newError(
                    "Cube '" + xmlMeasure.cubeName + "' not found");
            }
            List<Member> cubeMeasures = cube.getMeasures();
            boolean found = false;
            boolean isDefaultMeasureFound = false;
            for (Member cubeMeasure : cubeMeasures) {
                if (cubeMeasure.getUniqueName().equals(xmlMeasure.name)) {
                    if (cubeMeasure.getName().equalsIgnoreCase(
                            xmlVirtualCube.defaultMeasure))
                    {
                      defaultMeasure = cubeMeasure;
                      isDefaultMeasureFound = true;
                    }
                    found = true;
                    if (cubeMeasure instanceof RolapCalculatedMember) {
                        // We have a calculated member!  Keep track of which
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
                        RolapVirtualCubeMeasure virtualCubeMeasure =
                            new RolapVirtualCubeMeasure(
                                null,
                                measuresLevel,
                                (RolapStoredMeasure) cubeMeasure,
                                RolapHierarchy.createAnnotationMap(
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
                        //Set the actual virtual cube measure
                        //to the default measure
                        if (isDefaultMeasureFound) {
                          defaultMeasure = virtualCubeMeasure;
                        }
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

        // Must init the dimensions before dealing with calculated members
        init(xmlVirtualCube.dimensions);

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

        // iterate through a calculated member definitions in a virtual cube
        // retrieve calculated member source cube
        // set it appropriate rolap calculated measure
        Map<String, RolapHierarchy.RolapCalculatedMeasure> calcMeasuresWithBaseCube =
                new HashMap<>();
        for (RolapCube rolapCube : calculatedMembersMap.keySet()) {
            List<MondrianDef.CalculatedMember> calculatedMembers =
                    calculatedMembersMap.get(rolapCube);
            for (MondrianDef.CalculatedMember calculatedMember
                    : calculatedMembers)
            {
                List<Member> measures = rolapCube.getMeasures();
                for (Member measure : measures) {
                    if (measure instanceof
                            RolapHierarchy.RolapCalculatedMeasure)
                    {
                        RolapHierarchy.RolapCalculatedMeasure
                                calculatedMeasure =
                                (RolapHierarchy.RolapCalculatedMeasure) measure;
                        if (calculatedMember
                                .name.equals(calculatedMeasure.getKey()))
                        {
                            calculatedMeasure.setBaseCube(rolapCube);
                            calcMeasuresWithBaseCube.put(calculatedMeasure.getUniqueName(),
                                    calculatedMeasure);
                        }
                    }
                }
            }
        }

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

        // We modify the measures schema reader one last time with a version
        // which includes all calculated members as well.
        final List<RolapMember> finalMeasureMembers =
            new ArrayList<RolapMember>();
        for (RolapVirtualCubeMeasure measure : origMeasureList) {
            finalMeasureMembers.add((RolapMember)measure);
        }
        for (Formula formula : calculatedMemberList) {
            final RolapMember calcMeasure = (RolapMember) formula.getMdxMember();
            if (calcMeasure instanceof RolapHierarchy.RolapCalculatedMeasure
                    && calcMeasuresWithBaseCube.containsKey(calcMeasure.getUniqueName())) {
                ((RolapHierarchy.RolapCalculatedMeasure) calcMeasure)
                        .setBaseCube(calcMeasuresWithBaseCube.get(calcMeasure.getUniqueName()).getBaseCube());
            }
            finalMeasureMembers.add(calcMeasure);
        }
        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(
                    this.measuresHierarchy,
                    Util.<RolapMember>cast(finalMeasureMembers))));
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
            Hierarchy hierarchy = null;
            if (xmlCalcMember.dimension != null) {
                Dimension dimension =
                    lookupDimension(
                        new Id.NameSegment(
                            xmlCalcMember.dimension,
                            Id.Quoting.UNQUOTED));
                if (dimension != null
                    && dimension.getHierarchy() != null)
                {
                    hierarchy = dimension.getHierarchy();
                }
            } else if (xmlCalcMember.hierarchy != null) {
                hierarchy =
                    lookupHierarchy(
                        new Id.NameSegment(
                            xmlCalcMember.hierarchy,
                            Id.Quoting.UNQUOTED),
                        true);
            }
            if (formula.getName().equals(xmlCalcMember.name)
                && formula.getMdxMember().getHierarchy().equals(
                    hierarchy))
            {
                calcMemberList.add(formula);
                return true;
            }
        }
        return false;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    public boolean hasAggGroup() {
        return aggGroup != null;
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
     * @param dimensionOrdinal Ordinal of dimension
     * @param cubeHierarchyList List of hierarchies in cube
     * @return A dimension
     */
    private RolapCubeDimension getOrCreateDimension(
        MondrianDef.CubeDimension xmlCubeDimension,
        RolapSchema schema,
        MondrianDef.Schema xmlSchema,
        int dimensionOrdinal,
        List<RolapHierarchy> cubeHierarchyList)
    {
        RolapDimension dimension = null;
        if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            MondrianDef.DimensionUsage usage =
                (MondrianDef.DimensionUsage) xmlCubeDimension;
            final RolapHierarchy sharedHierarchy =
                schema.getSharedHierarchy(usage.source);
            if (sharedHierarchy != null) {
                dimension =
                    (RolapDimension) sharedHierarchy.getDimension();
            }
        }

        if (dimension == null) {
            MondrianDef.Dimension xmlDimension =
                xmlCubeDimension.getDimension(xmlSchema);
            dimension =
                new RolapDimension(
                    schema, this, xmlDimension, xmlCubeDimension);
        }

        // wrap the shared or regular dimension with a
        // rolap cube dimension object
        return new RolapCubeDimension(
            this, dimension, xmlCubeDimension,
            xmlCubeDimension.name, dimensionOrdinal,
            cubeHierarchyList, xmlCubeDimension.highCardinality);
    }

    /**
     * Post-initialization, doing things which cannot be done in the
     * constructor.
     */
    private void init(
        MondrianDef.Cube xmlCube,
        final List<RolapMember> memberList)
    {
        // Load calculated members and named sets.
        // (We cannot do this in the constructor, because
        // cannot parse the generated query, because the schema has not been
        // set in the cube at this point.)
        List<Formula> formulaList = new ArrayList<Formula>();
        createCalcMembersAndNamedSets(
            Arrays.asList(xmlCube.calculatedMembers),
            Arrays.asList(xmlCube.namedSets),
            memberList,
            formulaList,
            this,
            true);
    }

    /**
     * Checks that the ordinals of measures (including calculated measures)
     * are unique.
     *
     * @param cubeName        name of the cube (required for error messages)
     * @param measures        measure list
     */
    private void checkOrdinals(
        String cubeName,
        List<RolapMember> measures)
    {
        Map<Integer, String> ordinals = new HashMap<Integer, String>();
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
    }

    /**
     * Adds a collection of calculated members and named sets to this cube.
     * The members and sets can refer to each other.
     *
     * @param xmlCalcMembers XML objects representing members
     * @param xmlNamedSets Array of XML definition of named set
     * @param memberList Output list of {@link mondrian.olap.Member} objects
     * @param formulaList Output list of {@link mondrian.olap.Formula} objects
     * @param cube the cube that the calculated members originate from
     * @param errOnDups throws an error if a duplicate member is found
     */
    private void createCalcMembersAndNamedSets(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        List<MondrianDef.NamedSet> xmlNamedSets,
        List<RolapMember> memberList,
        List<Formula> formulaList,
        RolapCube cube,
        boolean errOnDups)
    {
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
        Util.assertTrue(
            queryExp.getFormulas().length
            == xmlCalcMembers.size() + xmlNamedSets.size());
        for (int i = 0; i < xmlCalcMembers.size(); i++) {
            postCalcMember(xmlCalcMembers, i, queryExp, memberList);
        }
        for (int i = 0; i < xmlNamedSets.size(); i++) {
            postNamedSet(
                xmlNamedSets, xmlCalcMembers.size(), i, queryExp, formulaList);
        }
    }

    private Query resolveCalcMembers(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        List<MondrianDef.NamedSet> xmlNamedSets,
        RolapCube cube,
        boolean errOnDups)
    {
        // If there are no objects to create, our generated SQL will be so
        // silly, the parser will laugh.
        if (xmlCalcMembers.size() == 0 && xmlNamedSets.size() == 0) {
            return null;
        }

        StringBuilder buf = new StringBuilder(256);
        buf.append("WITH").append(Util.nl);

        // Check the members individually, and generate SQL.
        final Set<String> fqNames = new LinkedHashSet<String>();
        for (int i = 0; i < xmlCalcMembers.size(); i++) {
            preCalcMember(xmlCalcMembers, i, buf, cube, errOnDups, fqNames);
        }

        // Check the named sets individually (for uniqueness) and generate SQL.
        Set<String> nameSet = new HashSet<String>();
        for (Formula namedSet : namedSetList) {
            nameSet.add(namedSet.getName());
        }
        for (MondrianDef.NamedSet xmlNamedSet : xmlNamedSets) {
            preNamedSet(xmlNamedSet, nameSet, buf);
        }

        buf.append("SELECT FROM ").append(cube.getUniqueName());

        // Parse and validate this huge MDX query we've created.
        final String queryString = buf.toString();
        try {
            final RolapConnection conn = schema.getInternalConnection();
            return Locus.execute(
                conn,
                "RolapCube.resolveCalcMembers",
                new Locus.Action<Query>() {
                    public Query execute() {
                        final Query queryExp =
                            conn.parseQuery(queryString);
                        queryExp.resolve();
                        return queryExp;
                    }
                });
        } catch (Exception e) {
            throw MondrianResource.instance().UnknownNamedSetHasBadFormula.ex(
                getName(), e);
        }
    }

    private void postNamedSet(
        List<MondrianDef.NamedSet> xmlNamedSets,
        final int offset,
        int i,
        final Query queryExp,
        List<Formula> formulaList)
    {
        MondrianDef.NamedSet xmlNamedSet = xmlNamedSets.get(i);
        Util.discard(xmlNamedSet);
        Formula formula = queryExp.getFormulas()[offset + i];
        final SetBase namedSet = (SetBase) formula.getNamedSet();
        if (xmlNamedSet.caption != null
            && xmlNamedSet.caption.length() > 0)
        {
            namedSet.setCaption(xmlNamedSet.caption);
        }

        if (xmlNamedSet.description != null
            && xmlNamedSet.description.length() > 0)
        {
            namedSet.setDescription(xmlNamedSet.description);
        }

        namedSet.setAnnotationMap(
            RolapHierarchy.createAnnotationMap(xmlNamedSet.annotations));

        namedSetList.add(formula);
        formulaList.add(formula);
    }

    private void preNamedSet(
        MondrianDef.NamedSet xmlNamedSet,
        Set<String> nameSet,
        StringBuilder buf)
    {
        if (!nameSet.add(xmlNamedSet.name)) {
            throw MondrianResource.instance().NamedSetNotUnique.ex(
                xmlNamedSet.name, getName());
        }

        buf.append("SET ")
            .append(Util.makeFqName(xmlNamedSet.name))
            .append(Util.nl)
            .append(" AS ");
        Util.singleQuoteString(xmlNamedSet.getFormula(), buf);
        buf.append(Util.nl);
    }

    private void postCalcMember(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        int i,
        final Query queryExp,
        List<RolapMember> memberList)
    {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers.get(i);

        final Formula formula = queryExp.getFormulas()[i];

        calculatedMemberList.add(formula);

        final RolapMember member = (RolapMember) formula.getMdxMember();

        Boolean visible = xmlCalcMember.visible;
        if (visible == null) {
            visible = Boolean.TRUE;
        }
        member.setProperty(Property.VISIBLE.name, visible);

        if (xmlCalcMember.caption != null
            && xmlCalcMember.caption.length() > 0)
        {
            member.setProperty(
                Property.CAPTION.name, xmlCalcMember.caption);
        }

        if (xmlCalcMember.description != null
            && xmlCalcMember.description.length() > 0)
        {
            member.setProperty(
                Property.DESCRIPTION.name, xmlCalcMember.description);
        }

        if (xmlCalcMember.getFormatString() != null
            && xmlCalcMember.getFormatString().length() > 0)
        {
            member.setProperty(
                Property.FORMAT_STRING.name, xmlCalcMember.getFormatString());
        }

        final RolapMember member1 = RolapUtil.strip(member);
        ((RolapCalculatedMember) member1).setAnnotationMap(
            RolapHierarchy.createAnnotationMap(xmlCalcMember.annotations));

        memberList.add(member);
    }

    private void preCalcMember(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        int j,
        StringBuilder buf,
        RolapCube cube,
        boolean errOnDup,
        Set<String> fqNames)
    {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers.get(j);

        if (xmlCalcMember.hierarchy != null
            && xmlCalcMember.dimension != null)
        {
            throw MondrianResource.instance()
                .CalcMemberHasBothDimensionAndHierarchy.ex(
                    xmlCalcMember.name, getName());
        }

        // Lookup dimension
        Hierarchy hierarchy = null;
        String dimName = null;
        if (xmlCalcMember.dimension != null) {
            dimName = xmlCalcMember.dimension;
            final Dimension dimension =
                lookupDimension(
                    new Id.NameSegment(
                        xmlCalcMember.dimension,
                        Id.Quoting.UNQUOTED));
            if (dimension != null) {
                hierarchy = dimension.getHierarchy();
            }
        } else if (xmlCalcMember.hierarchy != null) {
            dimName = xmlCalcMember.hierarchy;
            hierarchy = (Hierarchy)
                getSchemaReader().withLocus().lookupCompound(
                    this,
                    Util.parseIdentifier(dimName),
                    false,
                    Category.Hierarchy);
        }
        if (hierarchy == null) {
            throw MondrianResource.instance().CalcMemberHasBadDimension.ex(
                dimName, xmlCalcMember.name, getName());
        }

        // Root of fully-qualified name.
        String parentFqName;
        if (xmlCalcMember.parent != null) {
            parentFqName = xmlCalcMember.parent;
        } else {
            parentFqName = hierarchy.getUniqueNameSsas();
        }

        if (!hierarchy.getDimension().isMeasures()) {
            // Check if the parent exists.
            final OlapElement parent =
                Util.lookupCompound(
                    getSchemaReader().withLocus(),
                    this,
                    Util.parseIdentifier(parentFqName),
                    false,
                    Category.Unknown);

            if (parent == null) {
                throw MondrianResource.instance()
                    .CalcMemberHasUnknownParent.ex(
                        parentFqName, xmlCalcMember.name, getName());
            }

            if (parent.getHierarchy() != hierarchy) {
                throw MondrianResource.instance()
                .CalcMemberHasDifferentParentAndHierarchy.ex(
                    xmlCalcMember.name, getName(), hierarchy.getUniqueName());
            }
        }

        // If we're processing a virtual cube, it's possible that we've
        // already processed this calculated member because it's
        // referenced in another measure; in that case, remove it from the
        // list, since we'll add it back in later; otherwise, in the
        // non-virtual cube case, throw an exception
        final String fqName = Util.makeFqName(parentFqName, xmlCalcMember.name);
        for (int i = 0; i < calculatedMemberList.size(); i++) {
            Formula formula = calculatedMemberList.get(i);
            if (formula.getName().equals(xmlCalcMember.name)
                && formula.getMdxMember().getHierarchy().equals(
                    hierarchy))
            {
                if (errOnDup) {
                    throw MondrianResource.instance().CalcMemberNotUnique.ex(
                        fqName,
                        getName());
                } else {
                    calculatedMemberList.remove(i);
                    --i;
                }
            }
        }

        // Check this calc member doesn't clash with one earlier in this
        // batch.
        if (!fqNames.add(fqName)) {
            throw MondrianResource.instance().CalcMemberNotUnique.ex(
                fqName,
                getName());
        }

        final MondrianDef.CalculatedMemberProperty[] xmlProperties =
                xmlCalcMember.memberProperties;
        List<String> propNames = new ArrayList<String>();
        List<String> propExprs = new ArrayList<String>();
        validateMemberProps(
            xmlProperties, propNames, propExprs, xmlCalcMember.name);

        final int measureCount =
            cube.measuresHierarchy.getMemberReader().getMemberCount();

        // Generate SQL.
        assert fqName.startsWith("[");
        buf.append("MEMBER ")
            .append(fqName)
            .append(Util.nl)
            .append("  AS ");
        Util.singleQuoteString(xmlCalcMember.getFormula(), buf);

        if (xmlCalcMember.cellFormatter != null) {
            if (xmlCalcMember.cellFormatter.className != null) {
                propNames.add(Property.CELL_FORMATTER.name);
                propExprs.add(
                    Util.quoteForMdx(xmlCalcMember.cellFormatter.className));
            }
            if (xmlCalcMember.cellFormatter.script != null) {
                if (xmlCalcMember.cellFormatter.script.language != null) {
                    propNames.add(Property.CELL_FORMATTER_SCRIPT_LANGUAGE.name);
                    propExprs.add(
                        Util.quoteForMdx(
                            xmlCalcMember.cellFormatter.script.language));
                }
                propNames.add(Property.CELL_FORMATTER_SCRIPT.name);
                propExprs.add(
                    Util.quoteForMdx(xmlCalcMember.cellFormatter.script.cdata));
            }
        }

        assert propNames.size() == propExprs.size();
        processFormatStringAttribute(xmlCalcMember, buf);

        for (int i = 0; i < propNames.size(); i++) {
            String name = propNames.get(i);
            String expr = propExprs.get(i);
            buf.append(",").append(Util.nl);
            expr = removeSurroundingQuotesIfNumericProperty(name, expr);
            buf.append(name).append(" = ").append(expr);
        }
        // Flag that the calc members are defined against a cube; will
        // determine the value of Member.isCalculatedInQuery
        buf.append(",")
            .append(Util.nl);
        Util.quoteMdxIdentifier(Property.MEMBER_SCOPE.name, buf);
        buf.append(" = 'CUBE'");

        // Assign the member an ordinal higher than all of the stored measures.
        if (!propNames.contains(Property.MEMBER_ORDINAL.getName())) {
            buf.append(",")
                .append(Util.nl)
                .append(Property.MEMBER_ORDINAL)
                .append(" = ")
                .append(measureCount + j);
        }
        buf.append(Util.nl);
    }

    private String removeSurroundingQuotesIfNumericProperty(
        String name,
        String expr)
    {
        Property prop = Property.lookup(name, false);
        if (prop != null
            && prop.getType().isNumeric()
            && isSurroundedWithQuotes(expr)
            && expr.length() > 2)
        {
            return expr.substring(1, expr.length() - 1);
        }
        return expr;
    }

    private boolean isSurroundedWithQuotes(String expr) {
        return expr.startsWith("\"") && expr.endsWith("\"");
    }

    void processFormatStringAttribute(
        MondrianDef.CalculatedMember xmlCalcMember,
        StringBuilder buf)
    {
        if (xmlCalcMember.formatString != null) {
            buf.append(",")
                .append(Util.nl)
                .append(Property.FORMAT_STRING.name)
                .append(" = ")
                .append(Util.quoteForMdx(xmlCalcMember.formatString));
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
        String memberName)
    {
        if (xmlProperties == null) {
            return;
        }
        for (MondrianDef.CalculatedMemberProperty xmlProperty : xmlProperties) {
            if (xmlProperty.expression == null && xmlProperty.value == null) {
                throw MondrianResource.instance()
                    .NeitherExprNorValueForCalcMemberProperty.ex(
                        xmlProperty.name, memberName, getName());
            }
            if (xmlProperty.expression != null && xmlProperty.value != null) {
                throw MondrianResource.instance().ExprAndValueForMemberProperty
                    .ex(
                        xmlProperty.name, memberName, getName());
            }
            propNames.add(xmlProperty.name);
            if (xmlProperty.expression != null) {
                propExprs.add(xmlProperty.expression);
            } else {
                propExprs.add(Util.quoteForMdx(xmlProperty.value));
            }
        }
    }

    public RolapSchema getSchema() {
        return schema;
    }

    /**
     * Returns the named sets of this cube.
     */
    public NamedSet[] getNamedSets() {
        NamedSet[] namedSetsArray = new NamedSet[namedSetList.size()];
        for (int i = 0; i < namedSetList.size(); i++) {
            namedSetsArray[i] = namedSetList.get(i).getNamedSet();
        }
        return namedSetsArray;
    }

    /**
     * Returns the schema reader which enforces the appropriate access-control
     * context. schemaReader is cached, and needs to stay in sync with
     * any changes to the cube.
     *
     * @post return != null
     * @see #getSchemaReader(Role)
     */
    public synchronized SchemaReader getSchemaReader() {
        if (schemaReader == null) {
            schemaReader =
                new RolapCubeSchemaReader(Util.createRootRole(schema));
        }
        return schemaReader;
    }

    public SchemaReader getSchemaReader(Role role) {
        if (role == null) {
            return getSchemaReader();
        } else {
            return new RolapCubeSchemaReader(role);
        }
    }

    MondrianDef.CubeDimension lookup(
        MondrianDef.CubeDimension[] xmlDimensions,
        String name)
    {
        for (MondrianDef.CubeDimension cd : xmlDimensions) {
            if (name.equals(cd.name)) {
                return cd;
            }
        }
        // TODO: this ought to be a fatal error.
        return null;
    }

    private void init(MondrianDef.CubeDimension[] xmlDimensions) {
        for (Dimension dimension1 : dimensions) {
            final RolapDimension dimension = (RolapDimension) dimension1;
            dimension.init(lookup(xmlDimensions, dimension.getName()));
        }
        register();
    }

    private void register() {
        if (isVirtual()) {
            return;
        }
        List<RolapBaseCubeMeasure> storedMeasures =
            new ArrayList<RolapBaseCubeMeasure>();
        for (Member measure : getMeasures()) {
            if (measure instanceof RolapBaseCubeMeasure) {
                storedMeasures.add((RolapBaseCubeMeasure) measure);
            }
        }

        RolapStar star = getStar();
        RolapStar.Table table = star.getFactTable();

        // create measures (and stars for them, if necessary)
        for (RolapBaseCubeMeasure storedMeasure : storedMeasures) {
            table.makeMeasure(storedMeasure);
        }
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
        clearCachedAggregations(false);
    }

    /**
     * Clear the in memory aggregate cache associated with this Cube.
     */
    public void clearCachedAggregations(boolean forced) {
        if (isVirtual()) {
            // TODO:
            // Currently a virtual cube does not keep a list of all of its
            // base cubes, so we need to iterate through each and flush
            // the ones that should be flushed. Could use a CacheControl
            // method here.
            for (RolapStar star1 : schema.getStars()) {
                // this will only flush the star's aggregate cache if
                // 1) DisableCaching is true or 2) the star's cube has
                // cacheAggregations set to false in the schema.
                star1.clearCachedAggregations(forced);
            }
        } else {
            star.clearCachedAggregations(forced);
        }
    }

    /**
     * Returns this cube's underlying star schema.
     */
    public RolapStar getStar() {
        return star;
    }

    private void createUsages(
        RolapCubeDimension dimension,
        MondrianDef.CubeDimension xmlCubeDimension)
    {
        // RME level may not be in all hierarchies
        // If one uses the DimensionUsage attribute "level", which level
        // in a hierarchy to join on, and there is more than one hierarchy,
        // then a HierarchyUsage can not be created for the hierarchies
        // that do not have the level defined.
        RolapCubeHierarchy[] hierarchies =
            (RolapCubeHierarchy[]) dimension.getHierarchies();

        if (hierarchies.length == 1) {
            // Only one, so let lower level error checking handle problems
            createUsage(hierarchies[0], xmlCubeDimension);

        } else if ((xmlCubeDimension instanceof MondrianDef.DimensionUsage)
            && (((MondrianDef.DimensionUsage) xmlCubeDimension).level
                != null))
        {
            // More than one, make sure if we are joining by level, that
            // at least one hierarchy can and those that can not are
            // not registered
            MondrianDef.DimensionUsage du =
                (MondrianDef.DimensionUsage) xmlCubeDimension;

            int cnt = 0;

            for (RolapCubeHierarchy hierarchy : hierarchies) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapCube<init>: hierarchy="
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
            for (RolapCubeHierarchy hierarchy : hierarchies) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(
                        "RolapCube<init>: hierarchy="
                        + hierarchy.getName());
                }
                createUsage(hierarchy, xmlCubeDimension);
            }
        }
    }

    synchronized void createUsage(
        RolapCubeHierarchy hierarchy,
        MondrianDef.CubeDimension cubeDim)
    {
        HierarchyUsage usage = new HierarchyUsage(this, hierarchy, cubeDim);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "RolapCube.createUsage: "
                + "cube=" + getName()
                + ", hierarchy=" + hierarchy.getName()
                + ", usage=" + usage);
        }
        for (HierarchyUsage hierUsage : hierarchyUsages) {
            if (hierUsage.equals(usage)) {
                getLogger().warn(
                    "RolapCube.createUsage: duplicate " + hierUsage);
                return;
            }
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.createUsage: register " + usage);
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
        if (!name.equals(hierarchy.getDimension().getName())
            && MondrianProperties.instance().SsasCompatibleNaming.get())
        {
            name = hierarchy.getDimension().getName() + "." + name;
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.getUsages: name=" + name);
        }

        HierarchyUsage hierUsage = null;
        List<HierarchyUsage> list = null;

        for (HierarchyUsage hu : hierarchyUsages) {
            if (hu.getHierarchyName().equals(name)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.getUsages: "
                            + "add list HierarchyUsage.name=" + hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList<HierarchyUsage>();
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.getUsages: "
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
     * This method is currently only called if an error occurs in lookupChild(),
     * so that more information can be displayed in the error log.
     *
     * @param source Name of shared dimension
     * @return array of HierarchyUsage (HierarchyUsage[]) - never null.
     */
    private synchronized HierarchyUsage[] getUsagesBySource(String source) {
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("RolapCube.getUsagesBySource: source=" + source);
        }

        HierarchyUsage hierUsage = null;
        List<HierarchyUsage> list = null;

        for (HierarchyUsage hu : hierarchyUsages) {
            String s = hu.getSource();
            if ((s != null) && s.equals(source)) {
                if (list != null) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.getUsagesBySource: "
                            + "add list HierarchyUsage.name="
                            + hu.getName());
                    }
                    list.add(hu);
                } else if (hierUsage == null) {
                    hierUsage = hu;
                } else {
                    list = new ArrayList<HierarchyUsage>();
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(
                            "RolapCube.getUsagesBySource: "
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
     * @param dimension Dimension
     */
    void registerDimension(RolapCubeDimension dimension) {
        RolapStar star = getStar();

        Hierarchy[] hierarchies = dimension.getHierarchies();

        for (Hierarchy hierarchy1 : hierarchies) {
            RolapHierarchy hierarchy = (RolapHierarchy) hierarchy1;

            MondrianDef.RelationOrJoin relation = hierarchy.getRelation();
            if (relation == null) {
                continue; // e.g. [Measures] hierarchy
            }
            RolapCubeLevel[] levels = (RolapCubeLevel[]) hierarchy.getLevels();

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
                        RolapLevel childLevel =
                            (RolapLevel) level.getChildLevel();
                        if (childLevel != null) {
                            String tableName = childLevel.getTableName();
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
                    MondrianDef.Column column =
                        new MondrianDef.Column(
                            table.getAlias(),
                            hierarchyUsage.getForeignKey());
                    // parameters:
                    //   left column
                    //   right column
                    RolapStar.Condition joinCondition =
                        new RolapStar.Condition(
                            column,
                            hierarchyUsage.getJoinExp());

                    if (hierarchy.getXmlHierarchy() != null
                            && hierarchy.getXmlHierarchy()
                            .primaryKeyTable != null
                            && relation instanceof MondrianDef.Join
                            && ((MondrianDef.Join) relation)
                            .right instanceof MondrianDef.Table
                            && ((MondrianDef.Table)
                            ((MondrianDef.Join) relation).right)
                            .getAlias() != null
                            && ((MondrianDef.Table)
                            ((MondrianDef.Join) relation).right)
                            .getAlias()
                            .equals(
                                hierarchy.getXmlHierarchy()
                              .primaryKeyTable))
                    {
                        MondrianDef.Join newRelation = new MondrianDef.Join();
                        newRelation.left = ((MondrianDef.Join) relation).right;
                        newRelation.right = ((MondrianDef.Join) relation).left;
                        newRelation.leftAlias = ((MondrianDef.Join)
                                relation).getRightAlias();
                        newRelation.rightAlias = ((MondrianDef.Join)
                                relation).getLeftAlias();
                        newRelation.leftKey = ((MondrianDef.Join)
                                relation).rightKey;
                        newRelation.rightKey = ((MondrianDef.Join)
                                relation).leftKey;

                        relation = newRelation;
                    }

                    table = table.addJoin(this, relation, joinCondition);
                }

                // The parent Column is used so that non-shared dimensions
                // which use the fact table (not a separate dimension table)
                // can keep a record of what other columns are in the
                // same set of levels.
                RolapStar.Column parentColumn = null;

                // RME
                // If the level name is not null, then we need only register
                // those columns for that level and above.
                if (levelName != null) {
                    for (RolapCubeLevel level : levels) {
                        if (level.getKeyExp() != null) {
                            parentColumn = makeColumns(
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
                        if (level.getKeyExp() != null) {
                            parentColumn = makeColumns(
                                table, level, parentColumn, usagePrefix);
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
        RolapCubeLevel level,
        RolapStar.Column parentColumn,
        String usagePrefix)
    {
        // If there is a table name, then first see if the table name is the
        // table parameter's name or alias and, if so, simply add the column
        // to that table. On the other hand, find the ancestor of the table
        // parameter and if found, then associate the new column with
        // that table.
        //
        // Lastly, if the ancestor can not be found, i.e., there is no table
        // with the level's table name, what to do.  Here we simply punt and
        // associated the new column with the table parameter which might
        // be an error. We do issue a warning in any case.
        String tableName = level.getTableName();
        if (tableName != null) {
            if (table.getAlias().equals(tableName)) {
                parentColumn = table.makeColumns(
                    this, level, parentColumn, usagePrefix);
            } else if (table.equalsTableName(tableName)) {
                parentColumn = table.makeColumns(
                    this, level, parentColumn, usagePrefix);
            } else {
                RolapStar.Table t = table.findAncestor(tableName);
                if (t != null) {
                    parentColumn = t.makeColumns(
                        this, level, parentColumn, usagePrefix);
                } else {
                    // Issue warning and keep going.
                    getLogger().warn(
                        "RolapCube.makeColumns: for cube \""
                        + getName()
                        + "\" the Level \""
                        + level.getName()
                        + "\" has a table name attribute \""
                        + tableName
                        + "\" but the associated RolapStar does not"
                        + " have a table with that name.");

                    parentColumn = table.makeColumns(
                        this, level, parentColumn, usagePrefix);
                }
            }
        } else {
            // level's expr is not a MondrianDef.Column (this is used by tests)
            // or there is no table name defined
            parentColumn = table.makeColumns(
                this, level, parentColumn, usagePrefix);
        }

        return parentColumn;
    }

    // The following code deals with handling the DimensionUsage level attribute
    // and snowflake dimensions only.

    /**
     * Formats a {@link mondrian.olap.MondrianDef.RelationOrJoin}, indenting
     * joins for readability.
     *
     * @param relation A table or a join
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
     * This method tells us if unrelated dimensions to measures from
     * the input base cube should be pushed to default member or not
     * during aggregation.
     * @param baseCubeName name of the base cube for which we want
     * to check this property
     * @return boolean
     */
    public boolean shouldIgnoreUnrelatedDimensions(String baseCubeName) {
        return cubeUsages != null
            && cubeUsages.shouldIgnoreUnrelatedDimensions(baseCubeName);
    }

    /**
     * Returns a list of all hierarchies in this cube, in order of dimension.
     *
     * <p>TODO: Make this method return RolapCubeHierarchy, when the measures
     * hierarchy is a RolapCubeHierarchy.
     *
     * @return List of hierarchies
     */
    public List<RolapHierarchy> getHierarchies() {
        return hierarchyList;
    }

    public boolean isLoadInProgress() {
        return loadInProgress
            || getSchema().getSchemaLoadDate() == null;
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
         * @param table Is supposed a {@link MondrianDef.Table}
         * @param map Names of tables and {@link RelNode} pairs
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
     * @param relation A table or a join
     * @param levels Levels in hierarchy
     */
    private static MondrianDef.RelationOrJoin reorder(
        MondrianDef.RelationOrJoin relation,
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
     * @param relation A table or a join
     * @param map Names of tables and {@link RelNode} pairs
     */
    private static boolean validateNodes(
        MondrianDef.RelationOrJoin relation,
        Map<String, RelNode> map)
    {
        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation table =
                (MondrianDef.Relation) relation;

            RelNode relNode = RelNode.lookup(table, map);
            return (relNode != null);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

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
     * @param relation is a table or a join
     * @param map Names of tables and {@link RelNode} pairs
     */
    private static int leftToRight(
        MondrianDef.RelationOrJoin relation,
        Map<String, RelNode> map)
    {
        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation table =
                (MondrianDef.Relation) relation;

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
                MondrianDef.RelationOrJoin left = join.left;
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
     * @param relation A table or a join
     */
    private static void topToBottom(MondrianDef.RelationOrJoin relation) {
        if (relation instanceof MondrianDef.Table) {
            // nothing

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            while (join.left instanceof MondrianDef.Join) {
                MondrianDef.Join jleft = (MondrianDef.Join) join.left;

                join.right =
                    new MondrianDef.Join(
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
     * Copies a {@link mondrian.olap.MondrianDef.RelationOrJoin}.
     *
     * @param relation A table or a join
     */
    private static MondrianDef.RelationOrJoin copy(
        MondrianDef.RelationOrJoin relation)
    {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            return new MondrianDef.Table(table);

        } else if (relation instanceof MondrianDef.InlineTable) {
            MondrianDef.InlineTable table = (MondrianDef.InlineTable) relation;
            return new MondrianDef.InlineTable(table);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            MondrianDef.RelationOrJoin left = copy(join.left);
            MondrianDef.RelationOrJoin right = copy(join.right);

            return new MondrianDef.Join(
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
     * @param relation A table or a join
     * @param tableName Table name in relation
     */
    private static MondrianDef.RelationOrJoin snip(
        MondrianDef.RelationOrJoin relation,
        String tableName)
    {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            // Return null if the table's name or alias matches tableName
            return ((table.alias != null) && table.alias.equals(tableName))
                ? null
                : (table.name.equals(tableName) ? null : table);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;

            // snip left
            MondrianDef.RelationOrJoin left = snip(join.left, tableName);
            if (left == null) {
                // left got snipped so return the right
                // (the join is no longer a join).
                return join.right;

            } else {
                // whatever happened on the left, save it
                join.left = left;

                // snip right
                MondrianDef.RelationOrJoin right = snip(join.right, tableName);
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

    public Member[] getMembersForQuery(String query, List<Member> calcMembers) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the time hierarchy for this cube. If there is no time hierarchy,
     * throws.
     */
    public RolapHierarchy getTimeHierarchy(String funName) {
        for (RolapHierarchy hierarchy : hierarchyList) {
            if (hierarchy.getDimension().getDimensionType()
                == DimensionType.TimeDimension)
            {
                return hierarchy;
            }
        }

        throw MondrianResource.instance().NoTimeDimensionInCube.ex(funName);
    }

    /**
     * Finds out non joining dimensions for this cube.
     * Useful for finding out non joining dimensions for a stored measure from
     * a base cube.
     *
     * @param tuple array of members
     * @return Set of dimensions that do not exist (non joining) in this cube
     */
    public Set<Dimension> nonJoiningDimensions(Member[] tuple) {
        Set<Dimension> otherDims = new HashSet<Dimension>();
        for (Member member : tuple) {
            if (!member.isCalculated()) {
                otherDims.add(member.getDimension());
            }
        }
        return nonJoiningDimensions(otherDims);
    }

    /**
     * Finds out non joining dimensions for this cube.  Equality test for
     * dimensions is done based on the unique name. Object equality can't be
     * used.
     *
     * @param otherDims Set of dimensions to be tested for existence in this
     * cube
     * @return Set of dimensions that do not exist (non joining) in this cube
     */
    public Set<Dimension> nonJoiningDimensions(Set<Dimension> otherDims) {
        Dimension[] baseCubeDimensions = getDimensions();
        Set<String>  baseCubeDimNames = new HashSet<String>();
        for (Dimension baseCubeDimension : baseCubeDimensions) {
            baseCubeDimNames.add(baseCubeDimension.getUniqueName());
        }
        Set<Dimension> nonJoiningDimensions = new HashSet<Dimension>();
        for (Dimension otherDim : otherDims) {
            if (!baseCubeDimNames.contains(otherDim.getUniqueName())) {
                nonJoiningDimensions.add(otherDim);
            }
        }
        return nonJoiningDimensions;
    }

    List<Member> getMeasures() {
        Level measuresLevel = dimensions[0].getHierarchies()[0].getLevels()[0];
        return getSchemaReader().getLevelMembers(measuresLevel, true);
    }

    /**
     * Returns this cube's fact table, null if the cube is virtual.
     */
    public MondrianDef.RelationOrJoin getFact() {
        return fact;
    }

    /**
     * Returns whether this cube is virtual. We use the fact that virtual cubes
     * do not have fact tables.
     */
    public boolean isVirtual() {
        return fact == null;
    }

    /**
     * Returns the system measure that counts the number of fact table rows in
     * a given cell.
     *
     * <p>Never null, because if there is no count measure explicitly defined,
     * the system creates one.
     */
    RolapMeasure getFactCountMeasure() {
        return factCountMeasure;
    }

    /**
     * Returns the system measure that counts the number of atomic cells in
     * a given cell.
     *
     * <p>A cell is atomic if all dimensions are at their lowest level.
     * If the fact table has a primary key, this measure is equivalent to the
     * {@link #getFactCountMeasure() fact count measure}.
     */
    RolapMeasure getAtomicCellCountMeasure() {
        // TODO: separate measure
        return factCountMeasure;
    }

    /**
     * Locates the base cube hierarchy for a particular virtual hierarchy.
     * If not found, return null. This may be converted to a map lookup
     * or cached in some way in the future to increase performance
     * with cubes that have large numbers of hierarchies
     *
     * @param hierarchy virtual hierarchy
     * @return base cube hierarchy if found
     */
    RolapHierarchy findBaseCubeHierarchy(RolapHierarchy hierarchy) {
        for (int i = 0; i < getDimensions().length; i++) {
            Dimension dimension = getDimensions()[i];
            if (dimension.getName().equals(
                    hierarchy.getDimension().getName()))
            {
                for (int j = 0; j <  dimension.getHierarchies().length; j++) {
                    Hierarchy hier = dimension.getHierarchies()[j];
                    if (hier.getName().equals(hierarchy.getName())) {
                        return (RolapHierarchy)hier;
                    }
                }
            }
        }
        return null;
    }


    /**
     * Locates the base cube level for a particular virtual level.
     * If not found, return null. This may be converted to a map lookup
     * or cached in some way in the future to increase performance
     * with cubes that have large numbers of hierarchies and levels
     *
     * @param level virtual level
     * @return base cube level if found
     */
    public RolapCubeLevel findBaseCubeLevel(RolapLevel level) {
        if (virtualToBaseMap.containsKey(level)) {
            return virtualToBaseMap.get(level);
        }
        String levelDimName = level.getDimension().getName();
        String levelHierName = level.getHierarchy().getName();

        // Closures are not in the dimension list so we need special logic for
        // locating the level.
        //
        // REVIEW: jhyde, 2009/7/21: This may no longer be the case, and we may
        // be able to improve performance. RolapCube.hierarchyList now contains
        // all hierarchies, including closure hierarchies; and
        // RolapHierarchy.closureFor indicates the base hierarchy for a closure
        // hierarchy.

        boolean isClosure = false;
        String closDimName = null;
        String closHierName = null;
        if (levelDimName.endsWith("$Closure")) {
            isClosure = true;
            closDimName = levelDimName.substring(0, levelDimName.length() - 8);
            closHierName =
                levelHierName.substring(0, levelHierName.length() - 8);
        }

        for (Dimension dimension : getDimensions()) {
            final String dimensionName = dimension.getName();
            if (dimensionName.equals(levelDimName)
                || (isClosure && dimensionName.equals(closDimName)))
            {
                for (Hierarchy hier : dimension.getHierarchies()) {
                    final String hierarchyName = hier.getName();
                    if (hierarchyName.equals(levelHierName)
                        || (isClosure && hierarchyName.equals(closHierName)))
                    {
                        if (isClosure) {
                            final RolapCubeLevel baseLevel =
                                ((RolapCubeLevel)
                                    hier.getLevels()[1]).getClosedPeer();
                            virtualToBaseMap.put(level, baseLevel);
                            return baseLevel;
                        }
                        for (Level lvl : hier.getLevels()) {
                            if (lvl.getName().equals(level.getName())) {
                                final RolapCubeLevel baseLevel =
                                    (RolapCubeLevel) lvl;
                                virtualToBaseMap.put(level, baseLevel);
                                return baseLevel;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    RolapCubeDimension createDimension(
        MondrianDef.CubeDimension xmlCubeDimension,
        MondrianDef.Schema xmlSchema)
    {
        RolapCubeDimension dimension =
            getOrCreateDimension(
                xmlCubeDimension, schema, xmlSchema,
                dimensions.length, hierarchyList);

        if (! isVirtual()) {
            createUsages(dimension, xmlCubeDimension);
        }
        registerDimension(dimension);

        dimension.init(xmlCubeDimension);

        // add to dimensions array
        this.dimensions = Util.append(dimensions, dimension);

        return dimension;
    }

    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment s) {
        return lookupChild(schemaReader, s, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        if (!(s instanceof Id.NameSegment)) {
            return null;
        }
        final Id.NameSegment nameSegment = (Id.NameSegment) s;

        // Note that non-exact matches aren't supported at this level,
        // so the matchType is ignored
        String status = null;
        OlapElement oe = null;
        if (matchType == MatchType.EXACT_SCHEMA) {
            oe = super.lookupChild(
                schemaReader, nameSegment, MatchType.EXACT_SCHEMA);
        } else {
            oe = super.lookupChild(
                schemaReader, nameSegment, MatchType.EXACT);
        }

        if (oe == null) {
            HierarchyUsage[] usages = getUsagesBySource(nameSegment.name);
            if (usages.length > 0) {
                StringBuilder buf = new StringBuilder(64);
                buf.append("RolapCube.lookupChild: ");
                buf.append("In cube \"");
                buf.append(getName());
                buf.append("\" use of unaliased Dimension name \"");
                buf.append(nameSegment);
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

        if (getLogger().isDebugEnabled()) {
            if (!nameSegment.matches("Measures")) {
                HierarchyUsage hierUsage = getUsageByName(nameSegment.name);
                if (hierUsage == null) {
                    status = "hierUsage == null";
                } else {
                    status =
                        "hierUsage == "
                        + (hierUsage.isShared() ? "shared" : "not shared");
                }
            }
            StringBuilder buf = new StringBuilder(64);
            buf.append("RolapCube.lookupChild: ");
            buf.append("name=");
            buf.append(getName());
            buf.append(", childname=");
            buf.append(nameSegment);
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
    public Hierarchy getMeasuresHierarchy() {
        return measuresHierarchy;
    }

    public List<RolapMember> getMeasuresMembers() {
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
                throw new XOMException(
                    "Got <" + tagName + "> when expecting <CalculatedMember>");
            }
        } catch (XOMException e) {
            throw Util.newError(
                e,
                "Error while creating calculated member from XML ["
                + xml + "]");
        }

        try {
            loadInProgress = true;
            final List<RolapMember> memberList = new ArrayList<RolapMember>();
            createCalcMembersAndNamedSets(
                Collections.singletonList(xmlCalcMember),
                Collections.<MondrianDef.NamedSet>emptyList(),
                memberList,
                new ArrayList<Formula>(),
                this,
                true);
            assert memberList.size() == 1;
            return memberList.get(0);
        } finally {
            loadInProgress = false;
        }
    }

    /**
     * Creates a calculated member.
     *
     * <p>The member will be called [{dimension name}].[{name}].
     *
     * <p>Not for public use.
     *
     * @param hierarchy Hierarchy the calculated member belongs to
     * @param name Name of member
     * @param calc Compiled expression
     */
    RolapMember createCalculatedMember(
        RolapHierarchy hierarchy,
        String name,
        Calc calc)
    {
        final List<Id.Segment> segmentList = new ArrayList<Id.Segment>();
        segmentList.addAll(
            Util.parseIdentifier(hierarchy.getUniqueName()));
        segmentList.add(new Id.NameSegment(name));
        final Formula formula = new Formula(
            new Id(segmentList),
            createDummyExp(calc),
            new MemberProperty[0]);
        final Statement statement =
            schema.getInternalConnection().getInternalStatement();
        try {
            final Query query =
                new Query(
                    statement,
                    this,
                    new Formula[] {formula},
                    new QueryAxis[0],
                    null,
                    new QueryPart[0],
                    new Parameter[0],
                    false);
            query.createValidator().validate(formula);
            calculatedMemberList.add(formula);
            return (RolapMember) formula.getMdxMember();
        } finally {
            statement.close();
        }
    }

    /**
     * Schema reader which works from the perspective of a particular cube
     * (and hence includes calculated members defined in that cube) and also
     * applies the access-rights of a given role.
     */
    private class RolapCubeSchemaReader
        extends RolapSchemaReader
        implements NameResolver.Namespace
    {
        public RolapCubeSchemaReader(Role role) {
            super(role, RolapCube.this.schema);
            assert role != null : "precondition: role != null";
        }

        public List<Member> getLevelMembers(
            Level level,
            boolean includeCalculated)
        {
            List<Member> members = super.getLevelMembers(level, false);
            if (includeCalculated) {
                members = Util.addLevelCalculatedMembers(this, level, members);
            }
            return members;
        }

        public Member getCalculatedMember(List<Id.Segment> nameParts) {
            final String uniqueName = Util.implode(nameParts);
            for (Formula formula : calculatedMemberList) {
                final String formulaUniqueName =
                    formula.getMdxMember().getUniqueName();
                if (formulaUniqueName.equals(uniqueName)
                    && getRole().canAccess(formula.getMdxMember()))
                {
                    return formula.getMdxMember();
                }
            }
            return null;
        }

        public NamedSet getNamedSet(List<Id.Segment> segments) {
            if (segments.size() == 1) {
                Id.Segment segment = segments.get(0);
                for (Formula namedSet : namedSetList) {
                    if (segment.matches(namedSet.getName())) {
                        return namedSet.getNamedSet();
                    }
                }
            }
            return super.getNamedSet(segments);
        }

        public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
            ArrayList<Member> list = new ArrayList<Member>();

            if (getRole().getAccess(hierarchy) == Access.NONE) {
                return list;
            }

            for (Member member : getCalculatedMembers()) {
                if (member.getHierarchy().equals(hierarchy)) {
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

            for (Member member : getCalculatedMembers()) {
                if (member.getLevel().equals(level)) {
                    list.add(member);
                }
            }
            return list;
        }

        public List<Member> getCalculatedMembers() {
            List<Member> list =
                roleToAccessibleCalculatedMembers.get(getRole());
            if (list == null) {
                list = new ArrayList<Member>();
                for (Formula formula : calculatedMemberList) {
                    Member member = formula.getMdxMember();
                    if (getRole().canAccess(member)) {
                        list.add(member);
                    }
                }
                //  calculatedMembers array may not have been initialized
                if (list.size() > 0) {
                    roleToAccessibleCalculatedMembers.put(getRole(), list);
                }
            }
            return list;
        }

        public SchemaReader withoutAccessControl() {
            assert getClass() == RolapCubeSchemaReader.class
                : "Derived class " + getClass() + " must override method";
            return RolapCube.this.getSchemaReader();
        }

        public Member getMemberByUniqueName(
            List<Id.Segment> uniqueNameParts,
            boolean failIfNotFound,
            MatchType matchType)
        {
            Member member =
                (Member) lookupCompound(
                    RolapCube.this,
                    uniqueNameParts,
                    failIfNotFound,
                    Category.Member,
                    matchType);
            if (member == null) {
                assert !failIfNotFound;
                return null;
            }
            if (getRole().canAccess(member)) {
                return member;
            } else {
                if (failIfNotFound) {
                    throw Util.newElementNotFoundException(
                        Category.Member,
                        new IdentifierNode(
                            Util.toOlap4j(uniqueNameParts)));
                }
                return null;
            }
        }

        public Cube getCube() {
            return RolapCube.this;
        }

        public List<NameResolver.Namespace> getNamespaces() {
            final List<NameResolver.Namespace> list =
                new ArrayList<NameResolver.Namespace>();
            list.add(this);
            list.addAll(schema.getSchemaReader().getNamespaces());
            return list;
        }

        public OlapElement lookupChild(
            OlapElement parent,
            IdentifierSegment segment,
            MatchType matchType)
        {
            // ignore matchType
            return lookupChild(parent, segment);
        }

        public OlapElement lookupChild(
            OlapElement parent,
            IdentifierSegment segment)
        {
            // Don't look for stored members, or look for dimensions,
            // hierarchies, levels at all. Only look for calculated members
            // and named sets defined against this cube.

            // Look up calc member.
            for (Formula formula : calculatedMemberList) {
                if (NameResolver.matches(formula, parent, segment)) {
                    return formula.getMdxMember();
                }
            }

            // Look up named set.
            if (parent == RolapCube.this) {
                for (Formula formula : namedSetList) {
                    if (Util.matches(segment, formula.getName())) {
                        return formula.getNamedSet();
                    }
                }
            }

            return null;
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
                if (!calcMembersSeen.contains(calcMember)) {
                  calcMembersSeen.add(calcMember);
                }
                formula.accept(this);

                // now that we've located all measures referenced in the
                // calculated member's formula, resolve the calculated
                // member relative to the virtual cube
                virtualCube.setMeasuresHierarchyMemberReader(
                    new CacheMemberReader(
                        new MeasureMemberSource(
                            virtualCube.measuresHierarchy,
                            Util.<RolapMember>cast(measuresFound))));

                MondrianDef.CalculatedMember xmlCalcMember =
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
                RolapVirtualCubeMeasure virtualCubeMeasure =
                    new RolapVirtualCubeMeasure(
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

        public List<RolapVirtualCubeMeasure> getMeasuresFound()
        {
            return measuresFound;
        }
    }

    public static class CubeComparator implements Comparator<RolapCube>
    {
        public int compare(RolapCube c1, RolapCube c2)
        {
            return c1.getName().compareTo(c2.getName());
        }
    }

    /**
     * Creates an expression that compiles to a given compiled expression.
     *
     * <p>Use this for synthetic expressions that do not correspond to anything
     * in an MDX parse tree, and just need to compile to a particular compiled
     * expression. The expression has minimal amounts of metadata, for example
     * type information, but the function has no name or description.
     *
     * @see mondrian.calc.DummyExp
     */
    static Exp createDummyExp(final Calc calc) {
        return new ResolvedFunCall(
            new FunDefBase("dummy", null, "fn") {
                public Calc compileCall(
                    ResolvedFunCall call, ExpCompiler compiler)
                {
                    return calc;
                }
            },
            new Exp[0],
            calc.getType());
    }



    /**Returns the list of base cubes associated with this cube
     * if this one is a virtual cube,
     * otherwise return just this cube
     *
     * @return the list of base cubes
     */
    public List<RolapCube> getBaseCubes() {
      if (baseCubes == null) {
        baseCubes = findBaseCubes(this);
      }
      return baseCubes;
    }

    /**
     * Locates all base cubes associated with the virtual cube.
     */
    private static List<RolapCube> findBaseCubes(RolapCube cube) {
      if (!cube.isVirtual()) {
        return Collections.singletonList(cube);
      }
      List<RolapCube> cubesList = new ArrayList<RolapCube>();
      Set<RolapCube> cubes = new TreeSet<>(new RolapCube.CubeComparator());
      for (Member member : cube.getMeasures()) {
        if (member instanceof RolapStoredMeasure) {
          cubes.add(((RolapStoredMeasure) member).getCube());
        } else if (member instanceof RolapHierarchy.RolapCalculatedMeasure) {
          RolapCube baseCube =
              ((RolapHierarchy.RolapCalculatedMeasure) member).getBaseCube();
          if (baseCube != null) {
            cubes.add(baseCube);
          }
        }
      }
      cubesList.addAll(cubes);
      return cubesList;
    }
}

// End RolapCube.java
