/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
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
import mondrian.server.Statement;

import org.apache.log4j.Logger;

import org.olap4j.impl.NamedListImpl;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.metadata.NamedList;

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
    private final Larder larder;
    private final RolapCubeHierarchy measuresHierarchy;

    /** Schema reader which can see this cube and nothing else. */
    private SchemaReader schemaReader;

    /**
     * List of calculated members.
     */
    final List<Formula> calculatedMemberList = new ArrayList<Formula>();

    /**
     * Role-based cache of calculated members
     */
    private final SoftSmartCache<Role, List<RolapMember>>
        roleToAccessibleCalculatedMembers =
        new SoftSmartCache<Role, List<RolapMember>>();

    /**
     * List of named sets.
     */
    final List<Formula> namedSetList = new ArrayList<Formula>();

    private ExplicitRules.Group aggGroup;

    final List<RolapCubeHierarchy> hierarchyList =
        new ArrayList<RolapCubeHierarchy>();

    final NamedList<RolapCubeDimension> dimensionList =
        new NamedListImpl<RolapCubeDimension>();

    /**
     * Set to true when a cube is being modified after creation.
     *
     * @see #isLoadInProgress()
     */
    private boolean loadInProgress = false;

    private Map<RolapLevel, RolapCubeLevel> virtualToBaseMap =
        new HashMap<RolapLevel, RolapCubeLevel>();

    private final List<RolapMeasureGroup> measureGroupList =
        new ArrayList<RolapMeasureGroup>();

    private BitKey closureColumnBitKey;

    // set in init
    public RolapGalaxy galaxy;

    RolapCubeHierarchy scenarioHierarchy;

    /**
     * Creates a <code>RolapCube</code> from a regular cube.
     *
     * @param schemaLoader Schema loader
     * @param name Name of cube
     * @param larder Larder
     * @param measuresCaption Caption for measures dimension
     */
    RolapCube(
        RolapSchemaLoader schemaLoader,
        final String name,
        boolean visible,
        final Larder larder,
        final String measuresCaption)
    {
        super(name, visible);

        assert larder != null;
        this.larder = larder;
        this.schema = schemaLoader.schema;

        RolapDimension measuresDimension =
            new RolapDimension(
                schema,
                Dimension.MEASURES_NAME,
                true,
                org.olap4j.metadata.Dimension.Type.MEASURE,
                false,
                Larders.ofCaption(Dimension.MEASURES_NAME, measuresCaption));
        RolapHierarchy measuresHierarchy =
            new RolapHierarchy(
                measuresDimension,
                measuresDimension.getName(),
                Util.quoteMdxIdentifier(Dimension.MEASURES_NAME),
                measuresDimension.isVisible(),
                false,
                null,
                null,
                measuresDimension.getLarder());
        measuresDimension.addHierarchy(measuresHierarchy);
        measuresHierarchy.initHierarchy(schemaLoader, null);

        final List<RolapCubeHierarchy> cubeHierarchyList =
            new ArrayList<RolapCubeHierarchy>();
        final RolapCubeDimension measuresCubeDimension =
            new RolapCubeDimension(
                this,
                measuresDimension,
                measuresDimension.getName(),
                0,
                measuresDimension.getLarder());
        schemaLoader.initCubeDimension(
            measuresCubeDimension, null, cubeHierarchyList);

        dimensionList.add(measuresCubeDimension);
        this.measuresHierarchy =
            measuresCubeDimension.getHierarchyList().get(0);
        this.hierarchyList.add(this.measuresHierarchy);
    }

    public Dimension[] getDimensions() {
        return dimensionList.toArray(new Dimension[dimensionList.size()]);
    }

    public List<? extends RolapCubeDimension> getDimensionList() {
        return Util.cast(dimensionList);
    }

    @Override
    public RolapDimension lookupDimension(Id.Segment s) {
        if (!(s instanceof Id.NameSegment)) {
            return null;
        }
        final Id.NameSegment nameSegment = (Id.NameSegment) s;
        for (RolapDimension dimension : dimensionList) {
            if (Util.equalName(dimension.getName(), nameSegment.name)) {
                return dimension;
            }
        }
        return null;
    }

    /**
     * Makes sure that the schemaReader cache is invalidated.
     * Problems can occur if the measure hierarchy member reader is out
     * of sync with the cache.
     *
     * @param memberReader new member reader for measures hierarchy
     *
     * @see Util#deprecated(Object) make private
     */
    void setMeasuresHierarchyMemberReader(MemberReader memberReader) {
        this.measuresHierarchy.setMemberReader(memberReader);
        // this invalidates any cached schema reader
        this.schemaReader = null;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public Larder getLarder() {
        return larder;
    }

    public String getDescription() {
        return Larders.getDescription(larder);
    }

    public boolean hasAggGroup() {
        return aggGroup != null;
    }

    public ExplicitRules.Group getAggGroup() {
        return aggGroup;
    }

    void setAggGroup(final ExplicitRules.Group aggGroup) {
        this.aggGroup = aggGroup;
    }

    /**
     * Post-initialization, doing things which cannot be done in the
     * constructor.
     */
    void init(final List<RolapMember> measureList) {
        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(
                    measuresHierarchy,
                    measureList)));

        // Initialize closure bit key only when we know how many columns are in
        // the star.
        if (!isVirtual() && measureGroupList.size() > 0) {
            closureColumnBitKey =
                BitKey.Factory.makeBitKey(
                    measureGroupList.get(0).getStar().getColumnCount());
        } else {
            closureColumnBitKey = null;
        }
    }

    /**
     * Post-initialization, doing things which cannot be done until
     * {@link RolapMeasureGroup}s and their {@link RolapStar}s are initialized.
     */
    void init2()
    {
        this.galaxy = new RolapGalaxy(this);
    }

    public RolapSchema getSchema() {
        return schema;
    }

    /**
     * Returns the named sets of this cube.
     */
    public NamedSet[] getNamedSets() {
        List<NamedSet> list = new ArrayList<NamedSet>();
        for (Formula namedSet : namedSetList) {
            list.add(namedSet.getNamedSet());
        }
        return list.toArray(new NamedSet[list.size()]);
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

    /**
     * Returns a list of unique stars in this Cube.
     *
     * @return stars in this Cube
     */
    public List<RolapStar> getStars() {
        List<RolapStar> starList = new ArrayList<RolapStar>();
        for (Member member : getMeasures()) {
            if (member instanceof RolapStoredMeasure) {
                RolapStoredMeasure storedMeasure =
                    (RolapStoredMeasure) member;
                final RolapStar star =
                    storedMeasure.getMeasureGroup().getStar();
                assert star != null : "measure " + member + " has no star";
                if (!starList.contains(star)) {
                    starList.add(star);
                }
            }
        }
        return starList;
    }

    /**
     * Returns this cube's underlying star schema.
     *
     * @deprecated
     */
    public RolapStar getStar() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a list of all hierarchies in this cube, in order of dimension.
     *
     * @deprecated Use {@link #getHierarchyList()}. Will be removed before 4.0.
     *
     * @return List of hierarchies
     */
    public List<RolapHierarchy> getHierarchies() {
        return Util.cast(hierarchyList);
    }

    public List<RolapCubeHierarchy> getHierarchyList() {
        return Util.cast(hierarchyList);
    }

    public boolean isLoadInProgress() {
        return loadInProgress
            || getSchema().getSchemaLoadDate() == null;
    }

    /**
     * Returns the measure groups in this cube.
     *
     * @return Measure groups.
     */
    public List<RolapMeasureGroup> getMeasureGroups() {
        return measureGroupList;
    }

    /**
     * Adds a dimension to this cube. Called by RolapSchemaLoader only.
     *
     * @param dimension Dimension
     */
    void addDimension(RolapCubeDimension dimension) {
        assert dimensionList.get(dimension.getName()) == null;
        dimensionList.add(dimension);
    }

    /**
     * Adds a measure group to this cube. Called by RolapSchemaLoader only.
     *
     * @param measureGroup Measure group
     */
    void addMeasureGroup(RolapMeasureGroup measureGroup) {
        measureGroupList.add(measureGroup);
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
                == org.olap4j.metadata.Dimension.Type.TIME)
            {
                return hierarchy;
            }
        }

        throw MondrianResource.instance().NoTimeDimensionInCube.ex(funName);
    }

    List<Member> getMeasures() {
        Util.deprecated("remove either this or getMeasuresMembers?", false);
        RolapCubeLevel measuresLevel =
            dimensionList.get(0)
                .getHierarchyList().get(0)
                .getLevelList().get(0);
        return getSchemaReader().getLevelMembers(measuresLevel, true);
    }

    /**
     * Returns whether this cube is virtual.
     *
     * @return whether this cube is virtual
     */
    public boolean isVirtual() {
        return Util.deprecated(false, false);
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
    RolapCubeHierarchy findBaseCubeHierarchy(RolapHierarchy hierarchy) {
        for (RolapCubeDimension dimension : dimensionList) {
            if (dimension.getName().equals(
                    hierarchy.getDimension().getName()))
            {
                for (RolapCubeHierarchy hier : dimension.getHierarchyList()) {
                    if (hier.getName().equals(hierarchy.getName())) {
                        return hier;
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

        for (RolapCubeDimension dimension : dimensionList) {
            final String dimensionName = dimension.getName();
            if (dimensionName.equals(levelDimName)
                || (isClosure && dimensionName.equals(closDimName)))
            {
                for (RolapCubeHierarchy hier : dimension.getHierarchyList()) {
                    final String hierarchyName = hier.getName();
                    if (hierarchyName.equals(levelHierName)
                        || (isClosure && hierarchyName.equals(closHierName)))
                    {
                        if (isClosure) {
                            final RolapCubeLevel baseLevel =
                                hier.getLevelList().get(1).getClosedPeer();
                            virtualToBaseMap.put(level, baseLevel);
                            return baseLevel;
                        }
                        for (RolapCubeLevel lvl : hier.getLevelList()) {
                            if (lvl.getName().equals(level.getName())) {
                                virtualToBaseMap.put(level, lvl);
                                return lvl;
                            }
                        }
                    }
                }
            }
        }
        return null;
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
        if (matchType != MatchType.EXACT_SCHEMA) {
            matchType = MatchType.EXACT;
        }
        return super.lookupChild(schemaReader, s, matchType);
    }

    /**
     * Returns the the measures hierarchy.
     */
    public RolapCubeHierarchy getMeasuresHierarchy() {
        return measuresHierarchy;
    }

    public List<RolapMember> getMeasuresMembers() {
        return measuresHierarchy.getMemberReader().getMembers();
    }

    public Member createCalculatedMember(String xml) {
        Member calculatedMember;
        try {
            loadInProgress = true;
            calculatedMember =
                new RolapSchemaLoader(schema).createCalculatedMember(this, xml);
        } finally {
            loadInProgress = false;
        }
        return calculatedMember;
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

        public int getLevelCardinality(
            Level _level,
            boolean approximate,
            boolean materialize)
        {
            int levelCardinality =
                super.getLevelCardinality(
                    _level, approximate, materialize);
            levelCardinality += getCalculatedMembers(_level).size();
            return levelCardinality;
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

        protected List<RolapMember> getCalculatedMembers(RolapCubeLevel level) {
            List<RolapMember> list = new ArrayList<RolapMember>();

            if (getRole().getAccess(level) == Access.NONE) {
                return list;
            }

            for (RolapMember member : _getCalculatedMembers()) {
                if (member.getLevel().equals(level)) {
                    list.add(member);
                }
            }
            return list;
        }

        @Override
        public List<Member> getCalculatedMembers(Level level) {
            return Util.cast(getCalculatedMembers((RolapCubeLevel) level));
        }

        protected List<RolapMember> _getCalculatedMembers() {
            List<RolapMember> list =
                roleToAccessibleCalculatedMembers.get(getRole());
            if (list == null) {
                list = new ArrayList<RolapMember>();
                for (Formula formula : calculatedMemberList) {
                    RolapMember member = (RolapMember) formula.getMdxMember();
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

        @Override
        public List<Member> getCalculatedMembers() {
            return Util.cast(_getCalculatedMembers());
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
                if (!failIfNotFound) {
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
}

// End RolapCube.java

