/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
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
import mondrian.util.Pair;

import org.apache.log4j.Logger;
import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.NamedList;

import java.util.*;

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
    private final Map<String, Annotation> annotationMap;
    private final RolapCubeHierarchy measuresHierarchy;

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

    /**
     * Creates a <code>RolapCube</code> from a regular cube.
     *
     * @param schema Schema cube belongs to
     * @param name Name of cube
     * @param caption Caption of cube
     * @param description Description of cube
     * @param annotationMap Annotations on cube
     * @param measuresCaption Caption for measures dimension
     */
    RolapCube(
        RolapSchema schema,
        final String name,
        final String caption,
        final String description,
        final Map<String, Annotation> annotationMap,
        final String measuresCaption)
    {
        super(name, caption, description);

        assert annotationMap != null;
        this.annotationMap = annotationMap;
        this.caption = caption;
        this.schema = schema;

        final RolapSchemaLoader schemaLoader = new RolapSchemaLoader(schema);

        RolapDimension measuresDimension =
            new RolapDimension(
                schema,
                Dimension.MEASURES_NAME,
                measuresCaption,
                null,
                DimensionType.MeasuresDimension,
                Collections.<String, Annotation>emptyMap());
        RolapHierarchy measuresHierarchy =
            measuresDimension.newHierarchy(null, false, null);
        measuresHierarchy.initMeasures();
        schemaLoader.initDimension(measuresDimension);

        final RolapCubeDimension measuresCubeDimension =
            new RolapCubeDimension(
                null,
                this,
                measuresDimension,
                measuresDimension.getName(),
                null,
                measuresDimension.getCaption(),
                measuresDimension.getDescription(),
                0,
                new ArrayList<RolapCubeHierarchy>(),
                Collections.<String, Annotation>emptyMap());
        schemaLoader.initDimension(measuresCubeDimension);
        dimensionList.add(measuresCubeDimension);
        this.measuresHierarchy =
            (RolapCubeHierarchy)
                measuresCubeDimension.getHierarchyList().get(0);
        this.hierarchyList.add(this.measuresHierarchy);
    }

    public Dimension[] getDimensions() {
        return dimensionList.toArray(new Dimension[dimensionList.size()]);
    }

    @Override
    public RolapDimension lookupDimension(Id.Segment s) {
        for (RolapDimension dimension : dimensionList) {
            if (Util.equalName(dimension.getName(), s.name)) {
                return dimension;
            }
        }
        return null;
    }

    private boolean isWritebackEnabled() {
        Util.deprecated("remove if not used", false);
        boolean writebackEnabled = false;
        for (RolapHierarchy hierarchy : hierarchyList) {
            if (ScenarioImpl.isScenario(hierarchy)) {
                writebackEnabled = true;
            }
        }
        return writebackEnabled;
    }


    /**
     * Makes sure that the schemaReader cache is invalidated.
     * Problems can occur if the measure hierarchy member reader is out
     * of sync with the cache.
     *
     * @param memberReader new member reader for measures hierarchy
     */
    private void setMeasuresHierarchyMemberReader(MemberReader memberReader) {
        this.measuresHierarchy.getRolapHierarchy().setMemberReader(
            memberReader);
        // this invalidates any cached schema reader
        this.schemaReader = null;
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

    void setAggGroup(final ExplicitRules.Group aggGroup) {
        this.aggGroup = aggGroup;
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
            throw schema.fatal(
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
            throw schema.fatal(
                "Cannot convert schema: hierarchies in dimension '"
                + dimension.name
                + "' do not have consistent primary keys",
                dimension,
                null);
        }
        return primaryKeySet.iterator().next();
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
     * Returns a set of the names of the cubes used by a virtual cube. The
     * order is deterministic.
     *
     * @param xmlVirtualCube XML virtual cube
     * @return Set of cube names
     */
    private Set<String> getUsedCubeNames(
        Mondrian3Def.VirtualCube xmlVirtualCube)
    {
        final Set<String> names = new LinkedHashSet<String>();
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
        }
        for (Mondrian3Def.VirtualCubeMeasure virtualCubeMeasure
            : xmlVirtualCube.measures)
        {
            if (virtualCubeMeasure.cubeName != null) {
                names.add(virtualCubeMeasure.cubeName);
            }
        }
        return names;
    }

    /**
     * Post-initialization, doing things which cannot be done in the
     * constructor.
     */
    void init(final List<RolapMember> measureList, Member defaultMeasure)
    {
        setMeasuresHierarchyMemberReader(
            new CacheMemberReader(
                new MeasureMemberSource(measuresHierarchy, measureList)));

        this.measuresHierarchy.setDefaultMember(defaultMeasure);

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
            RoleImpl schemaDefaultRoleImpl = schema.getDefaultRole();
            RoleImpl roleImpl = schemaDefaultRoleImpl.makeMutableClone();
            roleImpl.grant(this, Access.ALL);
            schemaReader = new RolapCubeSchemaReader(roleImpl);
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
            if (member instanceof RolapBaseCubeMeasure) {
                RolapBaseCubeMeasure storedMeasure =
                    (RolapBaseCubeMeasure) member;
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
     * <p>TODO: Make this method return RolapCubeHierarchy, when the measures
     * hierarchy is a RolapCubeHierarchy.
     *
     * @return List of hierarchies
     */
    public List<RolapHierarchy> getHierarchies() {
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
                == DimensionType.TimeDimension)
            {
                return hierarchy;
            }
        }

        throw MondrianResource.instance().NoTimeDimensionInCube.ex(funName);
    }

    List<Member> getMeasures() {
        Util.deprecated("remove either this or getMeasuresMembers?", false);
        Level measuresLevel =
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
    RolapHierarchy findBaseCubeHierarchy(RolapHierarchy hierarchy) {
        for (RolapCubeDimension dimension : dimensionList) {
            if (dimension.getName().equals(
                hierarchy.getDimension().getName()))
            {
                for (RolapCubeHierarchy hier
                    : dimension .getRolapCubeHierarchyList())
                {
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
                for (RolapCubeHierarchy hier
                    : dimension.getRolapCubeHierarchyList())
                {
                    final String hierarchyName = hier.getName();
                    if (hierarchyName.equals(levelHierName)
                        || (isClosure && hierarchyName.equals(closHierName)))
                    {
                        if (isClosure) {
                            final RolapCubeLevel baseLevel =
                                hier.getRolapCubeLevelList().get(1)
                                    .getClosedPeer();
                            virtualToBaseMap.put(level, baseLevel);
                            return baseLevel;
                        }
                        for (RolapCubeLevel lvl
                            : hier.getRolapCubeLevelList())
                        {
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
    public Hierarchy getMeasuresHierarchy() {
        return measuresHierarchy;
    }

    public List<RolapMember> getMeasuresMembers() {
        return measuresHierarchy.getMemberReader().getMembers();
    }

    public Member createCalculatedMember(String xml) {
        return new RolapSchemaLoader(schema).createCalculatedMember(this, xml);
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
        segmentList.add(new Id.Segment(name, Id.Quoting.QUOTED));
        final Formula formula = new Formula(
            new Id(segmentList),
            createDummyExp(calc),
            new MemberProperty[0]);
        final Query query =
            new Query(
                schema.getInternalConnection(),
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
    private class RolapCubeSchemaReader extends RolapSchemaReader {
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
}

// End RolapCube.java

