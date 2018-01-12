/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2018 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.VisualTotalsFunDef;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.util.UnsupportedList;

import java.sql.SQLException;
import java.util.*;

/**
 * Hierarchy that is associated with a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeHierarchy extends RolapHierarchy {

    private final boolean cachingEnabled =
        MondrianProperties.instance().EnableRolapCubeMemberCache.get();
    private final RolapCubeDimension cubeDimension;
    private final RolapHierarchy rolapHierarchy;
    private final RolapCubeLevel currentNullLevel;
    private RolapCubeMember currentNullMember;
    private RolapCubeMember currentAllMember;
    private final MondrianDef.RelationOrJoin currentRelation;
    private final RolapCubeHierarchyMemberReader reader;
    private HierarchyUsage usage;
    private final Map<String, String> aliases = new HashMap<String, String>();
    private RolapCubeMember currentDefaultMember;
    private final int ordinal;

    /**
     * True if the hierarchy is degenerate - has no dimension table of its own,
     * just drives from the cube's fact table.
     */
    protected final boolean usingCubeFact;

    /**
     * Length of prefix to be removed when translating member unique names, or
     * 0 if no translation is necessary.
     */
    private final int removePrefixLength;

    // redundant copy of {@link #levels} with tigher type
    private final RolapCubeLevel[] cubeLevels;

    /**
     * Creates a RolapCubeHierarchy.
     *
     * @param cubeDimension Dimension
     * @param cubeDim XML dimension element
     * @param rolapHierarchy Wrapped hierarchy
     * @param subName Name of hierarchy within dimension
     * @param ordinal Ordinal of hierarchy within cube
     */
    public RolapCubeHierarchy(
        RolapCubeDimension cubeDimension,
        MondrianDef.CubeDimension cubeDim,
        RolapHierarchy rolapHierarchy,
        String subName,
        int ordinal)
    {
      this(
          cubeDimension,
          cubeDim,
          rolapHierarchy,
          subName,
          ordinal, null);
    }

    /**
     * Creates a RolapCubeHierarchy.
     *
     * @param cubeDimension Dimension
     * @param cubeDim XML dimension element
     * @param rolapHierarchy Wrapped hierarchy
     * @param subName Name of hierarchy within dimension
     * @param ordinal Ordinal of hierarchy within cube
     * @param factCube Optional - specified for virtual cube dimension
     */
    public RolapCubeHierarchy(
        RolapCubeDimension cubeDimension,
        MondrianDef.CubeDimension cubeDim,
        RolapHierarchy rolapHierarchy,
        String subName,
        int ordinal,
        RolapCube factCube)
    {
        super(
            cubeDimension,
            subName,
            applyPrefix(cubeDim, rolapHierarchy.getCaption()),
            rolapHierarchy.isVisible(),
            applyPrefix(cubeDim, rolapHierarchy.getDescription()),
            rolapHierarchy.hasAll(),
            null,
            rolapHierarchy.getAnnotationMap());
        this.ordinal = ordinal;
        final boolean cubeIsVirtual = cubeDimension.getCube().isVirtual();
        if (!cubeIsVirtual) {
            this.usage =
                new HierarchyUsage(
                    cubeDimension.getCube(), rolapHierarchy, cubeDim);
        }

        this.rolapHierarchy = rolapHierarchy;
        this.cubeDimension = cubeDimension;
        this.xmlHierarchy = rolapHierarchy.getXmlHierarchy();
        // this relation should equal the name of the new dimension table
        // The null member belongs to a level with very similar properties to
        // the 'all' level.
        this.currentNullLevel = new RolapCubeLevel(nullLevel, this);

        if (factCube == null) {
          factCube = cubeDimension.getCube();
        }

        usingCubeFact =
            (factCube == null
              || factCube.getFact() == null
              || factCube.getFact().equals(
                  rolapHierarchy.getRelation()));

        // re-alias names if necessary
        if (!cubeIsVirtual && !usingCubeFact) {
            // join expressions are columns only
            assert (usage.getJoinExp() instanceof MondrianDef.Column);
            currentRelation =
                this.cubeDimension.getCube().getStar().getUniqueRelation(
                    rolapHierarchy.getRelation(),
                    usage.getForeignKey(),
                    ((MondrianDef.Column)usage.getJoinExp()).getColumnName(),
                    usage.getJoinTable().getAlias());
        } else {
            currentRelation = rolapHierarchy.getRelation();
        }
        extractNewAliases(rolapHierarchy.getRelation(), currentRelation);
        this.relation = currentRelation;
        this.levels =
            this.cubeLevels =
                new RolapCubeLevel[rolapHierarchy.getLevels().length];
        for (int i = 0; i < rolapHierarchy.getLevels().length; i++) {
            this.cubeLevels[i] =
                new RolapCubeLevel(
                    (RolapLevel) rolapHierarchy.getLevels()[i], this);
            if (i == 0) {
                if (rolapHierarchy.getAllMember() != null) {
                    RolapCubeLevel allLevel;
                    if (hasAll()) {
                        allLevel = this.cubeLevels[0];
                    } else {
                        // create an all level if one doesn't normally
                        // exist in the hierarchy
                        allLevel =
                            new RolapCubeLevel(
                                rolapHierarchy.getAllMember().getLevel(),
                                this);
                        allLevel.init(cubeDimension.xmlDimension);
                    }

                    this.currentAllMember =
                        new RolapAllCubeMember(
                            rolapHierarchy.getAllMember(),
                            allLevel);
                }
            }
        }

        // Compute whether the unique names of members of this hierarchy are
        // different from members of the underlying hierarchy. If so, compute
        // the length of the prefix to be removed before this hierarchy's unique
        // name is added. For example, if this.uniqueName is "[Ship Time]" and
        // rolapHierarchy.uniqueName is "[Time]", remove prefixLength will be
        // length("[Ship Time]") = 11.
        if (uniqueName.equals(rolapHierarchy.getUniqueName())) {
            this.removePrefixLength = 0;
        } else {
            this.removePrefixLength = rolapHierarchy.getUniqueName().length();
        }

        if (cubeDimension.isHighCardinality() || !cachingEnabled) {
            this.reader = new NoCacheRolapCubeHierarchyMemberReader();
        } else {
            this.reader = new CacheRolapCubeHierarchyMemberReader();
        }
    }

    /**
     * Applies a prefix to a caption or description of a hierarchy in a shared
     * dimension. Ensures that if a dimension is used more than once in the same
     * cube then the hierarchies are distinguishable.
     *
     * <p>For example, if the [Time] dimension is imported as [Order Time] and
     * [Ship Time], then the [Time].[Weekly] hierarchy would have caption
     * "Order Time.Weekly caption" and description "Order Time.Weekly
     * description".
     *
     * <p>If the dimension usage has a caption, it overrides.
     *
     * <p>If the dimension usage has a null name, or the name is the same
     * as the dimension, and no caption, then no prefix is applied.
     *
     * @param cubeDim Cube dimension (maybe a usage of a shared dimension)
     * @param caption Caption or description
     * @return Caption or description, possibly prefixed by dimension role name
     */
    private static String applyPrefix(
        MondrianDef.CubeDimension cubeDim,
        String caption)
    {
        if (caption == null) {
            return null;
        }
        if (cubeDim instanceof MondrianDef.DimensionUsage) {
            final MondrianDef.DimensionUsage dimensionUsage =
                (MondrianDef.DimensionUsage) cubeDim;
            if (dimensionUsage.name != null
                && !dimensionUsage.name.equals(dimensionUsage.source))
            {
                if (dimensionUsage.caption != null) {
                    return dimensionUsage.caption + "." + caption;
                } else {
                    return dimensionUsage.name + "." + caption;
                }
            }
        }
        return caption;
    }

    @Override
    public RolapCubeLevel[] getLevels() {
        return cubeLevels;
    }

    public String getAllMemberName() {
        return rolapHierarchy.getAllMemberName();
    }

    public String getSharedHierarchyName() {
        return rolapHierarchy.getSharedHierarchyName();
    }

    public String getAllLevelName() {
        return rolapHierarchy.getAllLevelName();
    }

    public boolean isUsingCubeFact() {
        return usingCubeFact;
    }

    public String lookupAlias(String origTable) {
        return aliases.get(origTable);
    }

    public final RolapHierarchy getRolapHierarchy() {
        return rolapHierarchy;
    }

    public final int getOrdinalInCube() {
        return ordinal;
    }

    /**
     * Populates the alias map for the old and new relations.
     *
     * <p>This method may be simplified when we obsolete
     * {@link mondrian.rolap.HierarchyUsage}.
     *
     * @param oldrel Original relation, as defined in the schema
     * @param newrel New star relation, generated by RolapStar, canonical, and
     * shared between all cubes with similar structure
     */
    protected void extractNewAliases(
        MondrianDef.RelationOrJoin oldrel,
        MondrianDef.RelationOrJoin newrel)
    {
        if (oldrel == null && newrel == null) {
            return;
        } else if (oldrel instanceof MondrianDef.Relation
            && newrel instanceof MondrianDef.Relation)
        {
            aliases.put(
                ((MondrianDef.Relation) oldrel).getAlias(),
                ((MondrianDef.Relation) newrel).getAlias());
        } else if (oldrel instanceof MondrianDef.Join
            && newrel instanceof MondrianDef.Join)
        {
            MondrianDef.Join oldjoin = (MondrianDef.Join)oldrel;
            MondrianDef.Join newjoin = (MondrianDef.Join)newrel;
            extractNewAliases(oldjoin.left, newjoin.left);
            extractNewAliases(oldjoin.right, newjoin.right);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapCubeHierarchy)) {
            return false;
        }

        RolapCubeHierarchy that = (RolapCubeHierarchy)o;
        return cubeDimension.equals(that.cubeDimension)
            && getUniqueName().equals(that.getUniqueName());
    }

    protected int computeHashCode() {
        return Util.hash(super.computeHashCode(), this.cubeDimension.cube);
    }

    public Member createMember(
        Member parent,
        Level level,
        String name,
        Formula formula)
    {
        RolapLevel rolapLevel = ((RolapCubeLevel)level).getRolapLevel();
        if (formula == null) {
            RolapMember rolapParent = null;
            if (parent != null) {
                rolapParent = ((RolapCubeMember)parent).getRolapMember();
            }
            RolapMember member =
                new RolapMemberBase(rolapParent, rolapLevel, name);
            return new RolapCubeMember(
                (RolapCubeMember) parent, member,
                (RolapCubeLevel) level);
        } else if (level.getDimension().isMeasures()) {
            RolapCalculatedMeasure member =
                new RolapCalculatedMeasure(
                    (RolapMember) parent, rolapLevel, name, formula);
            return new RolapCubeMember(
                (RolapCubeMember) parent, member,
                (RolapCubeLevel) level);
        } else {
            RolapCalculatedMember member =
                new RolapCalculatedMember(
                    (RolapMember) parent, rolapLevel, name, formula);
            return new RolapCubeMember(
                (RolapCubeMember) parent, member,
                (RolapCubeLevel) level);
        }
    }


    boolean tableExists(String tableName) {
        return rolapHierarchy.tableExists(tableName);
    }

    /**
     * The currentRelation object is derived from the shared relation object
     * it is generated via the RolapStar object, and contains unique aliases
     * for it's particular join path
     *
     * @return rolap cube hierarchy relation
     */
    public MondrianDef.RelationOrJoin getRelation() {
        return currentRelation;
    }

    // override with stricter return type; make final, important for performance
    public final RolapCubeMember getDefaultMember() {
        if (currentDefaultMember == null) {
            reader.getRootMembers();
            currentDefaultMember =
                bootstrapLookup(
                    (RolapMember) rolapHierarchy.getDefaultMember());
        }
        return currentDefaultMember;
    }

    /**
     * Looks up a {@link RolapCubeMember} corresponding to a {@link RolapMember}
     * of the underlying hierarchy. Safe to be called while the hierarchy is
     * initializing.
     *
     * @param rolapMember Member of underlying hierarchy
     * @return Member of this hierarchy
     */
    private RolapCubeMember bootstrapLookup(RolapMember rolapMember) {
        RolapCubeMember parent =
            rolapMember.getParentMember() == null
                ? null
                : rolapMember.getParentMember().isAll()
                    ? currentAllMember
                    : bootstrapLookup(rolapMember.getParentMember());
        RolapCubeLevel level = cubeLevels[rolapMember.getLevel().getDepth()];
        return reader.lookupCubeMember(parent, rolapMember, level);
    }

    public Member getNullMember() {
        // use lazy initialization to get around bootstrap issues
        if (currentNullMember == null) {
            currentNullMember =
                new RolapCubeMember(
                    null,
                    (RolapMember) rolapHierarchy.getNullMember(),
                    currentNullLevel);
        }
        return currentNullMember;
    }

    /**
     * Returns the 'all' member.
     */
    public RolapCubeMember getAllMember() {
        return currentAllMember;
    }

    void setMemberReader(MemberReader memberReader) {
        rolapHierarchy.setMemberReader(memberReader);
    }

    MemberReader getMemberReader() {
        return reader;
    }

    public void setDefaultMember(Member defaultMeasure) {
        // refactor this!
        rolapHierarchy.setDefaultMember(defaultMeasure);

        RolapCubeLevel level =
            new RolapCubeLevel(
                (RolapLevel)rolapHierarchy.getDefaultMember().getLevel(),
                this);
        currentDefaultMember =
            new RolapCubeMember(
                null,
                (RolapMember) rolapHierarchy.getDefaultMember(),
                level);
    }

    void init(MondrianDef.CubeDimension xmlDimension) {
        // first init shared hierarchy
        rolapHierarchy.init(xmlDimension);
        // second init cube hierarchy
        super.init(xmlDimension);
    }

    /**
     * Converts the unique name of a member of the underlying hierarchy to
     * the appropriate name for this hierarchy.
     *
     * <p>For example, if the shared hierarchy is [Time].[Quarterly] and the
     * hierarchy usage is [Ship Time].[Quarterly], then [Time].[1997].[Q1] would
     * be translated to [Ship Time].[Quarerly].[1997].[Q1].
     *
     * @param memberUniqueName Unique name of member from underlying hierarchy
     * @return Translated unique name
     */
    final String convertMemberName(String memberUniqueName) {
        if (removePrefixLength > 0
            && !memberUniqueName.startsWith(uniqueName))
        {
            return uniqueName + memberUniqueName.substring(removePrefixLength);
        }
        return memberUniqueName;
    }

    public final RolapCube getCube() {
        return cubeDimension.cube;
    }

    private static RolapCubeMember createAncestorMembers(
        RolapCubeHierarchyMemberReader memberReader,
        RolapCubeLevel level,
        RolapMember member)
    {
        if (member == null) {
            return null;
        }
        RolapCubeMember parent = null;
        if (member.getParentMember() != null) {
            parent =
                createAncestorMembers(
                    memberReader,
                    level.getParentLevel(),
                    member.getParentMember());
        }
        return memberReader.lookupCubeMember(parent, member, level);
    }

    /**
     * TODO: Since this is part of a caching strategy, should be implemented
     * as a Strategy Pattern, avoiding hierarchy.
     */
    public static interface RolapCubeHierarchyMemberReader
        extends MemberReader
    {
        public RolapCubeMember lookupCubeMember(
            final RolapCubeMember parent,
            final RolapMember member,
            final RolapCubeLevel level);

        public MemberCacheHelper getRolapCubeMemberCacheHelper();
    }

    /******

     RolapCubeMember Caching Approach:

     - RolapHierarchy.SmartMemberReader.SmartCacheHelper ->
       This is the shared cache across shared hierarchies.  This
       member cache only
       contains members loaded by non-cube specific member lookups.  This cache
       should only contain RolapMembers, not RolapCubeMembers

     - RolapCubeHierarchy.RolapCubeHierarchyMemberReader.rolapCubeCacheHelper ->
       This cache contains the RolapCubeMember objects, which are cube specific
       wrappers of shared members.

     - RolapCubeHierarchy.RolapCubeHierarchyMemberReader.SmartCacheHelper ->
       This is the inherited shared cache from SmartMemberReader, and
       is used when a join with the fact table is necessary, aka a
       SqlContextConstraint is used. This cache may be redundant with
       rolapCubeCacheHelper.

     - A Special note regarding RolapCubeHierarchyMemberReader.cubeSource -
       This class was required for the special situation getMemberBuilder()
       method call from RolapNativeSet.  This class utilizes both the
       rolapCubeCacheHelper class for storing RolapCubeMembers, and also the
       RolapCubeHierarchyMemberReader's inherited SmartCacheHelper.


     ******/


    /**
     *  member reader wrapper - uses existing member reader,
     *  but wraps and caches all intermediate members.
     *
     *  <p>Synchronization. Most synchronization takes place within
     * SmartMemberReader.  All synchronization is done on the cacheHelper
     * object.
      */
    public class CacheRolapCubeHierarchyMemberReader
        extends SmartMemberReader
        implements RolapCubeHierarchyMemberReader
    {
        /**
         * cubeSource is passed as our member builder
         */
        protected final RolapCubeSqlMemberSource cubeSource;

        /**
         * this cache caches RolapCubeMembers that are light wrappers around
         * shared and non-shared Hierarchy RolapMembers.  The inherited
         * cacheHelper object contains non-shared hierarchy RolapMembers.
         * non-shared hierarchy RolapMembers are created when a member lookup
         * involves the Cube's fact table.
         */
        protected MemberCacheHelper rolapCubeCacheHelper;
        private final boolean enableCache =
            MondrianProperties.instance().EnableRolapCubeMemberCache.get();

        public CacheRolapCubeHierarchyMemberReader() {
            super(new SqlMemberSource(RolapCubeHierarchy.this));
            rolapCubeCacheHelper =
                new MemberCacheHelper(RolapCubeHierarchy.this);

            cubeSource =
                new RolapCubeSqlMemberSource(
                    this,
                    RolapCubeHierarchy.this,
                    rolapCubeCacheHelper,
                    cacheHelper);

            cubeSource.setCache(getMemberCache());
        }

        public MemberBuilder getMemberBuilder() {
            return this.cubeSource;
        }

        public MemberCacheHelper getRolapCubeMemberCacheHelper() {
            return rolapCubeCacheHelper;
        }

        public List<RolapMember> getRootMembers() {
            if (rootMembers == null) {
                rootMembers = getMembersInLevel(cubeLevels[0]);
            }
            return rootMembers;
        }

        protected void readMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            List<RolapMember> rolapChildren = new ArrayList<RolapMember>();
            List<RolapMember> rolapParents = new ArrayList<RolapMember>();
            Map<String, RolapCubeMember> lookup =
                new HashMap<String, RolapCubeMember>();

            // extract RolapMembers from their RolapCubeMember objects
            // populate lookup for reconnecting parents and children
            for (RolapMember member : parentMembers) {
                if (member instanceof VisualTotalsFunDef.VisualTotalMember) {
                    continue;
                }
                final RolapCubeMember cubeMember = (RolapCubeMember) member;
                final RolapMember rolapMember = cubeMember.getRolapMember();
                lookup.put(rolapMember.getUniqueName(), cubeMember);
                rolapParents.add(rolapMember);
            }

            // get member children from shared member reader if possible,
            // if not get them from our own source
            boolean joinReq =
                (constraint instanceof SqlContextConstraint);
            if (joinReq) {
                super.readMemberChildren(
                    parentMembers, rolapChildren, constraint);
            } else {
                rolapHierarchy.getMemberReader().getMemberChildren(
                    rolapParents, rolapChildren, constraint);
            }

            // now lookup or create RolapCubeMember
            for (RolapMember currMember : rolapChildren) {
                RolapCubeMember parent =
                    lookup.get(
                        currMember.getParentMember().getUniqueName());
                RolapCubeLevel level =
                    parent.getLevel().getChildLevel();
                if (level == null) {
                    // most likely a parent child hierarchy
                    level = parent.getLevel();
                }
                RolapCubeMember newmember =
                    lookupCubeMember(
                        parent, currMember, level);
                children.add(newmember);
            }

            // Put them in a temporary hash table first. Register them later,
            // when we know their size (hence their 'cost' to the cache pool).
            Map<RolapMember, List<RolapMember>> tempMap =
                new HashMap<RolapMember, List<RolapMember>>();
            for (RolapMember member1 : parentMembers) {
                tempMap.put(member1, Collections.<RolapMember>emptyList());
            }

            // note that this stores RolapCubeMembers in our cache,
            // which also stores RolapMembers.

            for (RolapMember child : children) {
            // todo: We could optimize here. If members.length is small, it's
            // more efficient to drive from members, rather than hashing
            // children.length times. We could also exploit the fact that the
            // result is sorted by ordinal and therefore, unless the "members"
            // contains members from different levels, children of the same
            // member will be contiguous.
                assert child != null : "child";
                final RolapMember parentMember = child.getParentMember();
                List<RolapMember> cacheList = tempMap.get(parentMember);
                if (cacheList == null) {
                    // The list is null if, due to dropped constraints, we now
                    // have a children list of a member we didn't explicitly
                    // ask for it. Adding it to the cache would be viable, but
                    // let's ignore it.
                    continue;
                } else if (cacheList == Collections.EMPTY_LIST) {
                    cacheList = new ArrayList<RolapMember>();
                    tempMap.put(parentMember, cacheList);
                }
                cacheList.add(child);
            }

            synchronized (cacheHelper) {
                for (Map.Entry<RolapMember, List<RolapMember>> entry
                    : tempMap.entrySet())
                {
                    final RolapMember member = entry.getKey();
                    if (rolapCubeCacheHelper.getChildrenFromCache(
                            member, constraint) == null)
                    {
                        final List<RolapMember> cacheList = entry.getValue();
                        if (enableCache) {
                            rolapCubeCacheHelper.putChildren(
                                member, constraint, cacheList);
                        }
                    }
                }
            }
        }

        public Map<? extends Member, Access> getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            synchronized (cacheHelper) {
                checkCacheStatus();

                List<RolapMember> missed = new ArrayList<RolapMember>();
                for (RolapMember parentMember : parentMembers) {
                    List<RolapMember> list =
                        rolapCubeCacheHelper.getChildrenFromCache(
                            parentMember, constraint);
                    if (list == null) {
                        // the null member has no children
                        if (!parentMember.isNull()) {
                            missed.add(parentMember);
                        }
                    } else {
                        children.addAll(list);
                    }
                }
                if (missed.size() > 0) {
                    readMemberChildren(missed, children, constraint);
                }
            }
            return Util.toNullValuesMap(children);
        }


        public List<RolapMember> getMembersInLevel(
            RolapLevel level,
            TupleConstraint constraint)
        {
            synchronized (cacheHelper) {
                checkCacheStatus();

                List<RolapMember> members =
                    rolapCubeCacheHelper.getLevelMembersFromCache(
                        level, constraint);
                if (members != null) {
                    return members;
                }

                // if a join is required, we need to pass in the RolapCubeLevel
                // vs. the regular level
                boolean joinReq =
                    (constraint instanceof SqlContextConstraint);
                List<RolapMember> list;
                final RolapCubeLevel cubeLevel = (RolapCubeLevel) level;
                if (!joinReq) {
                    list =
                        rolapHierarchy.getMemberReader().getMembersInLevel(
                            cubeLevel.getRolapLevel(), constraint);
                } else {
                    list =
                        super.getMembersInLevel(
                            level, constraint);
                }
                List<RolapMember> newlist = new ArrayList<RolapMember>();
                for (RolapMember member : list) {
                    // note that there is a special case for the all member

                    // REVIEW: disabled, to see what happens. if this code is
                    // for performance, we should check level.isAll at the top
                    // of the method; if it is for correctness, leave the code
                    // in
                    if (false && member == rolapHierarchy.getAllMember()) {
                        newlist.add(getAllMember());
                    } else {
                        RolapCubeMember cubeMember =
                                lookupCubeMemberWithParent(
                                        member,
                                        cubeLevel);
                        newlist.add(cubeMember);
                    }
                }
                rolapCubeCacheHelper.putLevelMembersInCache(
                    level, constraint, newlist);

                return newlist;
            }
        }

        private RolapCubeMember lookupCubeMemberWithParent(
            RolapMember member,
            RolapCubeLevel cubeLevel)
        {
            final RolapMember parentMember = member.getParentMember();
            final RolapCubeMember parentCubeMember;
            if (parentMember == null) {
                parentCubeMember = null;
            } else {
                // In parent-child hierarchies, a member's parent may be in the
                // same level.
                final RolapCubeLevel parentLevel =
                    parentMember.getLevel() == member.getLevel()
                        ? cubeLevel
                        : cubeLevel.getParentLevel();
                parentCubeMember =
                    lookupCubeMemberWithParent(
                        parentMember, parentLevel);
            }
            return lookupCubeMember(
                parentCubeMember, member, cubeLevel);
        }

        @Override
        public RolapMember getMemberByKey(
            RolapLevel level, List<Comparable> keyValues)
        {
            synchronized (cacheHelper) {
                final RolapMember member =
                    super.getMemberByKey(level, keyValues);
                return createAncestorMembers(
                    this, (RolapCubeLevel) level, member);
            }
        }

        public RolapCubeMember lookupCubeMember(
            RolapCubeMember parent,
            RolapMember member,
            RolapCubeLevel level)
        {
            synchronized (cacheHelper) {
                if (member.getKey() == RolapUtil.sqlNullValue) {
                    if (member.isAll()) {
                        return getAllMember();
                    }
                }

                RolapCubeMember cubeMember;
                if (enableCache) {
                    Object key =
                        rolapCubeCacheHelper.makeKey(parent, member.getKey());
                    cubeMember = (RolapCubeMember)
                        rolapCubeCacheHelper.getMember(key, false);
                    if (cubeMember == null) {
                        cubeMember =
                            new RolapCubeMember(parent, member, level);
                        rolapCubeCacheHelper.putMember(key, cubeMember);
                    } else {
                      if (level.hasOrdinalExp()) {
                        fixOrdinal(cubeMember, member.getOrdinal());
                      }
                    }
                } else {
                    cubeMember = new RolapCubeMember(parent, member, level);
                }
                return cubeMember;
            }
        }

        private void fixOrdinal(
            RolapCubeMember rlCubeMemberToFix,
            int ordinalToSet)
        {
          RolapMember rolapMember = rlCubeMemberToFix.getRolapMember();
          if (rolapMember instanceof RolapMemberBase) {
            ((RolapMemberBase) rolapMember).setOrdinal(ordinalToSet, true);
          }
        }

        public int getMemberCount() {
            return rolapHierarchy.getMemberReader().getMemberCount();
        }

        protected void checkCacheStatus() {
            synchronized (cacheHelper) {
                // if necessary, flush all caches:
                //   - shared SmartMemberReader RolapMember cache
                //   - local key to cube member RolapCubeMember cache
                //   - cube source RolapCubeMember cache
                //   - local regular RolapMember cache, used when cube
                //     specific joins occur

                if (cacheHelper.getChangeListener() != null) {
                    if (cacheHelper.getChangeListener().isHierarchyChanged(
                            getHierarchy()))
                    {
                        cacheHelper.flushCache();
                        rolapCubeCacheHelper.flushCache();

                        if (rolapHierarchy.getMemberReader()
                                instanceof SmartMemberReader)
                        {
                            SmartMemberReader smartMemberReader =
                                (SmartMemberReader)
                                    rolapHierarchy.getMemberReader();
                            if (smartMemberReader.getMemberCache()
                                    instanceof MemberCacheHelper)
                            {
                                MemberCacheHelper helper =
                                    (MemberCacheHelper)
                                        smartMemberReader.getMemberCache();
                                helper.flushCache();
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Same as {@link RolapCubeHierarchyMemberReader} but without caching
     * anything.
     */
    public class NoCacheRolapCubeHierarchyMemberReader
        extends NoCacheMemberReader
        implements RolapCubeHierarchyMemberReader
    {
        /**
         * cubeSource is passed as our member builder
         */
        protected final RolapCubeSqlMemberSource cubeSource;

        /**
         * this cache caches RolapCubeMembers that are light wrappers around
         * shared and non-shared Hierarchy RolapMembers.  The inherited
         * cacheHelper object contains non-shared hierarchy RolapMembers.
         * non-shared hierarchy RolapMembers are created when a member lookup
         * involves the Cube's fact table.
         */
        protected MemberCacheHelper rolapCubeCacheHelper;

        public NoCacheRolapCubeHierarchyMemberReader() {
            super(new SqlMemberSource(RolapCubeHierarchy.this));
            rolapCubeCacheHelper =
                new MemberNoCacheHelper();

            cubeSource =
                new RolapCubeSqlMemberSource(
                    this,
                    RolapCubeHierarchy.this,
                    rolapCubeCacheHelper,
                    new MemberNoCacheHelper());

            cubeSource.setCache(rolapCubeCacheHelper);
        }

        public MemberBuilder getMemberBuilder() {
            return this.cubeSource;
        }

        public MemberCacheHelper getRolapCubeMemberCacheHelper() {
            return rolapCubeCacheHelper;
        }

        public List<RolapMember> getRootMembers() {
            return getMembersInLevel(cubeLevels[0]);
        }

        protected void readMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            List<RolapMember> rolapChildren = new ArrayList<RolapMember>();
            List<RolapMember> rolapParents = new ArrayList<RolapMember>();
            Map<String, RolapCubeMember> lookup =
                new HashMap<String, RolapCubeMember>();

            // extract RolapMembers from their RolapCubeMember objects
            // populate lookup for reconnecting parents and children
            final List<RolapCubeMember> parentRolapCubeMemberList =
                Util.cast(parentMembers);
            for (RolapCubeMember member : parentRolapCubeMemberList) {
                final RolapMember rolapMember = member.getRolapMember();
                lookup.put(rolapMember.getUniqueName(), member);
                rolapParents.add(rolapMember);
            }

            // get member children from shared member reader if possible,
            // if not get them from our own source
            boolean joinReq =
                (constraint instanceof SqlContextConstraint);
            if (joinReq) {
                super.readMemberChildren(
                    parentMembers, rolapChildren, constraint);
            } else {
                rolapHierarchy.getMemberReader().getMemberChildren(
                    rolapParents, rolapChildren, constraint);
            }

            // now lookup or create RolapCubeMember
            for (RolapMember currMember : rolapChildren) {
                RolapCubeMember parent =
                    lookup.get(
                        currMember.getParentMember().getUniqueName());
                RolapCubeLevel level =
                    parent.getLevel().getChildLevel();
                if (level == null) {
                    // most likely a parent child hierarchy
                    level = parent.getLevel();
                }
                RolapCubeMember newmember =
                    lookupCubeMember(
                        parent, currMember, level);
                children.add(newmember);
            }

            // Put them in a temporary hash table first. Register them later,
            // when we know their size (hence their 'cost' to the cache pool).
            Map<RolapMember, List<RolapMember>> tempMap =
                new HashMap<RolapMember, List<RolapMember>>();
            for (RolapMember member1 : parentMembers) {
                tempMap.put(member1, Collections.<RolapMember>emptyList());
            }

            // note that this stores RolapCubeMembers in our cache,
            // which also stores RolapMembers.

            for (RolapMember child : children) {
            // todo: We could optimize here. If members.length is small, it's
            // more efficient to drive from members, rather than hashing
            // children.length times. We could also exploit the fact that the
            // result is sorted by ordinal and therefore, unless the "members"
            // contains members from different levels, children of the same
            // member will be contiguous.
                assert child != null : "child";
                final RolapMember parentMember = child.getParentMember();
            }
        }

        public Map<? extends Member, Access> getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            List<RolapMember> missed = new ArrayList<RolapMember>();
            for (RolapMember parentMember : parentMembers) {
                // the null member has no children
                if (!parentMember.isNull()) {
                    missed.add(parentMember);
                }
            }
            if (missed.size() > 0) {
                readMemberChildren(missed, children, constraint);
            }
            return Util.toNullValuesMap(children);
        }


        public List<RolapMember> getMembersInLevel(
            final RolapLevel level,
            TupleConstraint constraint)
        {
                List<RolapMember> members = null;

                // if a join is required, we need to pass in the RolapCubeLevel
                // vs. the regular level
                boolean joinReq =
                    (constraint instanceof SqlContextConstraint);
                final List<RolapMember> list;

                if (!joinReq) {
                    list =
                        rolapHierarchy.getMemberReader().getMembersInLevel(
                            ((RolapCubeLevel) level).getRolapLevel(),
                            constraint);
                } else {
                    list =
                        super.getMembersInLevel(
                            level, constraint);
                }

                return new UnsupportedList<RolapMember>() {
                    public RolapMember get(final int index) {
                        return mutate(list.get(index));
                    }

                    public int size() {
                        return list.size();
                    }

                    public Iterator<RolapMember> iterator() {
                        final Iterator<RolapMember> it = list.iterator();
                        return new Iterator<RolapMember>() {
                            public boolean hasNext() {
                                return it.hasNext();
                            }
                            public RolapMember next() {
                                return mutate(it.next());
                            }

                            public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    private RolapMember mutate(final RolapMember member) {
                        RolapCubeMember parent = null;
                        if (member.getParentMember() != null) {
                            parent =
                                createAncestorMembers(
                                    NoCacheRolapCubeHierarchyMemberReader.this,
                                    (RolapCubeLevel) level.getParentLevel(),
                                    member.getParentMember());
                        }
                        return lookupCubeMember(
                            parent, member, (RolapCubeLevel) level);
                    }
                };
        }

        public RolapCubeMember lookupCubeMember(
            RolapCubeMember parent,
            RolapMember member,
            RolapCubeLevel level)
        {
            if (member.getKey() == RolapUtil.sqlNullValue) {
                if (member.isAll()) {
                    return getAllMember();
                }
            }

            return new RolapCubeMember(parent, member, level);
        }

        public int getMemberCount() {
            return rolapHierarchy.getMemberReader().getMemberCount();
        }
    }

    public static class RolapCubeSqlMemberSource extends SqlMemberSource {

        private final RolapCubeHierarchyMemberReader memberReader;
        private final MemberCacheHelper memberSourceCacheHelper;
        private final Object memberCacheLock;

        public RolapCubeSqlMemberSource(
            RolapCubeHierarchyMemberReader memberReader,
            RolapCubeHierarchy hierarchy,
            MemberCacheHelper memberSourceCacheHelper,
            Object memberCacheLock)
        {
            super(hierarchy);
            this.memberReader = memberReader;
            this.memberSourceCacheHelper = memberSourceCacheHelper;
            this.memberCacheLock = memberCacheLock;
        }

        public RolapMember makeMember(
            RolapMember parentMember,
            RolapLevel childLevel,
            Object value,
            Object captionValue,
            boolean parentChild,
            SqlStatement stmt,
            Object key,
            int columnOffset)
            throws SQLException
        {
            final RolapCubeMember parentCubeMember =
                (RolapCubeMember) parentMember;
            final RolapCubeLevel childCubeLevel = (RolapCubeLevel) childLevel;
            final RolapMember parent;
            if (parentMember != null) {
                parent = parentCubeMember.getRolapMember();
            } else {
                parent = null;
            }
            RolapMember member =
                super.makeMember(
                    parent,
                    childCubeLevel.getRolapLevel(),
                    value, captionValue, parentChild, stmt, key,
                    columnOffset);
            return
                memberReader.lookupCubeMember(
                    parentCubeMember,
                    member, childCubeLevel);
        }

        public MemberCache getMemberCache() {
            // this is a special cache used solely for rolapcubemembers
            return memberSourceCacheHelper;
        }

        /**
         * use the same lock in the RolapCubeMemberSource as the
         * RolapCubeHiearchyMemberReader to avoid deadlocks
         */
        public Object getMemberCacheLock() {
            return memberCacheLock;
        }

        public RolapMember allMember() {
            return getHierarchy().getAllMember();
        }
    }
}

// End RolapCubeHierarchy.java
