/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Hierarchy that is associated with a specific Cube.
 * 
 * @author Will Gorman (wgorman@pentaho.org)
 * @version $Id$
 */
public class RolapCubeHierarchy extends RolapHierarchy {
    
    private final RolapCubeDimension parentDimension;
    private final RolapHierarchy rolapHierarchy;
    private final RolapCubeLevel currentNullLevel;
    private RolapNullMember currentNullMember;
    private RolapCubeMember currentAllMember;
    private final MondrianDef.RelationOrJoin currentRelation;
    private final CubeMemberReader reader;
    private HierarchyUsage usage;
    private final Map<String, String> aliases = new HashMap<String, String>();
    private RolapCubeMember currentDefaultMember;
    
    /**
     * True if the hierarchy is degenerate - has no dimension table of its own,
     * just drives from the cube's fact table.
     */
    protected final boolean usingCubeFact;
    
    public RolapCubeHierarchy(
        RolapCubeDimension dimension,
            MondrianDef.CubeDimension cubeDim,
            RolapHierarchy rolapHierarchy,
            String subName)
    {
        super(dimension, subName, rolapHierarchy.hasAll());
        
        if (!dimension.getCube().isVirtual()) {
            this.usage = 
                new HierarchyUsage(dimension.getCube(), rolapHierarchy, cubeDim);
        }
        
        this.rolapHierarchy = rolapHierarchy;
        this.parentDimension = dimension;
        this.xmlHierarchy = rolapHierarchy.getXmlHierarchy();
        // this relation should equal the name of the new dimension table
        // The null member belongs to a level with very similar properties to
        // the 'all' level.
        this.currentNullLevel = new RolapCubeLevel(nullLevel, this);

        usingCubeFact = 
            (dimension.getCube().getFact() == null 
              || dimension.getCube().getFact().equals(
                    rolapHierarchy.getRelation()));
        
        // re-alias names if necessary
        if (!usingCubeFact) {
            // join expressions are columns only
            assert(usage.getJoinExp() instanceof MondrianDef.Column);
            currentRelation = 
                parentDimension.getCube().getStar().getUniqueRelation(
                    rolapHierarchy.getRelation(), 
                    usage.getForeignKey(), 
                    ((MondrianDef.Column)usage.getJoinExp()).getColumnName(),
                    usage.getJoinTable().getAlias());
        } else {
            currentRelation = rolapHierarchy.getRelation();
        }
        extractNewAliases(rolapHierarchy.getRelation(), currentRelation);
        this.relation = currentRelation;
        this.levels = new RolapCubeLevel[rolapHierarchy.getLevels().length];
        for (int i = 0; i < rolapHierarchy.getLevels().length; i++) {
            this.levels[i] = 
                new RolapCubeLevel(
                        (RolapLevel)rolapHierarchy.getLevels()[i], this);
            if (i == 0) {
                if (rolapHierarchy.getAllMember() != null) {
                    RolapCubeLevel allLevel;
                    if (hasAll()) {
                        allLevel = (RolapCubeLevel)this.levels[0];
                    } else {
                        // create an all level if one doesn't normally
                        // exist in the hierarchy
                        allLevel = 
                            new RolapCubeLevel(
                                rolapHierarchy.getAllMember().getLevel(),
                                this);
                        allLevel.init(dimension.xmlDimension);
                    }
                    
                    this.currentAllMember = 
                        new RolapCubeMember(
                            null,
                            rolapHierarchy.getAllMember(),
                            allLevel,
                            dimension.getCube());
                }
            }
        }

        reader = new RolapCubeHierarchyMemberReader();
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

    // override with stricter return type
    public RolapCubeDimension getDimension() {
        return (RolapCubeDimension) super.getDimension();
    }

    public RolapHierarchy getRolapHierarchy() {
        return rolapHierarchy;
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
                     && newrel instanceof MondrianDef.Join) {
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
        if (!parentDimension.equals(that.parentDimension)) {
            return false;
        } else {
            return getUniqueName().equals(that.getUniqueName());
        }
    }
    
    public int hashCode() {
        return super.hashCode() ^ (getUniqueName() == null
                ? 0
                : getUniqueName().hashCode());
    }
    
    public Member createMember(
        Member parent,
        Level level,
        String name,
        Formula formula)
    {
        RolapLevel rolapLevel = ((RolapCubeLevel)level).getRolapLevel();
        if (formula == null) {
            RolapMember member = new RolapMember(
                (RolapMember) parent, rolapLevel, name);
            return new RolapCubeMember((RolapCubeMember)parent, member, 
                    (RolapCubeLevel)level, parentDimension.getCube());
        } else if (level.getDimension().isMeasures()) {
            RolapCalculatedMeasure member = new RolapCalculatedMeasure(
                (RolapMember) parent, rolapLevel, name, formula);
            return new RolapCubeMember((RolapCubeMember)parent, member, 
                    (RolapCubeLevel)level, parentDimension.getCube());
        } else {
            RolapCalculatedMember member = new RolapCalculatedMember(
                (RolapMember) parent, rolapLevel, name, formula);
            return new RolapCubeMember((RolapCubeMember)parent, member, 
                    (RolapCubeLevel)level, parentDimension.getCube());
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
    
    public Member getDefaultMember() {
        if (currentDefaultMember == null) {
            reader.getRootMembers();
            RolapCubeLevel level = 
                (RolapCubeLevel)
                    levels[rolapHierarchy.getDefaultMember().getDepth()];
            RolapMember rolapDefaultMember =
                (RolapMember) rolapHierarchy.getDefaultMember();

            currentDefaultMember = reader.lookupCubeMember(
                    hasAll() ? currentAllMember : null, 
                    rolapDefaultMember, level);
        }
        return currentDefaultMember;
    }

    public Member getNullMember() {
        // use lazy initialization to get around bootstrap issues
        if (currentNullMember == null) {
            currentNullMember = new RolapNullMember(currentNullLevel);
        }
        return currentNullMember;
    }
    
    /**
     * Returns the 'all' member.
     */
    public RolapCubeMember getAllMember() {
        return currentAllMember;
    }
    
    /**
     * Returns the display name of this catalog element.
     * If no caption is defined, the name is returned.
     */
    public String getCaption() {
        return rolapHierarchy.getCaption();
    }

    /**
     * Sets the display name of this catalog element.
     */
    public void setCaption(String caption) {
        rolapHierarchy.setCaption(caption);
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
                (RolapMember)rolapHierarchy.getDefaultMember(),
                level,
                parentDimension.getCube());
    }

    void init(MondrianDef.CubeDimension xmlDimension) {
        // first init shared hierarchy
        rolapHierarchy.init(xmlDimension);
        // second init cube hierarchy
        super.init(xmlDimension);
    }

    interface CubeMemberReader extends MemberReader {
        RolapCubeMember lookupCubeMember(
            RolapCubeMember parent,
            RolapMember member,
            RolapCubeLevel level);
    }

    /**
     * Implementation of {@link mondrian.rolap.MemberReader} that creates
     * wrapper {@link mondrian.rolap.RolapCubeMember} objects as it goes.
     * Underlying it is the existing member reader.
     *
     * <h3>Synchronization</h3>
     *
     * <p>Most synchronization takes place within SmartMemberReader. All
     * synchronization is done on the cacheHelper object.</p>
     *
     * <h3>Caching strategy</h3>
     *
     * <p>We take the following approach to caching RolapCubeMember objects:
     *
     * <p><b>RolapHierarchy.SmartMemberReader.SmartCacheHelper</b>
     * is the shared cache
     * across shared hierarchies.  This member cache only contains members
     * loaded by non-cube specific member lookups.  This cache should only
     * contain RolapMembers, not RolapCubeMembers.
     *
     * <p><b>RolapCubeHierarchy.RolapCubeHierarchyMemberReader.rolapCubeCacheHelper</b>
     * is a cache that contains the RolapCubeMember objects, which are
     * cube-specific wrappers of shared members.</p>
     *
     * <p><b>RolapCubeHierarchy.RolapCubeHierarchyMemberReader.SmartCacheHelper</b>
     * is the inherited shared cache from SmartMemberReader, and is used when
     * a join with the fact table is necessary,
     * SqlContextConstraint.isJoinRequired().
     * This cache may be redundant with rolapCubeCacheHelper.</p>
     *
     * <p>A special note regarding RolapCubeHierarchyMemberReader.cubeSource.
     * This class was required for the special situation getMemberBuilder()
     * method call from RolapNativeSet.  This class utilizes both the
     * rolapCubeCacheHelper class for storing RolapCubeMembers, and also the
     * RolapCubeHierarchyMemberReader's inherited SmartCacheHelper.</p>
     *
     * <p>The {@link mondrian.olap.MondrianProperties#EnableRolapCubeMemberCache}
     * property allows you to disable the RolapCubeMember cache.</p>
     */
    public class RolapCubeHierarchyMemberReader
        extends SmartMemberReader
        implements CubeMemberReader
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

        public RolapCubeHierarchyMemberReader() {
            super(new SqlMemberSource(RolapCubeHierarchy.this));
            rolapCubeCacheHelper = 
                new MemberCacheHelper(RolapCubeHierarchy.this);

            cubeSource = 
                new RolapCubeSqlMemberSource(
                    this,
                    RolapCubeHierarchy.this,
                    rolapCubeCacheHelper);
            
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
                rootMembers = 
                    getMembersInLevel(
                        (RolapCubeLevel) getLevels()[0],
                        0,
                        Integer.MAX_VALUE);
            }
            return rootMembers;
        }
        
        private String getUniqueNameForMemberWithoutHierarchy(
            RolapMember member)
        {
            String name = 
                (String) member.getPropertyValue(
                        Property.UNIQUE_NAME_WITHOUT_HIERARCHY.getName());
            RolapMember parent = member;
            if (name == null) {
                StringBuilder fullName = new StringBuilder();
                while (parent != null) {
                    fullName.append("[").append(parent.getName()).append("]");
                    parent = parent.getParentMember();
                }
                name = fullName.toString();
                member.setProperty(
                        Property.UNIQUE_NAME_WITHOUT_HIERARCHY.getName(),
                        name);
            }
            return name;
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
            final List<RolapCubeMember> parentMemberList =
                Util.cast(parentMembers);
            for (RolapCubeMember member : parentMemberList) {
                lookup.put(
                    getUniqueNameForMemberWithoutHierarchy(
                        member.getRolapMember()), member);
                rolapParents.add(member.getRolapMember());
            }

            // get member children from shared member reader if possible, 
            // if not get them from our own source
            boolean joinReq = 
                (constraint instanceof SqlContextConstraint) 
                && (((SqlContextConstraint)constraint).isJoinRequired() || 
                    ((SqlContextConstraint)constraint).getEvaluator().isNonEmpty());
            if (joinReq) {
                super.readMemberChildren(parentMembers, rolapChildren, constraint);
            } else {
                rolapHierarchy.getMemberReader().getMemberChildren(
                    rolapParents, rolapChildren, constraint);
            }
            
            // now lookup or create RolapCubeMember
            for (RolapMember currMember : rolapChildren) {
                RolapCubeMember parent =
                    lookup.get(
                        getUniqueNameForMemberWithoutHierarchy(
                            currMember.getParentMember()));
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
                for (Map.Entry<RolapMember, List<RolapMember>> entry :
                    tempMap.entrySet())
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
        
        public void getMemberChildren(
                List<RolapMember> parentMembers,
                List<RolapMember> children,
                MemberChildrenConstraint constraint) {

            synchronized(cacheHelper) {
                checkCacheStatus();
        
                List<RolapMember> missed = new ArrayList<RolapMember>();
                for (RolapMember parentMember : parentMembers) {
                    List<RolapMember> list = 
                        rolapCubeCacheHelper.getChildrenFromCache(parentMember, constraint);
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
        }
        
        
        public List<RolapMember> getMembersInLevel(
            RolapLevel level,
            int startOrdinal,
            int endOrdinal,
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
                    (constraint instanceof SqlContextConstraint)
                        && (((SqlContextConstraint)constraint).isJoinRequired() || 
                        ((SqlContextConstraint)constraint).getEvaluator().isNonEmpty());
                        
                List<RolapMember> list;
                if (!joinReq) {
                    list = 
                        rolapHierarchy.getMemberReader().getMembersInLevel(
                            ((RolapCubeLevel) level).getRolapLevel(),
                            startOrdinal, endOrdinal, constraint);
                } else {
                    list =
                        super.getMembersInLevel(
                            level, startOrdinal, endOrdinal, constraint);
                }
                List<RolapMember> newlist =
                    new ArrayList<RolapMember>(list.size());
                for (RolapMember member : list) {
                    // note that there is a special case for the all member
    
                    // REVIEW: disabled, to see what happens. if this code is for
                    // performance, we should check level.isAll at the top of the
                    // method; if it is for correctness, leave the code in
                    if (false && member == rolapHierarchy.getAllMember()) {
                        newlist.add(getAllMember());
                    } else {
                        // i'm not quite sure about this one yet
                        RolapCubeMember parent = null;
                        if (member.getParentMember() != null) {
                            parent =
                                createAncestorMembers(
                                    (RolapCubeLevel) level.getParentLevel(),
                                    member.getParentMember());
                        }
                        RolapCubeMember newmember =
                            lookupCubeMember(
                                parent, member, (RolapCubeLevel) level);
                        newlist.add(newmember);
                    }
                }
                if (enableCache) {
                    rolapCubeCacheHelper.putLevelMembersInCache(
                        level, constraint, newlist);
                }
                return newlist;
            }
        }
        
        private RolapCubeMember createAncestorMembers(
            RolapCubeLevel level,
            RolapMember member)
        {
            RolapCubeMember parent = null;
            if (member.getParentMember() != null) {
                parent =
                    createAncestorMembers(
                        level.getParentLevel(), member.getParentMember());
            }
            return lookupCubeMember(parent, member, level);
        }

        public RolapCubeMember lookupCubeMember(
            RolapCubeMember parent,
            RolapMember member,
            RolapCubeLevel level)
        {
            synchronized (cacheHelper) {
                if (member.getKey() == null) {
                    if (member.isAll()) {
                        return getAllMember();
                    }
                    
                    throw new NullPointerException();
                }

                RolapCubeMember cubeMember;
                if (enableCache) {
                    Object key =
                        rolapCubeCacheHelper.makeKey(parent, member.getKey());
                    cubeMember = (RolapCubeMember)
                        rolapCubeCacheHelper.getMember(key, false);
                    if (cubeMember == null) {
                        cubeMember =
                            new RolapCubeMember(
                                parent, member, level, parentDimension.getCube());
                        rolapCubeCacheHelper.putMember(key, cubeMember);
                    }
                } else {
                    cubeMember =
                        new RolapCubeMember(
                            parent, member, level, parentDimension.getCube());
                }
                return cubeMember;
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
                    if (cacheHelper.getChangeListener()
                            .isHierarchyChanged(getHierarchy())) {
                        cacheHelper.flushCache();
                        rolapCubeCacheHelper.flushCache();

                        if (rolapHierarchy.getMemberReader() 
                                instanceof SmartMemberReader) {
                            SmartMemberReader smartMemberReader = 
                                (SmartMemberReader)
                                    rolapHierarchy.getMemberReader();
                            if (smartMemberReader.getMemberCache() 
                                    instanceof MemberCacheHelper) {
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

    public static class RolapCubeSqlMemberSource extends SqlMemberSource {

        private final RolapCubeHierarchyMemberReader memberReader;
        private final MemberCacheHelper memberSourceCacheHelper;

        public RolapCubeSqlMemberSource(
            RolapCubeHierarchyMemberReader memberReader,
            RolapCubeHierarchy hierarchy,
            MemberCacheHelper memberSourceCacheHelper)
        {
            super(hierarchy);
            this.memberReader = memberReader;
            this.memberSourceCacheHelper = memberSourceCacheHelper;
        }

        public RolapMember makeMember(
                RolapMember parentMember,
                RolapLevel childLevel,
                Object value,
                Object captionValue,
                boolean parentChild,
                ResultSet resultSet,
                Object key,
                int columnOffset)
                throws SQLException {

            RolapMember member =
                super.makeMember(
                    parentMember,
                    ((RolapCubeLevel)childLevel).getRolapLevel(),
                    value, captionValue, parentChild, resultSet, key,
                    columnOffset);
            return
                memberReader.lookupCubeMember(
                    (RolapCubeMember)parentMember,
                    member,
                    (RolapCubeLevel)childLevel);
        }

        public MemberCache getMemberCache() {
            // this is a special cache used solely for rolapcubemembers
            return memberSourceCacheHelper;
        }
    }
}

// End RolapCubeHierarchy.java

