/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.DimensionType;
import mondrian.olap.LevelType;
import mondrian.olap.Role.HierarchyAccess;
import mondrian.olap.fun.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RestrictedMemberReader.MultiCardinalityDefaultMember;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.CellFormatter;
import mondrian.spi.impl.Scripts;
import mondrian.util.UnionIterator;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>RolapHierarchy</code> implements {@link Hierarchy} for a ROLAP database.
 *
 * <p>The ordinal of a hierarchy <em>within a particular cube</em> is found by
 * calling {@link #getOrdinalInCube()}. Ordinals are contiguous and zero-based.
 * Zero is always the <code>[Measures]</code> dimension.
 *
 * <p>NOTE: It is only valid to call that method on the measures hierarchy, and
 * on members of the {@link RolapCubeHierarchy} subclass. When the measures
 * hierarchy is of that class, we will move the method down.)
 *
 * @author jhyde
 * @since 10 August, 2001
  */
public class RolapHierarchy extends HierarchyBase {

    private static final Logger LOGGER = Logger.getLogger(RolapHierarchy.class);

    /**
     * The raw member reader. For a member reader which incorporates access
     * control and deals with hidden members (if the hierarchy is ragged), use
     * {@link #createMemberReader(Role)}.
     */
    private MemberReader memberReader;
    protected MondrianDef.Hierarchy xmlHierarchy;
    private String memberReaderClass;
    protected MondrianDef.RelationOrJoin relation;
    private Member defaultMember;
    private String defaultMemberName;
    private RolapNullMember nullMember;

    private String sharedHierarchyName;
    private String uniqueKeyLevelName;

    private Exp aggregateChildrenExpression;

    /**
     * The level that the null member belongs too.
     */
    protected final RolapLevel nullLevel;

    /**
     * The 'all' member of this hierarchy. This exists even if the hierarchy
     * does not officially have an 'all' member.
     */
    private RolapMemberBase allMember;
    private static final String ALL_LEVEL_CARDINALITY = "1";
    private final Map<String, Annotation> annotationMap;
    final RolapHierarchy closureFor;

    /**
     * Creates a hierarchy.
     *
     * @param dimension Dimension
     * @param subName Name of this hierarchy
     * @param hasAll Whether hierarchy has an 'all' member
     * @param closureFor Hierarchy for which the new hierarchy is a closure;
     *     null for regular hierarchies
     */
    RolapHierarchy(
        RolapDimension dimension,
        String subName,
        String caption,
        boolean visible,
        String description,
        boolean hasAll,
        RolapHierarchy closureFor,
        Map<String, Annotation> annotationMap)
    {
        super(dimension, subName, caption, visible, description, hasAll);
        this.annotationMap = annotationMap;
        this.allLevelName = "(All)";
        this.allMemberName =
            subName != null
            && (MondrianProperties.instance().SsasCompatibleNaming.get()
                || name.equals(subName + "." + subName))
                ? "All " + subName + "s"
                : "All " + name + "s";
        this.closureFor = closureFor;
        if (hasAll) {
            this.levels = new RolapLevel[1];
            this.levels[0] =
                new RolapLevel(
                    this,
                    this.allLevelName,
                    null,
                    true,
                    null,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    RolapProperty.emptyArray,
                    RolapLevel.FLAG_ALL | RolapLevel.FLAG_UNIQUE,
                    null,
                    null,
                    RolapLevel.HideMemberCondition.Never,
                    LevelType.Regular,
                    "",
                    Collections.<String, Annotation>emptyMap());
        } else {
            this.levels = new RolapLevel[0];
        }

        // The null member belongs to a level with very similar properties to
        // the 'all' level.
        this.nullLevel =
            new RolapLevel(
                this,
                this.allLevelName,
                null,
                true,
                null,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                RolapProperty.emptyArray,
                RolapLevel.FLAG_ALL | RolapLevel.FLAG_UNIQUE,
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                LevelType.Null,
                "",
                Collections.<String, Annotation>emptyMap());
    }

    /**
     * Creates a <code>RolapHierarchy</code>.
     *
     * @param dimension the dimension this hierarchy belongs to
     * @param xmlHierarchy the xml object defining this hierarchy
     * @param xmlCubeDimension the xml object defining the cube
     *   dimension for this object
     */
    RolapHierarchy(
        RolapDimension dimension,
        MondrianDef.Hierarchy xmlHierarchy,
        MondrianDef.CubeDimension xmlCubeDimension)
    {
        this(
            dimension,
            xmlHierarchy.name,
            xmlHierarchy.caption,
            xmlHierarchy.visible,
            xmlHierarchy.description,
            xmlHierarchy.hasAll,
            null,
            createAnnotationMap(xmlHierarchy.annotations));

        assert !(this instanceof RolapCubeHierarchy);

        this.xmlHierarchy = xmlHierarchy;
        this.relation = xmlHierarchy.relation;
        if (xmlHierarchy.relation instanceof MondrianDef.InlineTable) {
            this.relation =
                RolapUtil.convertInlineTableToRelation(
                    (MondrianDef.InlineTable) xmlHierarchy.relation,
                    getRolapSchema().getDialect());
        }
        this.memberReaderClass = xmlHierarchy.memberReaderClass;
        this.uniqueKeyLevelName = xmlHierarchy.uniqueKeyLevelName;

        // Create an 'all' level even if the hierarchy does not officially
        // have one.
        if (xmlHierarchy.allMemberName != null) {
            this.allMemberName = xmlHierarchy.allMemberName;
        }
        if (xmlHierarchy.allLevelName != null) {
            this.allLevelName = xmlHierarchy.allLevelName;
        }
        RolapLevel allLevel =
            new RolapLevel(
                this,
                this.allLevelName,
                null,
                true,
                null,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                RolapProperty.emptyArray,
                RolapLevel.FLAG_ALL | RolapLevel.FLAG_UNIQUE,
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                LevelType.Regular, ALL_LEVEL_CARDINALITY,
                Collections.<String, Annotation>emptyMap());
        allLevel.init(xmlCubeDimension);
        this.allMember = new RolapMemberBase(
            null, allLevel, RolapUtil.sqlNullValue,
            allMemberName, Member.MemberType.ALL);
        // assign "all member" caption
        if (xmlHierarchy.allMemberCaption != null
            && xmlHierarchy.allMemberCaption.length() > 0)
        {
            this.allMember.setCaption(xmlHierarchy.allMemberCaption);
        }
        this.allMember.setOrdinal(0);

        if (xmlHierarchy.levels.length == 0) {
            throw MondrianResource.instance().HierarchyHasNoLevels.ex(
                getUniqueName());
        }

        Set<String> levelNameSet = new HashSet<String>();
        for (MondrianDef.Level level : xmlHierarchy.levels) {
            if (!levelNameSet.add(level.name)) {
                throw MondrianResource.instance().HierarchyLevelNamesNotUnique
                    .ex(
                        getUniqueName(), level.name);
            }
        }

        // If the hierarchy has an 'all' member, the 'all' level is level 0.
        if (hasAll) {
            this.levels = new RolapLevel[xmlHierarchy.levels.length + 1];
            this.levels[0] = allLevel;
            for (int i = 0; i < xmlHierarchy.levels.length; i++) {
                final MondrianDef.Level xmlLevel = xmlHierarchy.levels[i];
                if (xmlLevel.getKeyExp() == null
                    && xmlHierarchy.memberReaderClass == null)
                {
                    throw MondrianResource.instance()
                        .LevelMustHaveNameExpression.ex(xmlLevel.name);
                }
                levels[i + 1] = new RolapLevel(this, i + 1, xmlLevel);
            }
        } else {
            this.levels = new RolapLevel[xmlHierarchy.levels.length];
            for (int i = 0; i < xmlHierarchy.levels.length; i++) {
                levels[i] = new RolapLevel(this, i, xmlHierarchy.levels[i]);
            }
        }

        if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            String sharedDimensionName =
                ((MondrianDef.DimensionUsage) xmlCubeDimension).source;
            this.sharedHierarchyName = sharedDimensionName;
            if (subName != null) {
                this.sharedHierarchyName += "." + subName; // e.g. "Time.Weekly"
            }
        } else {
            this.sharedHierarchyName = null;
        }
        if (xmlHierarchy.relation != null
            && xmlHierarchy.memberReaderClass != null)
        {
            throw MondrianResource.instance()
                .HierarchyMustNotHaveMoreThanOneSource.ex(getUniqueName());
        }
        if (!Util.isEmpty(xmlHierarchy.caption)) {
            setCaption(xmlHierarchy.caption);
        }
        defaultMemberName = xmlHierarchy.defaultMember;
    }

    public static Map<String, Annotation> createAnnotationMap(
        MondrianDef.Annotations annotations)
    {
        if (annotations == null
            || annotations.array == null
            || annotations.array.length == 0)
        {
            return Collections.emptyMap();
        }
        // Use linked hash map because it retains order.
        final Map<String, Annotation> map =
            new LinkedHashMap<String, Annotation>();
        for (MondrianDef.Annotation annotation : annotations.array) {
            final String name = annotation.name;
            final String value = annotation.cdata;
            map.put(
                annotation.name,
                new Annotation() {
                    public String getName() {
                        return name;
                    }

                    public Object getValue() {
                        return value;
                    }
                });
        }
        return map;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapHierarchy)) {
            return false;
        }

        RolapHierarchy that = (RolapHierarchy)o;
        if (sharedHierarchyName == null || that.sharedHierarchyName == null) {
            return false;
        } else {
            return sharedHierarchyName.equals(that.sharedHierarchyName)
                && getUniqueName().equals(that.getUniqueName());
        }
    }

    protected int computeHashCode() {
        return super.computeHashCode()
            ^ (sharedHierarchyName == null
                ? 0
                : sharedHierarchyName.hashCode());
    }

    /**
     * Initializes a hierarchy within the context of a cube.
     */
    void init(MondrianDef.CubeDimension xmlDimension) {
        // first create memberReader
        if (this.memberReader == null) {
            this.memberReader = getRolapSchema().createMemberReader(
                sharedHierarchyName, this, memberReaderClass);
        }
        for (Level level : levels) {
            ((RolapLevel) level).init(xmlDimension);
        }
        if (defaultMemberName != null) {
            List<Id.Segment> uniqueNameParts;
            if (defaultMemberName.contains("[")) {
                uniqueNameParts = Util.parseIdentifier(defaultMemberName);
            } else {
                uniqueNameParts =
                    Collections.<Id.Segment>singletonList(
                        new Id.NameSegment(
                            defaultMemberName,
                            Id.Quoting.UNQUOTED));
            }

            // First look up from within this hierarchy. Works for unqualified
            // names, e.g. [USA].[CA].
            defaultMember = (Member) Util.lookupCompound(
                getRolapSchema().getSchemaReader(),
                this,
                uniqueNameParts,
                false,
                Category.Member,
                MatchType.EXACT);

            // Next look up within global context. Works for qualified names,
            // e.g. [Store].[USA].[CA] or [Time].[Weekly].[1997].[Q2].
            if (defaultMember == null) {
                defaultMember = (Member) Util.lookupCompound(
                    getRolapSchema().getSchemaReader(),
                    new DummyElement(),
                    uniqueNameParts,
                    false,
                    Category.Member,
                    MatchType.EXACT);
            }
            if (defaultMember == null) {
                throw Util.newInternal(
                    "Can not find Default Member with name \""
                    + defaultMemberName + "\" in Hierarchy \""
                    + getName() + "\"");
            }
        }
    }

    void setMemberReader(MemberReader memberReader) {
        this.memberReader = memberReader;
    }

    MemberReader getMemberReader() {
        return memberReader;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    RolapLevel newMeasuresLevel() {
        RolapLevel level =
            new RolapLevel(
                this,
                "MeasuresLevel",
                null,
                true,
                null,
                this.levels.length,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                RolapProperty.emptyArray,
                0,
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                LevelType.Regular,
                "",
                Collections.<String, Annotation>emptyMap());
        this.levels = Util.append(this.levels, level);
        return level;
    }

    /**
     * If this hierarchy has precisely one table, returns that table;
     * if this hierarchy has no table, return the cube's fact-table;
     * otherwise, returns null.
     */
    MondrianDef.Relation getUniqueTable() {
        if (relation instanceof MondrianDef.Relation) {
            return (MondrianDef.Relation) relation;
        } else if (relation instanceof MondrianDef.Join) {
            return null;
        } else {
            throw Util.newInternal(
                "hierarchy's relation is a " + relation.getClass());
        }
    }

    boolean tableExists(String tableName) {
        return (relation != null) && getTable(tableName, relation) != null;
    }

    MondrianDef.Relation getTable(String tableName) {
        return relation == null ? null : getTable(tableName, relation);
    }

    private static MondrianDef.Relation getTable(
        String tableName,
        MondrianDef.RelationOrJoin relationOrJoin)
    {
        if (relationOrJoin instanceof MondrianDef.Relation) {
            MondrianDef.Relation relation =
                (MondrianDef.Relation) relationOrJoin;
            if (relation.getAlias().equals(tableName)) {
                return relation;
            } else {
                return null;
            }
        } else {
            MondrianDef.Join join = (MondrianDef.Join) relationOrJoin;
            MondrianDef.Relation rel = getTable(tableName, join.left);
            if (rel != null) {
                return rel;
            }
            return getTable(tableName, join.right);
        }
    }

    public RolapSchema getRolapSchema() {
        return (RolapSchema) dimension.getSchema();
    }

    public MondrianDef.RelationOrJoin getRelation() {
        return relation;
    }

    public MondrianDef.Hierarchy getXmlHierarchy() {
        return xmlHierarchy;
    }

    public Member getDefaultMember() {
        // use lazy initialization to get around bootstrap issues
        if (defaultMember == null) {
            List<RolapMember> rootMembers = memberReader.getRootMembers();
            final SchemaReader schemaReader =
                getRolapSchema().getSchemaReader();
            List<RolapMember> calcMemberList =
                Util.cast(schemaReader.getCalculatedMembers(getLevels()[0]));
            for (RolapMember rootMember
                : UnionIterator.over(rootMembers, calcMemberList))
            {
                if (rootMember.isHidden()) {
                    continue;
                }
                // Note: We require that the root member is not a hidden member
                // of a ragged hierarchy, but we do not require that it is
                // visible. In particular, if a cube contains no explicit
                // measures, the default measure will be the implicitly defined
                // [Fact Count] measure, which happens to be non-visible.
                defaultMember = rootMember;
                break;
            }
            if (defaultMember == null) {
                throw MondrianResource.instance().InvalidHierarchyCondition.ex(
                    this.getUniqueName());
            }
        }
        return defaultMember;
    }

    public Member getNullMember() {
        // use lazy initialization to get around bootstrap issues
        if (nullMember == null) {
            nullMember = new RolapNullMember(nullLevel);
        }
        return nullMember;
    }

    /**
     * Returns the 'all' member.
     */
    public RolapMember getAllMember() {
        return allMember;
    }

    public Member createMember(
        Member parent,
        Level level,
        String name,
        Formula formula)
    {
        if (formula == null) {
            return new RolapMemberBase(
                (RolapMember) parent, (RolapLevel) level, name);
        } else if (level.getDimension().isMeasures()) {
            return new RolapCalculatedMeasure(
                (RolapMember) parent, (RolapLevel) level, name, formula);
        } else {
            return new RolapCalculatedMember(
                (RolapMember) parent, (RolapLevel) level, name, formula);
        }
    }

    String getAlias() {
        return getName();
    }

    /**
     * Returns the name of the source hierarchy, if this hierarchy is shared,
     * otherwise null.
     *
     * <p>If this hierarchy is a public -- that is, it belongs to a dimension
     * which is a usage of a shared dimension -- then
     * <code>sharedHierarchyName</code> holds the unique name of the shared
     * hierarchy; otherwise it is null.
     *
     * <p> Suppose this hierarchy is "Weekly" in the dimension "Order Date" of
     * cube "Sales", and that "Order Date" is a usage of the "Time"
     * dimension. Then <code>sharedHierarchyName</code> will be
     * "[Time].[Weekly]".
     */
    public String getSharedHierarchyName() {
        return sharedHierarchyName;
    }

    /**
     * Adds to the FROM clause of the query the tables necessary to access the
     * members of this hierarchy in an inverse join order, used with agg tables.
     * If <code>expression</code> is not null, adds the tables necessary to
     * compute that expression.
     *
     * <p> This method is idempotent: if you call it more than once, it only
     * adds the table(s) to the FROM clause once.
     *
     * @param query Query to add the hierarchy to
     * @param expression Level to qualify up to; if null, qualifies up to the
     *    topmost ('all') expression, which may require more columns and more
     *    joins
     */
    void addToFromInverse(SqlQuery query, MondrianDef.Expression expression) {
        if (relation == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        final boolean failIfExists = false;
        MondrianDef.RelationOrJoin subRelation = relation;
        if (relation instanceof MondrianDef.Join) {
            if (expression != null) {
                subRelation =
                    relationSubsetInverse(relation, expression.getTableAlias());
            }
        }
        query.addFrom(subRelation, null, failIfExists);
    }

    /**
     * Adds to the FROM clause of the query the tables necessary to access the
     * members of this hierarchy. If <code>expression</code> is not null, adds
     * the tables necessary to compute that expression.
     *
     * <p> This method is idempotent: if you call it more than once, it only
     * adds the table(s) to the FROM clause once.
     *
     * @param query Query to add the hierarchy to
     * @param expression Level to qualify up to; if null, qualifies up to the
     *    topmost ('all') expression, which may require more columns and more
     *    joins
     */
    void addToFrom(SqlQuery query, MondrianDef.Expression expression) {
        if (relation == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        query.registerRootRelation(relation);
        final boolean failIfExists = false;
        MondrianDef.RelationOrJoin subRelation = relation;
        if (relation instanceof MondrianDef.Join) {
            if (expression != null) {
                // Suppose relation is
                //   (((A join B) join C) join D)
                // and the fact table is
                //   F
                // and our expression uses C. We want to make the expression
                //   F left join ((A join B) join C).
                // Search for the smallest subset of the relation which
                // uses C.
                subRelation =
                    relationSubset(relation, expression.getTableAlias());
            }
        }
        query.addFrom(
            subRelation,
            expression == null ? null : expression.getTableAlias(),
            failIfExists);
    }

    /**
     * Adds a table to the FROM clause of the query.
     * If <code>table</code> is not null, adds the table. Otherwise, add the
     * relation on which this hierarchy is based on.
     *
     * <p> This method is idempotent: if you call it more than once, it only
     * adds the table(s) to the FROM clause once.
     *
     * @param query Query to add the hierarchy to
     * @param table table to add to the query
     */
    void addToFrom(SqlQuery query, RolapStar.Table table) {
        if (getRelation() == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        final boolean failIfExists = false;
        MondrianDef.RelationOrJoin subRelation = null;
        if (table != null) {
            // Suppose relation is
            //   (((A join B) join C) join D)
            // and the fact table is
            //   F
            // and the table to add is C. We want to make the expression
            //   F left join ((A join B) join C).
            // Search for the smallest subset of the relation which
            // joins with C.
            subRelation = lookupRelationSubset(getRelation(), table);
        }

        if (subRelation == null) {
            // If no table is found or specified, add the entire base relation.
            subRelation = getRelation();
        }

        boolean tableAdded =
            query.addFrom(
                subRelation,
                table != null ? table.getAlias() : null,
                failIfExists);
        if (tableAdded && table != null) {
            RolapStar.Condition joinCondition = table.getJoinCondition();
            if (joinCondition != null) {
                query.addWhere(joinCondition);
            }
        }
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the relation <code>alias</code>, or null if these is no relation with
     * such an alias, in inverse join order, used for agg tables.
     *
     * @param relation the relation in which to look for table by its alias
     * @param alias table alias to search for
     * @return the smallest containing relation or null if no matching table
     * is found in <code>relation</code>
     */
    private static MondrianDef.RelationOrJoin relationSubsetInverse(
        MondrianDef.RelationOrJoin relation,
        String alias)
    {
        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation table =
                (MondrianDef.Relation) relation;
            return table.getAlias().equals(alias)
                ? relation
                : null;

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.RelationOrJoin leftRelation =
                relationSubsetInverse(join.left, alias);
            return (leftRelation == null)
                ? relationSubsetInverse(join.right, alias)
                : join;

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the relation <code>alias</code>, or null if these is no relation with
     * such an alias.
     * @param relation the relation in which to look for table by its alias
     * @param alias table alias to search for
     * @return the smallest containing relation or null if no matching table
     * is found in <code>relation</code>
     */
    private static MondrianDef.RelationOrJoin relationSubset(
        MondrianDef.RelationOrJoin relation,
        String alias)
    {
        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation table =
                (MondrianDef.Relation) relation;
            return table.getAlias().equals(alias)
                ? relation
                : null;

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.RelationOrJoin rightRelation =
                relationSubset(join.right, alias);
            return (rightRelation == null)
                ? relationSubset(join.left, alias)
                : MondrianProperties.instance()
                    .FilterChildlessSnowflakeMembers.get()
                ? join
                : rightRelation;
        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the table <code>targetTable</code>, or null if the targetTable is not
     * one of the joining table in <code>relation</code>.
     *
     * @param relation the relation in which to look for targetTable
     * @param targetTable table to add to the query
     * @return the smallest containing relation or null if no matching table
     * is found in <code>relation</code>
     */
    private static MondrianDef.RelationOrJoin lookupRelationSubset(
        MondrianDef.RelationOrJoin relation,
        RolapStar.Table targetTable)
    {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            if (table.name.equals(targetTable.getTableName())) {
                return relation;
            } else {
                // Not the same table if table names are different
                return null;
            }
        } else if (relation instanceof MondrianDef.Join) {
            // Search inside relation, starting from the rightmost table,
            // and move left along the join chain.
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.RelationOrJoin rightRelation =
                lookupRelationSubset(join.right, targetTable);
            if (rightRelation == null) {
                // Keep searching left.
                return lookupRelationSubset(
                    join.left, targetTable);
            } else {
                // Found a match.
                return join;
            }
        }
        return null;
    }

    /**
     * Creates a member reader which enforces the access-control profile of
     * <code>role</code>.
     *
     * <p>This method may not be efficient, so the caller should take care
     * not to call it too often. A cache is a good idea.
     *
     * @param role Role
     * @return Member reader that implements access control
     *
     * @pre role != null
     * @post return != null
     */
    MemberReader createMemberReader(Role role) {
        final Access access = role.getAccess(this);
        switch (access) {
        case NONE:
            role.getAccess(this); // todo: remove
            throw Util.newInternal(
                "Illegal access to members of hierarchy " + this);
        case ALL:
            return (isRagged())
                ? new SmartRestrictedMemberReader(getMemberReader(), role)
                : getMemberReader();

        case CUSTOM:
            final Role.HierarchyAccess hierarchyAccess =
                role.getAccessDetails(this);
            final Role.RollupPolicy rollupPolicy =
                hierarchyAccess.getRollupPolicy();
            final NumericType returnType = new NumericType();
            switch (rollupPolicy) {
            case FULL:
                return new SmartRestrictedMemberReader(
                    getMemberReader(), role);
            case PARTIAL:
                Type memberType1 =
                    new mondrian.olap.type.MemberType(
                        getDimension(),
                        this,
                        null,
                        null);
                SetType setType = new SetType(memberType1);
                ListCalc listCalc =
                    new AbstractListCalc(
                        new DummyExp(setType), new Calc[0])
                    {
                        public TupleList evaluateList(
                            Evaluator evaluator)
                        {
                            return
                                new UnaryTupleList(
                                    getLowestMembersForAccess(
                                        evaluator, hierarchyAccess, null));
                        }

                        public boolean dependsOn(Hierarchy hierarchy) {
                            return true;
                        }
                    };
                final Calc partialCalc =
                    new LimitedRollupAggregateCalc(returnType, listCalc);

                final Exp partialExp =
                    new ResolvedFunCall(
                        new FunDefBase("$x", "x", "In") {
                            public Calc compileCall(
                                ResolvedFunCall call,
                                ExpCompiler compiler)
                            {
                                return partialCalc;
                            }

                            public void unparse(Exp[] args, PrintWriter pw) {
                                pw.print("$RollupAccessibleChildren()");
                            }
                        },
                        new Exp[0],
                        returnType);
                return new LimitedRollupSubstitutingMemberReader(
                    getMemberReader(), role, hierarchyAccess, partialExp);

            case HIDDEN:
                Exp hiddenExp =
                    new ResolvedFunCall(
                        new FunDefBase("$x", "x", "In") {
                            public Calc compileCall(
                                ResolvedFunCall call, ExpCompiler compiler)
                            {
                                return new ConstantCalc(returnType, null);
                            }

                            public void unparse(Exp[] args, PrintWriter pw) {
                                pw.print("$RollupAccessibleChildren()");
                            }
                        },
                        new Exp[0],
                        returnType);
                return new LimitedRollupSubstitutingMemberReader(
                    getMemberReader(), role, hierarchyAccess, hiddenExp);
            default:
                throw Util.unexpected(rollupPolicy);
            }
        default:
            throw Util.badValue(access);
        }
    }

    /**
     * Goes recursively down a hierarchy and builds a list of the
     * members that should be constrained on because of access controls.
     * It isn't sufficient to constrain on the current level in the
     * evaluator because the actual constraint could be even more limited
     * <p>Example. If we only give access to Seattle but the query is on
     * the country level, we have to constrain at the city level, not state,
     * or else all the values of all cities in the state will be returned.
     */
    List<Member> getLowestMembersForAccess(
        Evaluator evaluator,
        HierarchyAccess hAccess,
        Map<Member, Access> membersWithAccess)
    {
        if (membersWithAccess == null) {
            membersWithAccess =
                FunUtil.getNonEmptyMemberChildrenWithDetails(
                    evaluator,
                    ((RolapEvaluator) evaluator)
                        .getExpanding());
        }
        boolean goesLower = false;
        for (Member member : membersWithAccess.keySet()) {
            Access access = membersWithAccess.get(member);
            if (access == null) {
                access = hAccess.getAccess(member);
            }
            if (access != Access.ALL) {
                goesLower = true;
                break;
            }
        }
        if (goesLower) {
            // We still have to go one more level down.
            Map<Member, Access> newMap =
                new HashMap<Member, Access>();
            for (Member member : membersWithAccess.keySet()) {
                int savepoint = evaluator.savepoint();
                try {
                    evaluator.setContext(member);
                    newMap.putAll(
                        FunUtil.getNonEmptyMemberChildrenWithDetails(
                            evaluator,
                            member));
                } finally {
                    evaluator.restore(savepoint);
                }
            }
            // Now pass it recursively to this method.
            return getLowestMembersForAccess(
                evaluator, hAccess, newMap);
        }
        return new ArrayList<Member>(membersWithAccess.keySet());
    }

    /**
     * A hierarchy is ragged if it contains one or more levels with hidden
     * members.
     */
    public boolean isRagged() {
        for (Level level : levels) {
            if (((RolapLevel) level).getHideMemberCondition()
                != RolapLevel.HideMemberCondition.Never)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an expression which will compute a member's value by aggregating
     * its children.
     *
     * <p>It is efficient to share one expression between all calculated members
     * in a parent-child hierarchy, so we only need need to validate the
     * expression once.
     */
    synchronized Exp getAggregateChildrenExpression() {
        if (aggregateChildrenExpression == null) {
            UnresolvedFunCall fc = new UnresolvedFunCall(
                "$AggregateChildren",
                Syntax.Internal,
                new Exp[] {new HierarchyExpr(this)});
            Validator validator =
                    Util.createSimpleValidator(BuiltinFunTable.instance());
            aggregateChildrenExpression = fc.accept(validator);
        }
        return aggregateChildrenExpression;
    }

    /**
     * Builds a dimension which maps onto a table holding the transitive
     * closure of the relationship for this parent-child level.
     *
     * <p>This method is triggered by the
     * {@link mondrian.olap.MondrianDef.Closure} element
     * in a schema, and is only meaningful for a parent-child hierarchy.
     *
     * <p>When a Schema contains a parent-child Hierarchy that has an
     * associated closure table, Mondrian creates a parallel internal
     * Hierarchy, called a "closed peer", that refers to the closure table.
     * This is indicated in the schema at the level of a Level, by including a
     * Closure element. The closure table represents
     * the transitive closure of the parent-child relationship.
     *
     * <p>The peer dimension, with its single hierarchy, and 3 levels (all,
     * closure, item) really 'belong to' the parent-child level. If a single
     * hierarchy had two parent-child levels (however unlikely this might be)
     * then each level would have its own auxiliary dimension.
     *
     * <p>For example, in the demo schema the [HR].[Employee] dimension
     * contains a parent-child hierarchy:
     *
     * <pre>
     * &lt;Dimension name="Employees" foreignKey="employee_id"&gt;
     *   &lt;Hierarchy hasAll="true" allMemberName="All Employees"
     *         primaryKey="employee_id"&gt;
     *     &lt;Table name="employee"/&gt;
     *     &lt;Level name="Employee Id" type="Numeric" uniqueMembers="true"
     *            column="employee_id" parentColumn="supervisor_id"
     *            nameColumn="full_name" nullParentValue="0"&gt;
     *       &lt;Closure parentColumn="supervisor_id"
     *                   childColumn="employee_id"&gt;
     *          &lt;Table name="employee_closure"/&gt;
     *       &lt;/Closure&gt;
     *       ...
     * </pre>
     * The internal closed peer Hierarchy has this structure:
     * <pre>
     * &lt;Dimension name="Employees" foreignKey="employee_id"&gt;
     *     ...
     *     &lt;Hierarchy name="Employees$Closure"
     *         hasAll="true" allMemberName="All Employees"
     *         primaryKey="employee_id" primaryKeyTable="employee_closure"&gt;
     *       &lt;Join leftKey="supervisor_id" rightKey="employee_id"&gt;
     *         &lt;Table name="employee_closure"/&gt;
     *         &lt;Table name="employee"/&gt;
     *       &lt;/Join&gt;
     *       &lt;Level name="Closure"  type="Numeric" uniqueMembers="false"
     *           table="employee_closure" column="supervisor_id"/&gt;
     *       &lt;Level name="Employee" type="Numeric" uniqueMembers="true"
     *           table="employee_closure" column="employee_id"/&gt;
     *     &lt;/Hierarchy&gt;
     * </pre>
     *
     * <p>Note that the original Level with the Closure produces two Levels in
     * the closed peer Hierarchy: a simple peer (Employee) and a closed peer
     * (Closure).
     *
     * @param src a parent-child Level that has a Closure clause
     * @param clos a Closure clause
     * @return the closed peer Level in the closed peer Hierarchy
     */
    RolapDimension createClosedPeerDimension(
        RolapLevel src,
        MondrianDef.Closure clos,
        MondrianDef.CubeDimension xmlDimension)
    {
        // REVIEW (mb): What about attribute primaryKeyTable?

        // Create a peer dimension.
        RolapDimension peerDimension = new RolapDimension(
            dimension.getSchema(),
            dimension.getName() + "$Closure",
            null,
            true,
            "Closure dimension for parent-child hierarchy " + getName(),
            DimensionType.StandardDimension,
            dimension.isHighCardinality(),
            Collections.<String, Annotation>emptyMap());

        // Create a peer hierarchy.
        RolapHierarchy peerHier = peerDimension.newHierarchy(null, true, this);
        peerHier.allMemberName = getAllMemberName();
        peerHier.allMember = (RolapMemberBase) getAllMember();
        peerHier.allLevelName = getAllLevelName();
        peerHier.sharedHierarchyName = getSharedHierarchyName();
        MondrianDef.Join join = new MondrianDef.Join();
        peerHier.relation = join;
        join.left = clos.table;         // the closure table
        join.leftKey = clos.parentColumn;
        join.right = relation;     // the unclosed base table
        join.rightKey = clos.childColumn;

        // Create the upper level.
        // This represents all groups of descendants. For example, in the
        // Employee closure hierarchy, this level has a row for every employee.
        int index = peerHier.levels.length;
        int flags = src.getFlags() &~ RolapLevel.FLAG_UNIQUE;
        MondrianDef.Expression keyExp =
            new MondrianDef.Column(clos.table.name, clos.parentColumn);

        RolapLevel level =
            new RolapLevel(
                peerHier, "Closure", caption, true, description, index++,
                keyExp, null, null, null,
                null, null,  // no longer a parent-child hierarchy
                null,
                RolapProperty.emptyArray,
                flags | RolapLevel.FLAG_UNIQUE,
                src.getDatatype(),
                null,
                src.getHideMemberCondition(),
                src.getLevelType(),
                "",
                Collections.<String, Annotation>emptyMap());
        peerHier.levels = Util.append(peerHier.levels, level);

        // Create lower level.
        // This represents individual items. For example, in the Employee
        // closure hierarchy, this level has a row for every direct and
        // indirect report of every employee (which is more than the number
        // of employees).
        flags = src.getFlags() | RolapLevel.FLAG_UNIQUE;
        keyExp = new MondrianDef.Column(clos.table.name, clos.childColumn);
        RolapLevel sublevel = new RolapLevel(
            peerHier,
            "Item",
            null,
            true,
            null,
            index++,
            keyExp,
            null,
            null,
            null,
            null,
            null,  // no longer a parent-child hierarchy
            null,
            RolapProperty.emptyArray,
            flags,
            src.getDatatype(),
            src.getInternalType(),
            src.getHideMemberCondition(),
            src.getLevelType(),
            "",
            Collections.<String, Annotation>emptyMap());
        peerHier.levels = Util.append(peerHier.levels, sublevel);

        return peerDimension;
    }

    /**
     * Sets default member of this Hierarchy.
     *
     * @param defaultMember Default member
     */
    public void setDefaultMember(Member defaultMember) {
        if (defaultMember != null) {
            this.defaultMember = defaultMember;
        }
    }


    /**
     * <p>Gets "unique key level name" attribute of this Hierarchy, if set.
     * If set, this property indicates that all level properties are
     * functionally dependent (invariant) on their associated levels,
     * and that the set of levels from the root to the named level (inclusive)
     * effectively defines an alternate key.</p>
     *
     * <p>This allows the GROUP BY to be eliminated from associated queries.</p>
     *
     * @return the name of the "unique key" level, or null if not specified
     */
    public String getUniqueKeyLevelName() {
        return uniqueKeyLevelName;
    }

    /**
     * Returns the ordinal of this hierarchy in its cube.
     *
     * <p>Temporarily defined against RolapHierarchy; will be moved to
     * RolapCubeHierarchy as soon as the measures hierarchy is a
     * RolapCubeHierarchy.
     *
     * @return Ordinal of this hierarchy in its cube
     */
    public int getOrdinalInCube() {
        // This is temporary to verify that all calls to this method are for
        // the measures hierarchy. For all other hierarchies, the context will
        // be a RolapCubeHierarchy.
        //
        // In particular, if this method is called from
        // RolapEvaluator.setContext, the caller of that method should have
        // passed in a RolapCubeMember, not a RolapMember.
        assert dimension.isMeasures();
        return 0;
    }


    /**
     * A <code>RolapNullMember</code> is the null member of its hierarchy.
     * Every hierarchy has precisely one. They are yielded by operations such as
     * <code>[Gender].[All].ParentMember</code>. Null members are usually
     * omitted from sets (in particular, in the set constructor operator "{ ...
     * }".
     */
    static class RolapNullMember extends RolapMemberBase {
        RolapNullMember(final RolapLevel level) {
            super(
                null,
                level,
                RolapUtil.sqlNullValue,
                RolapUtil.mdxNullLiteral(),
                MemberType.NULL);
            assert level != null;
        }
    }

    /**
     * Calculated member which is also a measure (that is, a member of the
     * [Measures] dimension).
     */
    protected static class RolapCalculatedMeasure
        extends RolapCalculatedMember
        implements RolapMeasure
    {
        private RolapResult.ValueFormatter cellFormatter;

        public RolapCalculatedMeasure(
            RolapMember parent, RolapLevel level, String name, Formula formula)
        {
            super(parent, level, name, formula);
        }

        public synchronized void setProperty(String name, Object value) {
            if (name.equals(Property.CELL_FORMATTER.getName())) {
                String cellFormatterClass = (String) value;
                try {
                    CellFormatter formatter =
                        RolapSchema.getCellFormatter(
                            cellFormatterClass,
                            null);
                    this.cellFormatter =
                        new RolapResult.CellFormatterValueFormatter(formatter);
                } catch (Exception e) {
                    throw MondrianResource.instance().CellFormatterLoadFailed
                        .ex(
                            cellFormatterClass, getUniqueName(), e);
                }
            }
            if (name.equals(Property.CELL_FORMATTER_SCRIPT.name)) {
                String language = (String) getPropertyValue(
                    Property.CELL_FORMATTER_SCRIPT_LANGUAGE.name);
                String scriptText = (String) value;
                try {
                    final Scripts.ScriptDefinition script =
                        new Scripts.ScriptDefinition(
                            scriptText,
                            Scripts.ScriptLanguage.lookup(language));
                    CellFormatter formatter =
                        RolapSchema.getCellFormatter(
                            null,
                            script);
                    this.cellFormatter =
                        new RolapResult.CellFormatterValueFormatter(formatter);
                } catch (Exception e) {
                    throw MondrianResource.instance().CellFormatterLoadFailed
                        .ex(
                            scriptText, getUniqueName(), e);
                }
            }
            super.setProperty(name, value);
        }

        public RolapResult.ValueFormatter getFormatter() {
            return cellFormatter;
        }
    }

    /**
     * Substitute for a member in a hierarchy whose rollup policy is 'partial'
     * or 'hidden'. The member is calculated using an expression which
     * aggregates only visible descendants.
     *
     * <p>Note that this class extends RolapCubeMember only because other code
     * expects that all members in a RolapCubeHierarchy are RolapCubeMembers.
     * As part of {@link mondrian.util.Bug#BugSegregateRolapCubeMemberFixed},
     * maybe make {@link mondrian.rolap.RolapCubeMember} an interface.
     *
     * @see mondrian.olap.Role.RollupPolicy
     */
    public static class LimitedRollupMember extends RolapCubeMember {
        public final RolapMember member;
        private final Exp exp;
        final HierarchyAccess hierarchyAccess;

        LimitedRollupMember(
            RolapCubeMember member,
            Exp exp,
            HierarchyAccess hierarchyAccess)
        {
            super(
                member.getParentMember(),
                member.getRolapMember(),
                member.getLevel());
            this.hierarchyAccess = hierarchyAccess;
            assert !(member instanceof LimitedRollupMember);
            this.member = member;
            this.exp = exp;
        }

        public boolean equals(Object o) {
            return o instanceof LimitedRollupMember
                && ((LimitedRollupMember) o).member.equals(member);
        }

        public int hashCode() {
            int hash = member.hashCode();
            hash = Util.hash(hash, exp);
            return hash;
        }

        public Exp getExpression() {
            return exp;
        }

        protected boolean computeCalculated(final MemberType memberType) {
            return true;
        }

        public boolean isCalculated() {
            return false;
        }

        public boolean isEvaluated() {
            return true;
        }
    }

    /**
     * Member reader which wraps a hierarchy's member reader, and if the
     * role has limited access to the hierarchy, replaces members with
     * dummy members which evaluate to the sum of only the accessible children.
     */
    private static class LimitedRollupSubstitutingMemberReader
        extends SubstitutingMemberReader
    {
        private final Role.HierarchyAccess hierarchyAccess;
        private final Exp exp;

        /**
         * Creates a LimitedRollupSubstitutingMemberReader.
         *
         * @param memberReader Underlying member reader
         * @param role Role to enforce
         * @param hierarchyAccess Access this role has to the hierarchy
         * @param exp Expression for hidden member
         */
        public LimitedRollupSubstitutingMemberReader(
            MemberReader memberReader,
            Role role,
            Role.HierarchyAccess hierarchyAccess,
            Exp exp)
        {
            super(
                new SmartRestrictedMemberReader(
                    memberReader, role));
            this.hierarchyAccess = hierarchyAccess;
            this.exp = exp;
        }

        public Map<? extends Member, Access> getMemberChildren(
            RolapMember member,
            List<RolapMember> memberChildren,
            MemberChildrenConstraint constraint)
        {
            return memberReader.getMemberChildren(
                member,
                new SubstitutingMemberList(memberChildren),
                constraint);
        }

        public Map<? extends Member, Access> getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            return memberReader.getMemberChildren(
                parentMembers,
                new SubstitutingMemberList(children),
                constraint);
        }

        public RolapMember substitute(RolapMember member, Access access) {
            if (member != null
                && member instanceof MultiCardinalityDefaultMember)
            {
                return new LimitedRollupMember(
                    (RolapCubeMember)
                        ((MultiCardinalityDefaultMember) member)
                            .member.getParentMember(),
                    exp,
                    hierarchyAccess);
            }
            if (member != null
                && (access == Access.CUSTOM || hierarchyAccess
                    .hasInaccessibleDescendants(member)))
            {
                // Member is visible, but at least one of its
                // descendants is not.
                if (member instanceof LimitedRollupMember) {
                    member = ((LimitedRollupMember) member).member;
                }
                return new LimitedRollupMember(
                    (RolapCubeMember) member,
                    exp,
                    hierarchyAccess);
            } else {
                // No need to substitute. Member and all of its
                // descendants are accessible. Total for member
                // is same as for FULL policy.
                return member;
            }
        }

        public RolapMember substitute(final RolapMember member) {
            if (member == null) {
                return null;
            }
            return substitute(member, hierarchyAccess.getAccess(member));
        }

        @Override
        public RolapMember desubstitute(RolapMember member) {
            if (member instanceof LimitedRollupMember) {
                return ((LimitedRollupMember) member).member;
            } else {
                return member;
            }
        }
    }

    /**
     * Compiled expression that computes rollup over a set of visible children.
     * The {@code listCalc} expression determines that list of children.
     */
    private static class LimitedRollupAggregateCalc
        extends AggregateFunDef.AggregateCalc
    {
        public LimitedRollupAggregateCalc(
            Type returnType,
            ListCalc listCalc)
        {
            super(
                new DummyExp(returnType),
                listCalc,
                new ValueCalc(new DummyExp(returnType)));
        }
    }

    /**
     * Dummy element that acts as a namespace for resolving member names within
     * shared hierarchies. Acts like a cube that has a single child, the
     * hierarchy in question.
     */
    private class DummyElement implements OlapElement {
        public String getUniqueName() {
            throw new UnsupportedOperationException();
        }

        public String getName() {
            return "$";
        }

        public String getDescription() {
            throw new UnsupportedOperationException();
        }

        public OlapElement lookupChild(
            SchemaReader schemaReader,
            Id.Segment s,
            MatchType matchType)
        {
            if (!(s instanceof Id.NameSegment)) {
                return null;
            }
            final Id.NameSegment nameSegment = (Id.NameSegment) s;

            if (Util.equalName(nameSegment.name, dimension.getName())) {
                return dimension;
            }
            // Archaic form <dimension>.<hierarchy>, e.g. [Time.Weekly].[1997]
            if (!MondrianProperties.instance().SsasCompatibleNaming.get()
                && Util.equalName(
                    nameSegment.name,
                    dimension.getName() + "." + subName))
            {
                return RolapHierarchy.this;
            }
            return null;
        }

        public String getQualifiedName() {
            throw new UnsupportedOperationException();
        }

        public String getCaption() {
            throw new UnsupportedOperationException();
        }

        public Hierarchy getHierarchy() {
            throw new UnsupportedOperationException();
        }

        public Dimension getDimension() {
            throw new UnsupportedOperationException();
        }

        public boolean isVisible() {
            throw new UnsupportedOperationException();
        }

        public String getLocalized(LocalizedProperty prop, Locale locale) {
            throw new UnsupportedOperationException();
        }
    }
}

// End RolapHierarchy.java
