/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.Role.HierarchyAccess;
import mondrian.olap.fun.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RestrictedMemberReader.MultiCardinalityDefaultMember;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.CellFormatter;
import mondrian.spi.impl.Scripts;

import org.apache.log4j.Logger;

import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.NamedList;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>RolapHierarchy</code> implements {@link Hierarchy} for a ROLAP
 * database.
 *
 * <p>NOTE: This class must not contain any references to XML (MondrianDef)
 * objects. Put those in {@link mondrian.rolap.RolapSchemaLoader}.
 *
 * @author jhyde
 * @since 10 August, 2001
  */
public class RolapHierarchy extends HierarchyBase {

    private static final Logger LOGGER = Logger.getLogger(RolapHierarchy.class);

    protected RolapMember nullMember;

    private Exp aggregateChildrenExpression;

    /**
     * The level that the null member belongs too.
     */
    protected RolapLevel nullLevel;
    protected RolapLevel allLevel;

    /**
     * The 'all' member of this hierarchy. This exists even if the hierarchy
     * does not officially have an 'all' member.
     */
    protected RolapMemberBase allMember;
    private static final int ALL_LEVEL_CARDINALITY = 1;
    private static final int NULL_LEVEL_CARDINALITY = 1;
    final RolapAttribute attribute;
    private final Larder larder;
    public final RolapHierarchy closureFor;

    final NamedList<RolapLevel> levelList = new NamedListImpl<RolapLevel>();

    /** Whether this hierarchy is the Scenario hierarchy of its cube. */
    public final boolean isScenario;

    /**
     * Creates a RolapHierarchy.
     *
     * @param dimension Dimension this hierarchy belongs to
     * @param subName Name of hierarchy, or null if it is the same as the
     *     dimension
     * @param uniqueName Unique name of hierarchy
     * @param hasAll Whether the dimension has an 'all' level
     * @param attribute Attribute this is a hierarchy for; or null
     */
    RolapHierarchy(
        RolapDimension dimension,
        String subName,
        String uniqueName,
        boolean visible,
        boolean hasAll,
        RolapHierarchy closureFor,
        RolapAttribute attribute,
        Larder larder)
    {
        super(dimension, subName, uniqueName, visible, hasAll);
        this.attribute = attribute;
        this.larder = larder;
        this.closureFor = closureFor;
        this.isScenario = subName != null && subName.equals("Scenario");
        assert !isScenario
            || dimension.getDimensionType()
            == org.olap4j.metadata.Dimension.Type.SCENARIO;
    }

    void initHierarchy(
        RolapSchemaLoader schemaLoader,
        String allLevelName)
    {
        if (this instanceof RolapCubeHierarchy) {
            throw new AssertionError();
        }

        // Create an 'all' level even if the hierarchy does not officially
        // have one.
        allLevel =
            new RolapLevel(
                this,
                Util.first(allLevelName, "(All)"),
                true,
                0,
                ALL_ATTRIBUTE.inDimension(getDimension()),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                Larders.EMPTY,
                schemaLoader.resourceMap);
        if (hasAll) {
            this.levelList.add(allLevel);
        }

        // The null member belongs to a level with very similar properties to
        // the 'all' level.
        this.nullLevel =
            new RolapLevel(
                this,
                Util.first(allLevelName, "(All)"),
                true,
                0,
                NULL_ATTRIBUTE.inDimension(getDimension()),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                Larders.EMPTY,
                schemaLoader.resourceMap);

        if (dimension.isMeasures()) {
            levelList.add(
                new RolapLevel(
                    this,
                    RolapSchemaLoader.MEASURES_LEVEL_NAME,
                    true,
                    levelList.size(),
                    MEASURES_ATTRIBUTE.inDimension(getDimension()),
                    null,
                    Collections.<RolapSchema.PhysColumn>emptyList(),
                    null,
                    null,
                    RolapLevel.HideMemberCondition.Never,
                    Larders.EMPTY,
                    schemaLoader.resourceMap));
        }
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
        return getUniqueName().equals(that.getUniqueName());
    }

    public Larder getLarder() {
        return larder;
    }

    @Override
    public RolapDimension getDimension() {
        return (RolapDimension) dimension;
    }

    public final RolapSchema getRolapSchema() {
        return ((RolapDimension) dimension).schema;
    }

    public RolapMember getDefaultMember() {
        throw new UnsupportedOperationException();
    }

    public RolapMember getNullMember() {
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
        throw new UnsupportedOperationException();
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
    void addToFromInverse(SqlQuery query, RolapSchema.PhysExpr expression) {
/*
        if (relation == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        final boolean failIfExists = false;
        Mondrian3Def.RelationOrJoin subRelation = relation;
                TODO:
        if (relation instanceof MondrianDef.Join) {
            if (expression != null) {
                subRelation =
                    relationSubsetInverse(relation, expression.getTableAlias());
            }
        }
        query.addFrom(
            subRelation,
            expression == null ? null : expression.getTableAlias(),
            failIfExists);
*/
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
    protected List<Member> getLowestMembersForAccess(
        Evaluator evaluator,
        HierarchyAccess hAccess,
        List<Member> currentList)
    {
        if (currentList == null) {
            currentList =
                FunUtil.getNonEmptyMemberChildren(
                    evaluator,
                    ((RolapEvaluator) evaluator)
                        .getExpanding());
        }
        boolean goesLower = false;
        for (Member member : currentList) {
            if (hAccess.getAccess(member) != Access.ALL) {
                goesLower = true;
                break;
            }
        }
        if (goesLower) {
            // We still have to go one more level down.
            List<Member> newList = new ArrayList<Member>();
            for (Member member : currentList) {
                int savepoint = evaluator.savepoint();
                try {
                    evaluator.setContext(member);
                    newList.addAll(
                        FunUtil.getNonEmptyMemberChildren(
                            evaluator,
                            member));
                } finally {
                    evaluator.restore(savepoint);
                }
            }
            // Now pass it recursively to this method.
            return getLowestMembersForAccess(evaluator, hAccess, newList);
        }
        return currentList;
    }

    /**
     * A hierarchy is ragged if it contains one or more levels with hidden
     * members.
     */
    public boolean isRagged() {
        for (RolapLevel level : levelList) {
            if (level.getHideMemberCondition()
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
    public synchronized Exp getAggregateChildrenExpression() {
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

    public List<? extends RolapLevel> getLevelList() {
        return Util.cast(levelList);
    }

    /**
     * A <code>RolapNullMember</code> is the null member of its hierarchy.
     * Every hierarchy has precisely one. They are yielded by operations such as
     * <code>[Gender].[All].ParentMember</code>. Null members are usually
     * omitted from sets (in particular, in the set constructor operator "{ ...
     * }".
     */
    static class RolapNullMember extends RolapMemberBase {
        RolapNullMember(final RolapCubeLevel level) {
            super(
                null,
                level,
                Util.COMPARABLE_EMPTY_LIST,
                MemberType.NULL,
                Util.makeFqName(
                    level.getHierarchy(), RolapUtil.mdxNullLiteral()),
                Larders.ofName(RolapUtil.mdxNullLiteral()));
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
            RolapMember parent,
            RolapCubeLevel level,
            String name,
            Formula formula)
        {
            super(parent, level, name, formula);
        }

        public synchronized void setProperty(Property property, Object value) {
            if (property == Property.CELL_FORMATTER) {
                String cellFormatterClass = (String) value;
                try {
                    CellFormatter formatter =
                        RolapSchemaLoader.getFormatter(
                            cellFormatterClass,
                            CellFormatter.class,
                            null);
                    this.cellFormatter =
                        new RolapResult.CellFormatterValueFormatter(formatter);
                } catch (Exception e) {
                    throw MondrianResource.instance().CellFormatterLoadFailed
                        .ex(
                            cellFormatterClass, getUniqueName(), e);
                }
            }
            if (property == Property.CELL_FORMATTER_SCRIPT) {
                String language = (String) getPropertyValue(
                    Property.CELL_FORMATTER_SCRIPT_LANGUAGE);
                String scriptText = (String) value;
                try {
                    final Scripts.ScriptDefinition script =
                        new Scripts.ScriptDefinition(
                            scriptText,
                            Scripts.ScriptLanguage.lookup(language));
                    CellFormatter formatter =
                        RolapSchemaLoader.getFormatter(
                            null,
                            CellFormatter.class,
                            script);
                    this.cellFormatter =
                        new RolapResult.CellFormatterValueFormatter(formatter);
                } catch (Exception e) {
                    throw MondrianResource.instance().CellFormatterLoadFailed
                        .ex(
                            scriptText, getUniqueName(), e);
                }
            }
            super.setProperty(property, value);
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
     * @see mondrian.olap.Role.RollupPolicy
     */
    public static class LimitedRollupMember extends DelegatingRolapMember {
        public final RolapMember member;
        private final Exp exp;

        LimitedRollupMember(
            RolapMember member,
            Exp exp)
        {
            super(member);
            assert !(member instanceof LimitedRollupMember);
            this.member = member;
            this.exp = exp;
        }

        public boolean equals(Object o) {
            return o instanceof LimitedRollupMember
                && ((LimitedRollupMember) o).member.equals(member);
        }

        public int hashCode() {
            return member.hashCode();
        }

        public Exp getExpression() {
            return exp;
        }

        @Override
        public Calc getCompiledExpression(RolapEvaluatorRoot root) {
            return root.getCompiled(getExpression(), true, null);
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
    static class LimitedRollupSubstitutingMemberReader
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
                new RestrictedMemberReader(
                    memberReader, role));
            this.hierarchyAccess = hierarchyAccess;
            this.exp = exp;
        }

        @Override
        public RolapMember substitute(final RolapMember member) {
            if (member == null) {
                return null;
            }
            if (member instanceof MultiCardinalityDefaultMember) {
                return new LimitedRollupMember(
                    member.getParentMember(),
                    exp);
            }
            if (hierarchyAccess.getAccess(member) == Access.CUSTOM
                || hierarchyAccess.hasInaccessibleDescendants(member))
            {
                // Member is visible, but at least one of its
                // descendants is not.
                return new LimitedRollupMember(member, exp);
            } else {
                // No need to substitute. Member and all of its
                // descendants are accessible. Total for member
                // is same as for FULL policy.
                return member;
            }
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
    static class LimitedRollupAggregateCalc
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
    static class DummyElement implements OlapElement {
        private final RolapHierarchy hierarchy;

        DummyElement(RolapHierarchy hierarchy) {
            this.hierarchy = hierarchy;
        }

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

            if (Util.equalName(nameSegment.name, hierarchy.dimension.getName()))
            {
                return hierarchy.dimension;
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

    private static final RolapSharedAttribute ALL_ATTRIBUTE =
        new RolapSharedAttribute(
            "All",
            true,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.ALL,
            ALL_LEVEL_CARDINALITY);

    private static final RolapSharedAttribute NULL_ATTRIBUTE =
        new RolapSharedAttribute(
            "Null",
            true,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.NULL,
            NULL_LEVEL_CARDINALITY);

    private static final RolapSharedAttribute MEASURES_ATTRIBUTE =
        new RolapSharedAttribute(
            "Measures",
            true,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.REGULAR,
            Integer.MIN_VALUE);
}

// End RolapHierarchy.java
