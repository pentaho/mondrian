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
 * <p>NOTE: This class must not contain any references to XML (MondrianDef)
 * objects. Put those in {@link mondrian.rolap.RolapSchemaLoader}.
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
    RolapMember defaultMember;
    protected RolapMember nullMember;

    private Exp aggregateChildrenExpression;

    /**
     * The level that the null member belongs too.
     */
    protected RolapLevel nullLevel;

    /**
     * The 'all' member of this hierarchy. This exists even if the hierarchy
     * does not officially have an 'all' member.
     */
    protected RolapMemberBase allMember;
    private static final int ALL_LEVEL_CARDINALITY = 1;
    private static final int NULL_LEVEL_CARDINALITY = 1;
    final RolapAttribute attribute;
    private final Map<String, Annotation> annotationMap;
    final RolapHierarchy closureFor;

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
        String caption,
        String description,
        boolean hasAll,
        RolapHierarchy closureFor,
        RolapAttribute attribute,
        Map<String, Annotation> annotationMap)
    {
        super(
            dimension, subName, uniqueName,
            visible, caption, description, hasAll);
        this.attribute = attribute;
        this.annotationMap = annotationMap;
        this.closureFor = closureFor;
        this.isScenario = subName != null && subName.equals("Scenario");
        assert !isScenario
            || dimension.getDimensionType()
            == org.olap4j.metadata.Dimension.Type.SCENARIO;
    }

    void initHierarchy(
        RolapSchemaLoader schemaLoader,
        String allLevelName,
        String allMemberName,
        String allMemberCaption)
    {
        assert !(this instanceof RolapCubeHierarchy);

        // Even if !hasAll, there is still an invisible 'all' member, therefore
        // we need to set allMemberName and allLevelName.
        if (allMemberName != null) {
            this.allMemberName = allMemberName;
        } else {
            this.allMemberName = "All " + name + "s";
        }

        // Create an 'all' level even if the hierarchy does not officially
        // have one.
        if (allLevelName == null) {
            allLevelName = "(All)";
        }
        final RolapLevel allLevel =
            new RolapLevel(
                this,
                allLevelName,
                true,
                null,
                null,
                0,
                ALL_ATTRIBUTE.inDimension(getDimension()),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                Collections.<String, Annotation>emptyMap());
        if (hasAll) {
            this.levelList.add(allLevel);
        }

        // The null member belongs to a level with very similar properties to
        // the 'all' level.
        this.nullLevel =
            new RolapLevel(
                this,
                allLevelName,
                true,
                null,
                null,
                0,
                NULL_ATTRIBUTE.inDimension(getDimension()),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                Collections.<String, Annotation>emptyMap());

        this.nullMember = new RolapNullMember(nullLevel);

        if (dimension.isMeasures()) {
            levelList.add(
                new RolapLevel(
                    this,
                    RolapSchemaLoader.MEASURES_LEVEL_NAME,
                    true,
                    null,
                    null,
                    levelList.size(),
                    MEASURES_ATTRIBUTE.inDimension(getDimension()),
                    null,
                    Collections.<RolapSchema.PhysColumn>emptyList(),
                    null,
                    null,
                    RolapLevel.HideMemberCondition.Never,
                    Collections.<String, Annotation>emptyMap()));
        }

        if (this instanceof RolapCubeHierarchy) {
            Util.deprecated("checked above", true);
            return;
        }

        // Create an all member; assign caption if supplied.
        this.allMember =
            new RolapMemberBase(
                null,
                allLevel,
                Util.COMPARABLE_EMPTY_LIST,
                this.allMemberName,
                Member.MemberType.ALL);
        if (allMemberCaption != null
            && allMemberCaption.length() > 0)
        {
            this.allMember.setCaption(allMemberCaption);
        }
        this.allMember.setOrdinal(0);
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

    /**
     * Initialize method, called before levels are initialized.
     *
     * @param schemaLoader Schema loader
     * @param memberReaderClass Class to use for reading members
     */
    void init1(
        RolapSchemaLoader schemaLoader,
        String memberReaderClass)
    {
        Util.discard(schemaLoader); // may be needed in future
    }

    /**
     * Initialize method, called after levels are initialized.
     *
     * @param schemaLoader Schema loader
     */
    void init2(RolapSchemaLoader schemaLoader) {
        Util.discard(schemaLoader); // may be needed in future

        // first create memberReader
        if (memberReader == null) {
            memberReader =
                getRolapSchema().createMemberReader(
                    this, null);
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

    @Override
    public RolapDimension getDimension() {
        return (RolapDimension) dimension;
    }

    public final RolapSchema getRolapSchema() {
        return ((RolapDimension) dimension).schema;
    }

    public RolapMember getDefaultMember() {
        assert defaultMember != null;
        return defaultMember;
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
        if (formula == null) {
            return new RolapMemberBase(
                (RolapMember) parent, (RolapLevel) level, name,
                name, mondrian.olap.Member.MemberType.REGULAR);
        } else if (level.getDimension().isMeasures()) {
            return new RolapCalculatedMeasure(
                (RolapMember) parent, (RolapLevel) level, name, formula);
        } else {
            return new RolapCalculatedMember(
                (RolapMember) parent, (RolapLevel) level, name, formula);
        }
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
     * Creates a member reader which enforces the access-control profile of
     * <code>role</code>.
     *
     * <p>This method may not be efficient, so the caller should take care
     * not to call it too often. A cache is a good idea.
     *
     * @param role Role (not null)
     * @return Member reader that implements access control (never null)
     */
    MemberReader createMemberReader(Role role) {
        return createMemberReader(this, role);
    }

    protected static MemberReader createMemberReader(
        final RolapHierarchy hierarchy,
        Role role)
    {
        final Access access = role.getAccess(hierarchy);
        switch (access) {
        case NONE:
            role.getAccess(hierarchy); // todo: remove
            throw Util.newInternal(
                "Illegal access to members of hierarchy " + hierarchy);
        case ALL:
            return (hierarchy.isRagged())
                ? new RestrictedMemberReader(hierarchy.getMemberReader(), role)
                : hierarchy.getMemberReader();

        case CUSTOM:
            final Role.HierarchyAccess hierarchyAccess =
                role.getAccessDetails(hierarchy);
            final Role.RollupPolicy rollupPolicy =
                hierarchyAccess.getRollupPolicy();
            final NumericType returnType = new NumericType();
            switch (rollupPolicy) {
            case FULL:
                return new RestrictedMemberReader(
                    hierarchy.getMemberReader(), role);
            case PARTIAL:
                Type memberType1 =
                    new mondrian.olap.type.MemberType(
                        hierarchy.getDimension(), hierarchy,
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
                                    hierarchy.getLowestMembersForAccess(
                                        evaluator, hierarchyAccess, null));
                        }

                        public boolean dependsOn(Hierarchy hierarchy) {
                            return true;
                        }
                    };
                final Calc partialCalc =
                    new LimitedRollupAggregateCalc(
                        returnType, listCalc);

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
                    hierarchy.getMemberReader(),
                    role,
                    hierarchyAccess,
                    partialExp);

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
                    hierarchy.getMemberReader(),
                    role,
                    hierarchyAccess,
                    hiddenExp);

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
    private List<Member> getLowestMembersForAccess(
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
                // Now pass it recursively to this method.
                return getLowestMembersForAccess(evaluator, hAccess, newList);
            }
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
     * <p>Also note that the upper level has {@code uniqueMembers="false"}, even
     * though this is illegal in user hierarchies. This is the 'magic' that
     * achieves the many-to-many behavior.
     *
     * @param src a parent-child Level that has a Closure clause
     * @return the closed peer Level in the closed peer Hierarchy
     */
    RolapDimension createClosedPeerDimension(RolapLevel src)
    {
        // Create a peer dimension.
        RolapDimension peerDimension = new RolapDimension(
            (RolapSchema) dimension.getSchema(),
            dimension.getName() + "$Closure",
            false,
            null,
            "Closure dimension for parent-child hierarchy " + getName(),
            org.olap4j.metadata.Dimension.Type.OTHER,
            false,
            Collections.<String, Annotation>emptyMap());

        // Create a peer hierarchy.
        RolapHierarchy peerHier =
            new RolapHierarchy(
                peerDimension,
                peerDimension.getName(),
                Util.makeFqName(peerDimension, peerDimension.getName()),
                peerDimension.isVisible(),
                peerDimension.getCaption(),
                peerDimension.getDescription(),
                true,
                this,
                null,
                Collections.<String, Annotation>emptyMap());
        peerDimension.addHierarchy(peerHier);
        peerHier.allMemberName = getAllMemberName();
        peerHier.allMember = (RolapMemberBase) getAllMember();

        // Create the upper level.
        // This represents all groups of descendants. For example, in the
        // Employee closure hierarchy, this level has a row for every employee.
        RolapLevel level =
            new RolapLevel(
                peerHier,
                "Closure",
                false,
                caption,
                description,
                peerHier.levelList.size(),
                src.getParentAttribute(),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                src.getHideMemberCondition(),
                Collections.<String, Annotation>emptyMap());
        peerHier.levelList.add(level);

        // Create lower level.
        // This represents individual items. For example, in the Employee
        // closure hierarchy, this level has a row for every direct and
        // indirect report of every employee (which is more than the number
        // of employees).
        RolapLevel sublevel =
            new RolapLevel(
                peerHier,
                "Item",
                false,
                null,
                null,
                peerHier.levelList.size(),
                src.attribute, // TODO: new attr, also change its row count
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                src.getHideMemberCondition(),
                Collections.<String, Annotation>emptyMap());
        peerHier.levelList.add(sublevel);
        return peerDimension;
    }

    /**
     * Sets default member of this Hierarchy.
     *
     * @param member Default member
     */
    public void setDefaultMember(RolapMember member) {
        if (member != null) {
            this.defaultMember = member;
        }
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
        Util.deprecated("move method to RolapCubeHierarchy", false);
        assert dimension.isMeasures();
        return 0;
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
        RolapNullMember(final RolapLevel level) {
            super(
                null,
                level,
                Util.COMPARABLE_EMPTY_LIST,
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

        LimitedRollupMember(
            RolapCubeMember member,
            Exp exp)
        {
            super(
                member.getParentMember(),
                member.getRolapMember(),
                member.getLevel());
            assert !(member instanceof LimitedRollupMember);
            this.member = member;
            this.exp = exp;
        }

        public boolean equals(OlapElement o) {
            return o instanceof LimitedRollupMember
                && ((LimitedRollupMember) o).member.equals(member);
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
                    ((MultiCardinalityDefaultMember) member).getParentMember(),
                    exp);
            }
            if (hierarchyAccess.getAccess(member) == Access.CUSTOM
                || hierarchyAccess.hasInaccessibleDescendants(member))
            {
                // Member is visible, but at least one of its
                // descendants is not.
                return new LimitedRollupMember((RolapCubeMember)member, exp);
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
            null,
            null,
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
            null,
            null,
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
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.REGULAR,
            Integer.MIN_VALUE);
}

// End RolapHierarchy.java
