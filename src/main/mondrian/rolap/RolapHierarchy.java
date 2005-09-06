/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.olap.type.Type;
import mondrian.olap.type.MemberType;
import mondrian.rolap.sql.SqlQuery;

import org.apache.log4j.Logger;
import java.util.List;

/**
 * <code>RolapHierarchy</code> implements {@link Hierarchy} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapHierarchy extends HierarchyBase {

    private static final Logger LOGGER = Logger.getLogger(RolapHierarchy.class);

    /**
     * The raw member reader. For a member reader which incorporates access
     * control and deals with hidden members (if the hierarchy is ragged), use
     * {@link #getMemberReader(Role)}.
     */
    MemberReader memberReader;
    MondrianDef.Hierarchy xmlHierarchy;
    private String memberReaderClass;
    private MondrianDef.Relation relation;
    private Member defaultMember;
    private String defaultMemberName;
    private RolapNullMember nullMember;

    /**
     * If this hierarchy is a public -- that is, it belongs to a dimension
     * which is a usage of a shared dimension -- then
     * <code>sharedHierarchy</code> holds the unique name of the shared
     * hierarchy; otherwise it is null.
     *
     * <p> Suppose this hierarchy is "Weekly" in the dimension "Order Date" of
     * cube "Sales", and that "Order Date" is a usage of the "Time"
     * dimension. Then <code>sharedHierarchy</code> will be "[Time].[Weekly]".
     **/
    private String sharedHierarchy;

    private Exp aggregateChildrenExpression;

    // for newClosedPeerHierarchy() to copy; but never used??
    private String primaryKey;

    /**
     * Type for members of this hierarchy. Set once to avoid excessive newing.
     */
    final Type memberType = new MemberType(this, null, null);

    RolapHierarchy(RolapDimension dimension, String subName, boolean hasAll) {
        super(dimension, subName, hasAll);

        this.levels = new RolapLevel[0];
        setCaption(dimension.getCaption());

        if (hasAll) {
            this.allLevelName = "(All)";
            Util.discard(newLevel(this.allLevelName,
                    RolapLevel.ALL | RolapLevel.UNIQUE));
            this.allMemberName = "All " + name + "s";
        }
    }

    /**
     * Creates a <code>RolapHierarchy</code>.
     *
     * @param cube Cube this hierarchy belongs to, or null if this is a shared
     *     hierarchy
     */
    RolapHierarchy(RolapCube cube, RolapDimension dimension,
            MondrianDef.Hierarchy xmlHierarchy,
            MondrianDef.CubeDimension xmlCubeDimension) {
        this(dimension, xmlHierarchy.name, xmlHierarchy.hasAll.booleanValue());

        if (xmlHierarchy.relation == null &&
                xmlHierarchy.memberReaderClass == null &&
                cube != null) {
            xmlHierarchy.relation = cube.fact;
        }
        this.xmlHierarchy = xmlHierarchy;
        this.relation = xmlHierarchy.relation;
        this.memberReaderClass = xmlHierarchy.memberReaderClass;
        if (hasAll) {
            if (xmlHierarchy.allMemberName != null) {
                this.allMemberName = xmlHierarchy.allMemberName;
            }
            if (xmlHierarchy.allLevelName != null) {
                this.allLevelName = xmlHierarchy.allLevelName;
            }
            this.levels = new RolapLevel[xmlHierarchy.levels.length + 1];
            this.levels[0] = new RolapLevel(
                    this, 0, this.allLevelName, null, null, null, null, null,
                    null, RolapProperty.emptyArray,
                    RolapLevel.ALL | RolapLevel.UNIQUE,
                    RolapLevel.HideMemberCondition.Never,
                    LevelType.Regular);
            for (int i = 0; i < xmlHierarchy.levels.length; i++) {
                final MondrianDef.Level xmlLevel = xmlHierarchy.levels[i];
                if (xmlLevel.getKeyExp() == null &&
                        xmlHierarchy.memberReaderClass == null) {
                    throw MondrianResource.instance()
                            .newLevelMustHaveNameExpression(xmlLevel.name);
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
            this.sharedHierarchy = sharedDimensionName;
            if (subName != null) {
                this.sharedHierarchy += "." + subName; // e.g. "Time.Weekly"
            }
        } else {
            this.sharedHierarchy = null;
        }
        if (xmlHierarchy.relation != null &&
                xmlHierarchy.memberReaderClass != null) {
            throw MondrianResource.instance()
                    .newHierarchyMustNotHaveMoreThanOneSource(getUniqueName());
        }
        this.primaryKey = xmlHierarchy.primaryKey;
        if (!Util.isEmpty(xmlHierarchy.caption)) {
            setCaption(xmlHierarchy.caption);
        }
        defaultMemberName = xmlHierarchy.defaultMember;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public boolean equals(Object o) {
        if (!(o instanceof RolapHierarchy)) {
            return false;
        }
        if (this == o) {
            return true;
        }

        RolapHierarchy that = (RolapHierarchy)o;
        return ((sharedHierarchy == null) || (that.sharedHierarchy == null))
            ? false
            : (sharedHierarchy.equals(that.sharedHierarchy) &&
                getUniqueName().equals(that.getUniqueName()));
    }

    public int hashCode() {
        return super.hashCode() ^ (sharedHierarchy == null
            ? 0
            : sharedHierarchy.hashCode());
    }

    /**
     * Initializes a hierarchy within the context of a cube.
     */
    void init(RolapCube cube, MondrianDef.CubeDimension xmlDimension) {
        for (int i = 0; i < levels.length; i++) {
            ((RolapLevel) levels[i]).init(cube, xmlDimension);
        }
        if (this.memberReader == null) {
            this.memberReader = getRolapSchema().createMemberReader(
                sharedHierarchy, this, memberReaderClass);
        }
        if (defaultMemberName != null) {
            String[] uniqueNameParts = Util.explode(defaultMemberName);

            // We strip off the parent dimension name if the defaultMemberName
            // is the full unique name, [Time].[2004] rather than simply
            // [2004].
            Dimension dim = getDimension();
            if (dim.getName().equals(uniqueNameParts[0])) {
                String[] tmp = new String[uniqueNameParts.length-1];
                System.arraycopy(uniqueNameParts, 1, tmp, 0,
                                uniqueNameParts.length-1);
                uniqueNameParts = tmp;
            }

            // Now lookup the name from the hierarchy's members.
            defaultMember = memberReader.lookupMember(uniqueNameParts, false);
            if (defaultMember == null) {
                throw Util.newInternal(
                    "Can not find Default Member with name \""
                        + defaultMemberName + "\"");
            }
        }
    }

    RolapLevel newLevel(String name, int flags) {
        RolapLevel level = new RolapLevel(
                this, this.levels.length, name, null, null, null,
                null, null, null, RolapProperty.emptyArray, flags,
                RolapLevel.HideMemberCondition.Never, LevelType.Regular);
        this.levels = (RolapLevel[]) RolapUtil.addElement(this.levels, level);
        return level;
    }

    /**
     * If this hierarchy has precisely one table, returns that table;
     * if this hierarchy has no table, return the cube's fact-table;
     * otherwise, returns null.
     */
    MondrianDef.Relation getUniqueTable() {
        if (relation instanceof MondrianDef.Table ||
                relation instanceof MondrianDef.View) {
            return relation;
        } else if (relation instanceof MondrianDef.Join) {
            return null;
        } else {
            throw Util.newInternal(
                    "hierarchy's relation is a " + relation.getClass());
        }
    }

    boolean tableExists(String tableName) {
        return (relation != null) && tableExists(tableName, relation);
    }

    private static boolean tableExists(String tableName,
                                       MondrianDef.Relation relation) {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            // Check by table name and alias
            return table.name.equals(tableName) ||
                ((table.alias != null) && table.alias.equals(tableName));
        }
        if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            return tableExists(tableName, join.left) ||
                tableExists(tableName, join.right);
        }
        return false;
    }

    RolapSchema getRolapSchema() {
        return (RolapSchema) dimension.getSchema();
    }

    MondrianDef.Relation getRelation() {
        return relation;
    }

    public Member getDefaultMember() {
        // use lazy initialization to get around bootstrap issues
        if (defaultMember == null) {
            List rootMembers = memberReader.getRootMembers();
            if (rootMembers.size() == 0) {
                throw MondrianResource.instance().
                newInvalidHierarchyCondition(this.getUniqueName());
/*
                throw Util.newError(
                    "cannot get default member: hierarchy " + getUniqueName() +
                    " has no root members");
*/
            }
            defaultMember = (RolapMember) rootMembers.get(0);
        }
        return defaultMember;
    }

    public Member getNullMember() {
        // use lazy initialization to get around bootstrap issues
        if (nullMember == null) {
            nullMember = new RolapNullMember(this);
        }
        return nullMember;
    }

    public Member createMember(Member parent,
                               Level level,
                               String name,
                               Formula formula) {
        return (formula != null)
            ? new RolapCalculatedMember(
                (RolapMember) parent, (RolapLevel) level, name, formula)
            : new RolapMember(
                (RolapMember) parent, (RolapLevel) level, name);
    }

    String getAlias() {
        return getName();
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
     *    topmost ('all') expression, which may require more columns and more joins
     */
    void addToFrom(SqlQuery query, MondrianDef.Expression expression) {
        if (relation == null) {
            throw Util.newError(
                    "cannot add hierarchy " + getUniqueName() +
                    " to query: it does not have a <Table>, <View> or <Join>");
        }
        final boolean failIfExists = false;
        MondrianDef.Relation subRelation = relation;
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
                subRelation = relationSubset(relation, expression.getTableAlias());

            }
        }
        query.addFrom(subRelation, null, failIfExists);
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the relation <code>alias</code>, or null if these is no relation with
     * such an alias.
     */
    private static MondrianDef.Relation relationSubset(
        MondrianDef.Relation relation,
        String alias) {

        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            // lookup by both alias and table name
            return (table.getAlias().equals(alias))
                ? relation
                : (table.name.equals(alias) ? relation : null);

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.Relation rightRelation = relationSubset(join.right, alias);
            return (rightRelation == null)
                ? relationSubset(join.left, alias)
                : join;

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Returns a member reader which enforces the access-control profile of
     * <code>role</code>.
     *
     * @pre role != null
     * @post return != null
     */
    MemberReader getMemberReader(Role role) {
        final int access = role.getAccess(this);
        switch (access) {
        case Access.NONE:
            throw Util.newInternal("Illegal access to members of hierarchy "
                    + this);
        case Access.ALL:
            return (isRagged())
                ? new RestrictedMemberReader(memberReader, role)
                : memberReader;

        case Access.CUSTOM:
            return new RestrictedMemberReader(memberReader, role);
        default:
            throw Access.instance().badValue(access);
        }
    }

    /**
     * A hierarchy is ragged if it contains one or more levels with hidden
     * members.
     */
    boolean isRagged() {
        for (int i = 0; i < levels.length; i++) {
            RolapLevel level = (RolapLevel) levels[i];
            if (level.getHideMemberCondition() !=
                    RolapLevel.HideMemberCondition.Never) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an expression which will compute a member's value by aggregating
     * its children.
     *
     * <p>It is efficient to share one expression between all calculated members in
     * a parent-child hierarchy, so we only need need to validate the expression
     * once.
     */
    synchronized Exp getAggregateChildrenExpression() {
        if (aggregateChildrenExpression == null) {
            FunCall fc = new FunCall(
                "$AggregateChildren",
                Syntax.Internal,
                new Exp[] {this});
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
     * <p>This method is triggered by the {@link MondrianDef.Closure} element
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
     *       &lt;Closure parentColumn="supervisor_id" childColumn="employee_id"&gt;
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
        RolapCube cube,
        MondrianDef.CubeDimension xmlDimension) {

        // REVIEW (mb): What about attribute primaryKeyTable?

        // Create a peer dimension.
        RolapDimension peerDimension = new RolapDimension(
            dimension.getSchema(),
            dimension.getName() + "$Closure",
            RolapDimension.getNextOrdinal(),
            DimensionType.StandardDimension);

        // Create a peer hierarchy.
        RolapHierarchy peerHier = peerDimension.newHierarchy(subName, true);
        peerHier.allMemberName = allMemberName;
        peerHier.allLevelName = allLevelName;
        peerHier.sharedHierarchy = sharedHierarchy;
        peerHier.primaryKey = primaryKey;
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
        int flags = src.getFlags() &~ RolapLevel.UNIQUE;
        MondrianDef.Expression keyExp =
            new MondrianDef.Column(clos.table.name, clos.parentColumn);
        RolapLevel level = new RolapLevel(peerHier, index++,
            "Closure",
            keyExp, null, null,
            null, null,  // no longer a parent-child hierarchy
            null, RolapProperty.emptyArray, flags,
            src.getHideMemberCondition(), src.getLevelType());
        peerHier.levels =
            (RolapLevel[]) RolapUtil.addElement(peerHier.levels, level);

        // Create lower level.
        // This represents individual items. For example, in the Employee
        // closure hierarchy, this level has a row for every direct and
        // indirect report of every employee (which is more than the number
        // of employees).
        flags = src.getFlags() | RolapLevel.UNIQUE;
        keyExp = new MondrianDef.Column(clos.table.name, clos.childColumn);
        RolapLevel sublevel = new RolapLevel(
            peerHier,
            index++,
            "Item",
            keyExp,
            null,
            null,
            null,
            null,  // no longer a parent-child hierarchy
            null,
            RolapProperty.emptyArray,
            flags,
            src.getHideMemberCondition(),
            src.getLevelType());
        peerHier.levels =
            (RolapLevel[]) RolapUtil.addElement(peerHier.levels, sublevel);

/*
RME HACK
*/
        cube.createUsage(peerHier, xmlDimension);

        return peerDimension;
    }



    /**
     * A <code>RolapNullMember</code> is the null member of its hierarchy.
     * Every hierarchy has precisely one. They are yielded by operations such as
     * <code>[Gender].[All].ParentMember</code>. Null members are usually
     * omitted from sets (in particular, in the set constructor operator "{ ...
     * }".
     */
    class RolapNullMember extends RolapMember {
        RolapNullMember(RolapHierarchy hierarchy) {
            super(null,
                  (RolapLevel) hierarchy.getLevels()[0],
                  null,
                  "#Null",
                  NULL_MEMBER_TYPE);
        }

        public boolean isNull() {
            return true;
        }
    }

}
// End RolapHierarchy.java
