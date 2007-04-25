/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package mondrian.rolap;

import java.util.*;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.agg.*;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.aggmatcher.AggStar;

/**
 * Utility class used by implementations of {@link mondrian.rolap.sql.SqlConstraint},
 * used to generate constraints into {@link mondrian.rolap.sql.SqlQuery}.
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public class SqlConstraintUtils {

    /** Utility class */
    private SqlConstraintUtils() {
    }

    /**
     * For every restricting member in the current context, generates
     * a WHERE condition and a join to the fact table.
     *
     * @param sqlQuery the query to modify
     * @param aggStar Aggregate table, or null if query is against fact table
     * @param strict defines the behavior if the current context contains
     *   calculated members.
     *   If true, an exception is thrown.
     * @param evaluator Evaluator
     */
    public static void addContextConstraint(
        SqlQuery sqlQuery,
        AggStar aggStar,
        Evaluator evaluator,
        boolean strict) {

        Member[] members = evaluator.getMembers();
        if (strict) {
            if (containsCalculatedMember(members)) {
                throw Util.newInternal("can not restrict SQL to calculated Members");
            }
        } else {
            members = removeCalculatedMembers(members);
            members = removeDefaultMembers(members);
        }

        CellRequest request =
                RolapAggregationManager.makeRequest(members, false, false);
        if (request == null) {
            if (strict) {
                throw Util.newInternal("CellRequest is null - why?");
            }
            // One or more of the members was null or calculated, so the
            // request is impossible to satisfy.
            return;
        }
        RolapStar.Column[] columns = request.getConstrainedColumns();
        Object[] values = request.getSingleValues();
        int arity = columns.length;
        // following code is similar to AbstractQuerySpec#nonDistinctGenerateSQL()
        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];

            String expr;
            if (aggStar != null) {
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                AggStar.Table table = aggColumn.getTable();
                table.addToFrom(sqlQuery, false, true);

                expr = aggColumn.generateExprString(sqlQuery);
            } else {
                RolapStar.Table table = column.getTable();
                table.addToFrom(sqlQuery, false, true);

                expr = column.generateExprString(sqlQuery);
            }

            final String value = String.valueOf(values[i]);
            if (RolapUtil.mdxNullLiteral.equalsIgnoreCase(value)) {
                sqlQuery.addWhere(
                    expr,
                    " is ",
                    RolapUtil.sqlNullLiteral);
            } else {
                if (column.getDatatype().isNumeric()) {
                    // make sure it can be parsed
                    Double.valueOf(value);
                }
                final StringBuilder buf = new StringBuilder();
                sqlQuery.getDialect().quote(buf, value, column.getDatatype());
                sqlQuery.addWhere(
                    expr,
                    " = ",
                    buf.toString());
            }
        }
    }

    /**
     * Removes the default members from an array.
     *
     * <p>This is required only if the default member is
     * not the ALL member. The time dimension for example, has 1997 as default
     * member. When we evaluate the query
     * <pre>
     *   select NON EMPTY crossjoin(
     *     {[Time].[1998]}, [Customer].[All].children
     *   ) on columns
     *   from [sales]
     * </pre>
     * the <code>[Customer].[All].children</code> is evaluated with the default
     * member <code>[Time].[1997]</code> in the evaluator context. This is wrong
     * because the NON EMPTY must filter out Customres with no rows in the fact
     * table for 1998 not 1997. So we do not restrict the time dimension and
     * fetch all children.
     *
     * @param members Array of members
     * @return Array of members with default members removed
     */
    private static Member[] removeDefaultMembers(Member[] members) {
        List<Member> result = new ArrayList<Member>();
        result.add(members[0]); // add the measure
        for (int i = 1; i < members.length; i++) {
            Member m = members[i];
            if (m.getHierarchy().getDefaultMember().equals(m)) {
                continue;
            }
            result.add(m);
        }
        return result.toArray(new Member[result.size()]);
    }

    private static Member[] removeCalculatedMembers(Member[] members) {
        List<Member> result = new ArrayList<Member>();
        for (Member member : members) {
            if (!member.isCalculated()) {
                result.add(member);
            }
        }
        return result.toArray(new Member[result.size()]);
    }

    private static boolean containsCalculatedMember(Member[] members) {
        for (Member member : members) {
            if (member.isCalculated()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensures that the table of <code>level</code> is joined to the fact
     * table
     *
     * @param sqlQuery sql query under construction
     * @param aggStar
     * @param e evaluator corresponding to query
     * @param level level to be added to query
     * @param levelToColumnMap maps level to star columns; set only in the
     */
    public static void joinLevelTableToFactTable(
        SqlQuery sqlQuery,
        AggStar aggStar,
        Evaluator e,
        RolapLevel level,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap)
    {
        RolapCube cube = (RolapCube) e.getCube();
        if (!cube.isVirtual()) {
            RolapStar star = cube.getStar();
            levelToColumnMap = star.getLevelToColumnMap(cube);
        }
        RolapStar.Column starColumn = levelToColumnMap.get(level);
        assert starColumn != null;
        if (aggStar != null) {
            int bitPos = starColumn.getBitPosition();
            AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
            AggStar.Table table = aggColumn.getTable();
            table.addToFrom(sqlQuery, false, true);
        } else {
            RolapStar.Table table = starColumn.getTable();
            assert table != null;
            table.addToFrom(sqlQuery, false, true);
        }
    }

    /**
     * Creates a "WHERE parent = value" constraint.
     *
     * @param sqlQuery the query to modify
     * @param levelToColumnMap where to find each level's key
     * @param aggStar Definition of the aggregate table, or null
     * @param parent the list of parent members
     * @param strict defines the behavior if <code>parent</code>
     * is a calculated member. If true, an exception is thrown
     */
    public static void addMemberConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        AggStar aggStar,
        RolapMember parent,
        boolean strict)
    {
        List<RolapMember> list = Collections.singletonList(parent);
        addMemberConstraint(
            sqlQuery, levelToColumnMap, aggStar, list, strict, false);
    }

    /**
     * Creates a "WHERE exp IN (...)" condition containing the values
     * of all parents.  All parents must belong to the same level.
     *
     * <p>If this constraint is part of a native cross join, there are
     * multiple constraining members, and the members comprise the cross
     * product of all unique member keys referenced at each level, then
     * generating IN expressions would result in incorrect results.  In that
     * case, "WHERE ((level1 = val1a AND level2 = val2a AND ...)
     * OR (level1 = val1b AND level2 = val2b AND ...) OR ..." is generated
     * instead.
     *
     * @param sqlQuery the query to modify
     * @param levelToColumnMap where to find each level's key
     * @param aggStar (not used)
     * @param parents the list of parent members
     * @param strict defines the behavior if <code>parents</code>
     *   contains calculated members.
     *   If true, and one of the members is calculated, an exception is thrown.
     * @param crossJoin true if constraint is being generated as part of
     *   a native crossjoin
     */
    public static void addMemberConstraint(
        SqlQuery sqlQuery,
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        AggStar aggStar,
        List<RolapMember> parents,
        boolean strict,
        boolean crossJoin)
    {
        if (parents.size() == 0) {
            return;
        }

        // If this constraint is part of a native cross join and there
        // are multiple values for the parent members, then we can't
        // use IN clauses
        if (crossJoin) {
            RolapLevel level = parents.get(0).getLevel();
            if (!level.isUnique() && !membersAreCrossProduct(parents)) {
                constrainMultiLevelMembers(sqlQuery, parents, strict);
                return;
            }
        }

        for (Collection<RolapMember> c = parents;
            !c.isEmpty();
            c = getUniqueParentMembers(c))
        {
            RolapMember m = c.iterator().next();
            if (m.isAll()) {
                continue;
            }
            if (m.isCalculated()) {
                if (strict) {
                    throw Util.newInternal("addMemberConstraint: cannot " +
                        "restrict SQL to calculated member :" + m);
                }
                continue;
            }
            RolapLevel level = m.getLevel();
            RolapHierarchy hierarchy = level.getHierarchy();
            hierarchy.addToFrom(sqlQuery, level.getKeyExp());
            String q = 
                level.getExpressionWithAlias(
                    sqlQuery, levelToColumnMap, level.getKeyExp());
            RolapStar.Column column = levelToColumnMap.get(level);
            StarColumnPredicate cc = getColumnPredicates(column, c);

            if (!strict &&
                cc instanceof ListColumnPredicate &&
                ((ListColumnPredicate) cc).getPredicates().size() >=
                    MondrianProperties.instance().MaxConstraints.get())
            {
                // Simply get them all, do not create where-clause.
                // Below are two alternative approaches (and code). They
                // both have problems.
            } else {
                final String where = RolapStar.Column.createInExpr(
                    q, cc, level.getDatatype(), sqlQuery.getDialect());
                if (!where.equals("true")) {
                    sqlQuery.addWhere(where);
                }
            }
            if (level.isUnique()) {
                break; // no further qualification needed
            }
        }
    }

    private static StarColumnPredicate getColumnPredicates(
        RolapStar.Column column,
        Collection<RolapMember> members)
    {
        switch (members.size()) {
        case 0:
            return new LiteralStarPredicate(column, false);
        case 1:
            return new MemberColumnPredicate(column, members.iterator().next());
        default:
            List<StarColumnPredicate> predicateList =
                new ArrayList<StarColumnPredicate>();
            for (RolapMember member : members) {
                predicateList.add(new MemberColumnPredicate(column, member));
            }
            return new ListColumnPredicate(column, predicateList);
        }
    }

    private static LinkedHashSet<RolapMember> getUniqueParentMembers(
        Collection<RolapMember> members)
    {
        LinkedHashSet<RolapMember> set = new LinkedHashSet<RolapMember>();
        for (RolapMember m : members) {
            m = m.getParentMember();
            if (m != null) {
                set.add(m);
            }
        }
        return set;
    }

    /**
     * Adds to the where clause of a query expression matching a specified
     * list of members
     *
     * @param sqlQuery query containing the where clause
     * @param members list of constraining members
     * @param strict defines the behavior when calculated members are present
     */
    private static void constrainMultiLevelMembers(
        SqlQuery sqlQuery,
        List<RolapMember> members,
        boolean strict)
    {
        if (sqlQuery.getDialect().supportsMultiValueInExpr()) {
            if (generateMultiValueInExpr(sqlQuery, members, strict)) {
                return;
            }
        }

        // iterate through each level in each member generating
        // AND's across the levels and OR's across the members
        String condition = "(";
        boolean firstMember = true;
        for (RolapMember m : members) {
            if (m.isCalculated()) {
                if (strict) {
                    throw Util.newInternal("addMemberConstraint: cannot " +
                        "restrict SQL to calculated member :" + m);
                }
                continue;
            }
            if (!firstMember) {
                condition += " or ";
            }
            condition += "(";
            boolean firstLevel = true;
            do {
                if (!m.isAll()) {
                    RolapLevel level = m.getLevel();
                    // add the level to the FROM clause if this is the
                    // first member we're generating sql for
                    if (firstMember) {
                        RolapHierarchy hierarchy =
                            (RolapHierarchy) level.getHierarchy();
                        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
                    }
                    if (!firstLevel) {
                        condition += " and ";
                    } else {
                        firstLevel = false;
                    }
                    condition += constrainLevel(
                        level,
                        sqlQuery,
                        getColumnValue(
                            m.getSqlKey(),
                            sqlQuery.getDialect(),
                            level.getDatatype()),
                            false);
                }
                m = m.getParentMember();
            } while (m != null);
            condition += ")";
            firstMember = false;
        }
        condition += ")";
        sqlQuery.addWhere(condition);
    }

    /**
     * @param members list of members
     *
     * @return true if the members comprise the cross product of all unique
     * member keys referenced at each level
     */
    private static boolean membersAreCrossProduct(List<RolapMember> members)
    {
        int crossProdSize = getNumUniqueMemberKeys(members);
        for (Collection<RolapMember> parents = getUniqueParentMembers(members);
            !parents.isEmpty(); parents = getUniqueParentMembers(parents))
        {
            crossProdSize *= parents.size();
        }
        return (crossProdSize == members.size());
    }

    /**
     * @param members list of members
     *
     * @return number of unique member keys in a list of members
     */
    private static int getNumUniqueMemberKeys(List<RolapMember> members)
    {
        Set<Object> set = new HashSet<Object>();
        for (RolapMember m : members) {
            set.add(m.getKey());
        }
        return set.size();
    }

    /**
     * @param key key corresponding to a member
     * @param dialect sql dialect being used
     * @param datatype data type of the member
     *
     * @return string value corresponding to the member
     */
    private static String getColumnValue(
        Object key,
        SqlQuery.Dialect dialect,
        SqlQuery.Datatype datatype)
    {
        if (key != RolapUtil.sqlNullValue) {
            return key.toString();
        } else {
            return RolapUtil.mdxNullLiteral;
        }
    }

    /**
     * Generates a sql expression constraining a level by some value
     *
     * @param level the level
     * @param query the query that the sql expression will be added to
     * @param columnValue value constraining the level
     * @param caseSensitive if true, need to handle case sensitivity of the
     * member value
     *
     * @return generated string corresponding to the expression
     */
    public static String constrainLevel(
        RolapLevel level,
        SqlQuery query,
        String columnValue,
        boolean caseSensitive)
    {
        MondrianDef.Expression exp = level.getNameExp();
        SqlQuery.Datatype datatype;
        if (exp == null) {
            exp = level.getKeyExp();
            datatype = level.getDatatype();
        } else {
            // The schema doesn't specify the datatype of the name column, but
            // we presume that it is a string.
            datatype = SqlQuery.Datatype.String;
        }
        String column = exp.getExpression(query);
        String constraint;
        if (RolapUtil.mdxNullLiteral.equalsIgnoreCase(columnValue)) {
            constraint = column + " is " + RolapUtil.sqlNullLiteral;
        } else {
            if (datatype.isNumeric()) {
                // make sure it can be parsed
                Double.valueOf(columnValue);
            }
            final StringBuilder buf = new StringBuilder();
            query.getDialect().quote(buf, columnValue, datatype);
            String value = buf.toString();
            if (caseSensitive && datatype == SqlQuery.Datatype.String) {
                // Some databases (like DB2) compare case-sensitive. We convert
                // the value to upper-case in the DBMS (e.g. UPPER('Foo'))
                // rather than in Java (e.g. 'FOO') in case the DBMS is running
                // a different locale.
                if (!MondrianProperties.instance().CaseSensitive.get()) {
                    column = query.getDialect().toUpper(column);
                    value = query.getDialect().toUpper(value);
                }
            }

            constraint = column + " = " + value;
        }

        return constraint;
    }

    /**
     * Generates a multi-value IN expression corresponding to a list of
     * member expressions, and adds the expression to the WHERE clause
     * of a query, provided the member values are all non-null
     *
     * @param sqlQuery query containing the where clause
     * @param members list of constraining members
     * @param strict defines the behavior when calculated members are present
     *
     * @return Whether it was possible to generate a multi-value IN expression
     */
    private static boolean generateMultiValueInExpr(
        SqlQuery sqlQuery,
        List<RolapMember> members,
        boolean strict)
    {
        final StringBuilder columnBuf = new StringBuilder();
        columnBuf.append("(");
        final StringBuilder valueBuf = new StringBuilder();
        valueBuf.append("(");

        boolean firstMember = true;
        for (RolapMember m : members) {
            if (m.isCalculated()) {
                if (strict) {
                    throw Util.newInternal("addMemberConstraint: cannot " +
                        "restrict SQL to calculated member :" + m);
                }
                continue;
            }
            boolean firstLevel = true;
            do {
                if (m.isAll()) {
                    m = (RolapMember) m.getParentMember();
                    continue;
                }
                RolapLevel level = m.getLevel();
                // generate the left-hand side of the IN expression, if we're
                // iterating over the first member
                if (firstMember) {
                    RolapHierarchy hierarchy =
                        (RolapHierarchy) level.getHierarchy();
                    hierarchy.addToFrom(sqlQuery, level.getKeyExp());
                    if (!firstLevel) {
                        columnBuf.append(",");
                    }
                    MondrianDef.Expression exp = level.getNameExp();
                    if (exp == null) {
                        exp = level.getKeyExp();
                    }
                    columnBuf.append(exp.getExpression(sqlQuery));
                }

                if (firstLevel) {
                    if (!firstMember) {
                        valueBuf.append("), ");
                    }
                    valueBuf.append("(");
                    firstLevel = false;
                } else {
                    valueBuf.append(",");
                }

                String value = getColumnValue(
                    m.getSqlKey(),
                    sqlQuery.getDialect(),
                    level.getDatatype());
                if (RolapUtil.mdxNullLiteral.equalsIgnoreCase(value)) {
                    return false;
                }
                final StringBuilder buf = new StringBuilder();
                sqlQuery.getDialect().quote(buf, value, level.getDatatype());
                valueBuf.append(buf.toString());

                m = m.getParentMember();
            } while (m != null);
            firstMember = false;
        }

        columnBuf.append(")");
        valueBuf.append("))");

        sqlQuery.addWhere(
            columnBuf.toString() + " in " + valueBuf.toString());

        return true;
    }
}

// End SqlConstraintUtils.java
