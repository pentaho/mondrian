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
     *   If true, an exception is thrown,
     */
    public static void addContextConstraint(
        SqlQuery sqlQuery,
        AggStar aggStar,
        Evaluator e,
        boolean strict) {

        Member[] members = e.getMembers();
        if (strict) {
            assert !containsCalculatedMember(members) :
                    "can not restrict SQL to calculated Members";
        } else {
            members = removeCalculatedMembers(members);
        }

        CellRequest request =
                RolapAggregationManager.makeRequest(members, false, false);
        if (request == null) {
            assert !strict : "CellRequest is null - why?";
            // One or more of the members was null or calculated, so the
            // request is impossible to satisfy.
            return;
        }
        RolapStar.Column[] columns = request.getConstrainedColumns();
        Object[] values = request.getSingleValues();
        int arity = columns.length;
        // following code is similar to AbsractQuerySpec#nonDistinctGenerateSQL()
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

            String value = sqlQuery.quote(column.isNumeric(), values[i]);
            sqlQuery.addWhere(expr,
                    RolapUtil.sqlNullLiteral.equals(value) ? " is " : " = ",
                    value);
        }
    }

    private static Member[] removeCalculatedMembers(Member[] members) {
        List result = new ArrayList();
        for (int i = 0; i < members.length; i++) {
            if (!members[i].isCalculated()) {
                result.add(members[i]);
            }
        }
        return (Member[]) result.toArray(new Member[result.size()]);
    }

    private static boolean containsCalculatedMember(Member[] members) {
        for (int i = 0; i < members.length; i++) {
            if (members[i].isCalculated()) {
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
        SqlQuery sqlQuery, AggStar aggStar, Evaluator e, RolapLevel level,
        Map levelToColumnMap)
    {
        RolapCube cube = (RolapCube) e.getCube();
        Map mapLevelToColumnMap;
        if (cube.isVirtual()) {
            mapLevelToColumnMap = levelToColumnMap;
        } else {
            RolapStar star = cube.getStar();
            mapLevelToColumnMap = star.getMapLevelToColumn(cube);
        }
        RolapStar.Column starColumn =
            (RolapStar.Column) mapLevelToColumnMap.get(level);
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
     * creates a WHERE parent = value
     * @param sqlQuery the query to modify
     * @param aggStar Definition of the aggregate table, or null if query is
     * @param parent the list of parent members
     * @param strict defines the behavior if <code>parent</code>
     * is a calculated member. If true, an exception is thrown
     */
    public static void addMemberConstraint(
        SqlQuery sqlQuery, AggStar aggStar, RolapMember parent, boolean strict)
    {
        List list = Collections.singletonList(parent);
        addMemberConstraint(sqlQuery, aggStar, list, strict);
    }

    /**
     * Creates a "WHERE exp IN (...)" condition containing the values
     * of all parents. All parents must belong to the same level.
     *
     * @param sqlQuery the query to modify
     * @param aggStar
     * @param parents the list of parent members
     * @param strict defines the behavior if <code>parents</code>
     *   contains calculated members.
     *   If true, an exception is thrown,
     */
    public static void addMemberConstraint(
        SqlQuery sqlQuery, AggStar aggStar, List parents, boolean strict)
    {
        if (parents.size() == 0) {
            return;
        }
        for (Collection c = parents; !c.isEmpty(); c = getUniqueParentMembers(c)) {
            RolapMember m = (RolapMember) c.iterator().next();
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
            RolapLevel level = m.getRolapLevel();
            RolapHierarchy hierarchy = (RolapHierarchy) level.getHierarchy();
            hierarchy.addToFrom(sqlQuery, level.getKeyExp());
            String q = level.getKeyExp().getExpression(sqlQuery);
            ColumnConstraint[] cc = getColumnConstraints(c);

            // ORA-01795: maximum number of expressions in a list is 1000
            // It is rumored that for some versions of Oracle the maximum number
            // is 255.
            final int ORACLE_MAX_EXPRSSIONS = 1000;
            if (sqlQuery.getDialect().isOracle() && 
                    cc.length >= ORACLE_MAX_EXPRSSIONS) {

                // Simply get them all, do not create where-clause.
                // Below are two alternative approaches (and code). They
                // both have problems.
                
/*
                // Approach 1
                // Convert the single, "q in ( .... )", clause into
                // a union of individual selects
                // "q in ((select 'a' from dual) union 
                //    (select 'b' from dual) union ... )"
                // where 'a', 'b', etc. are the individual values.
                // In this case, Oracle requires a very large shared memory
                // pool (ORA-04031: unable to allocate nnn bytes of 
                // shared memory) and so this is not very reasonable.
                
                // NOTE: the following produces legal Oracle SQL, but
                // Oracle requires very large shared memory buffer.
                StringBuffer buf = new StringBuffer(cc.length);
                buf.append(q);
                buf.append(" in (");
                // NOTE: this would have to be fixed to account for nulls, etc
                // as the code in RolapStar.Column.createInExpr does.
                for (int i = 0; i < cc.length; i++) {
                    ColumnConstraint constraint = cc[i];
                    Object key = constraint.getValue();
                    if (i > 0) {
                        buf.append(" union ");
                    }
                    buf.append("(select ");
                    if (level.isNumeric()) {
                        buf.append(key);
                    } else {
                        Util.singleQuoteString(key.toString(), buf);
                    }
                    buf.append(" from dual)");
                }
                buf.append(')');
                sqlQuery.addWhere(buf.toString());
*/
/*
                // Approach 2
                // Convert the single, "q in ( .... )", clause into
                // a set of "or-ed" clauses, 
                // "q in ( ...) or ... or q in ( ...)"
                // Oracle can not handle more than 1000 items in an 
                // expression list.
                
                // NOTE: the following produces legal Oracle SQL, but
                // when one has, say, 11108 column contraints which yield
                // 12 different parts to the "or-ed" clause, the Oracle 
                // DB does not return after 30 minutes. On the other hand,
                // if one simply removes the where-clause (do not create
                // it), the Oracle returns in less than 0.4 seconds.
                
                // multiple in-clauses or-ed together
                int ccs = cc.length/ORACLE_MAX_EXPRSSIONS;
                int ccx = cc.length - (ccs * ORACLE_MAX_EXPRSSIONS);
                // the length will clearly be bigger than cc.length but its a 
                // good starting point
                StringBuffer buf = new StringBuffer(cc.length);
                ColumnConstraint[] tmpcc = new ColumnConstraint[ORACLE_MAX_EXPRSSIONS];
                for (int i = 0; i < ccs; i++) {
                    System.arraycopy(cc, i * ORACLE_MAX_EXPRSSIONS, tmpcc, 0, ORACLE_MAX_EXPRSSIONS);
                    String cond = RolapStar.Column.createInExpr(q, tmpcc, level.isNumeric());
                    if (i != 0) {
                        buf.append(" or ");
                    }
                    buf.append(cond);
                }
                if (ccx > 0) {
                    buf.append(" or ");
                    tmpcc = new ColumnConstraint[ccx];
                    System.arraycopy(cc, ccs * ORACLE_MAX_EXPRSSIONS, tmpcc, 0, ccx);
                    String cond = RolapStar.Column.createInExpr(q, tmpcc, level.isNumeric());
                    buf.append(cond);
                } 

                sqlQuery.addWhere(buf.toString());
*/

            } else {
                String cond = RolapStar.Column.createInExpr(q, cc, level.isNumeric());
                sqlQuery.addWhere(cond);
            }
            if (level.isUnique()) {
                break; // no further qualification needed
            }
        }

    }

    private static ColumnConstraint[] getColumnConstraints(Collection members) {
        ColumnConstraint[] constraints = new ColumnConstraint[members.size()];
        Iterator it = members.iterator();
        for (int i = 0; i < constraints.length; i++) {
            RolapMember m = (RolapMember) it.next();
            constraints[i] = new MemberColumnConstraint(m);
        }
        return constraints;
    }

    private static Collection getUniqueParentMembers(Collection members) {
        Set set = new HashSet();
        for (Iterator it = members.iterator(); it.hasNext();) {
            RolapMember m = (RolapMember) it.next();
            m = (RolapMember) m.getParentMember();
            if (m != null) {
                set.add(m);
            }
        }
        return set;
    }
}

// End SqlConstraintUtils.java
