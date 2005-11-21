/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
 */
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Util;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.ColumnConstraint;
import mondrian.rolap.sql.SqlQuery;

/**
 * Utility class used by implementations of {@link mondrian.rolap.sql.SqlConstraint},
 * used to generate constraints into {@link mondrian.rolap.sql.SqlQuery}.
 *  
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintUtils {

    /** Utility class */
    private SqlConstraintUtils() {
    }

    /**
     * for every restricting member in the current context an WHERE condition
     * and a join to the fact table is generated.
     * 
     * @param sqlQuery the query to modify
     * @param parent the list of parent members
     * @param strict defines the behaviour if the current context contains 
     * calculated members. 
     * If true, an exception is thrown,
     * otherwise calculated members are silently ignored.
     */
    public static void addContextConstraint(SqlQuery sqlQuery, Evaluator e, boolean strict) {
        
        Member[] members = e.getMembers();
        if (strict) {
            assert !containsCalculatedMember(members) : "addContextConstraint: can not restrict SQL to calculated Members";
        }
        else {
            members = removeCalculatedMembers(members);
        }
            
        CellRequest request = RolapAggregationManager.makeRequest(members, false);
        RolapStar.Column[] columns = request.getColumns();
        Object[] values = request.getSingleValues();
        int arity = columns.length;
        // following code is similar to AbsractQuerySpec#nonDistinctGenerateSQL()
        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];
            RolapStar.Table table = column.getTable();
            table.addToFrom(sqlQuery, false, true);

            String expr = column.getExpression(sqlQuery);
            String value = sqlQuery.quote(column.isNumeric(), values[i]);
            sqlQuery.addWhere(expr, " = ", value);
        }
    }

    private static Member[] removeCalculatedMembers(Member[] members) {
        List result = new ArrayList();
        for (int i = 0; i < members.length; i++)
            if (!members[i].isCalculated())
                result.add(members[i]);
        return (Member[]) result.toArray(new Member[result.size()]);
    }
    
    private static boolean containsCalculatedMember(Member[] members) {
        for (int i = 0; i < members.length; i++)
            if (members[i].isCalculated())
                return true;
        return false;
    }

    /**
     * ensures that the table of <code>level</code> is joined to the fact table
     */
    public static void joinLevelTableToFactTable(SqlQuery sqlQuery, RolapCube cube,
            RolapLevel level) {
        RolapStar star = cube.getStar();
        Map mapLevelToColumnMap = star.getMapLevelToColumn(cube);
        RolapStar.Column starColumn = (RolapStar.Column) mapLevelToColumnMap.get(level);
        RolapStar.Table table = starColumn.getTable();
        table.addToFrom(sqlQuery, false, true);
    }

    /**
     * creates a WHERE parent = value
     * @param sqlQuery the query to modify
     * @param parent the list of parent members
     * @param strict defines the behaviour if <code>parent</code> 
     * is a calculated member. 
     * If true, an exception is thrown,
     * otherwise calculated members are silently ignored.
     */
    public static void addMemberConstraint(SqlQuery sqlQuery, RolapMember parent, boolean strict) {
        List list = new ArrayList(1);
        list.add(parent);
        addMemberConstraint(sqlQuery, list, strict);
    }

    /**
     * creates a "WHERE exp IN (...)" condition containing the values
     * of all parents. All parents must belong to the same level.
     * 
     * @param sqlQuery the query to modify
     * @param parent the list of parent members
     * @param strict defines the behaviour if <code>parents</code> 
     * contains calculated members. 
     * If true, an exception is thrown,
     * otherwise calculated members are silently ignored.
     */
    public static void addMemberConstraint(SqlQuery sqlQuery, List parents, boolean strict) {
        if (parents.size() == 0)
            return;

        for (Collection c = parents; !c.isEmpty(); c = getUniqueParentMembers(c)) {
            RolapMember m = (RolapMember) c.iterator().next();
            if (m.isAll())
                continue;
            if (m.isCalculated()) {
                if (strict)
                  throw Util.newInternal("addMemberConstraint: cant restrict SQL to calculated member");
                continue;
            }
            RolapLevel level = m.getRolapLevel();
            RolapHierarchy hierarchy = (RolapHierarchy) level.getHierarchy();
            hierarchy.addToFrom(sqlQuery, level.getKeyExp());
            String q = level.getKeyExp().getExpression(sqlQuery);
            ColumnConstraint[] cc = getColumnConstraints(c);
            String cond = RolapStar.Column.createInExpr(q, cc, level.isNumeric());
            sqlQuery.addWhere(cond);
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
            constraints[i] = new ColumnConstraint(m);
        }
        return constraints;
    }

    private static Collection getUniqueParentMembers(Collection members) {
        Set set = new HashSet();
        for (Iterator it = members.iterator(); it.hasNext();) {
            RolapMember m = (RolapMember) it.next();
            m = (RolapMember) m.getParentMember();
            if (m != null)
                set.add(m);
        }
        return set;
    }
}
