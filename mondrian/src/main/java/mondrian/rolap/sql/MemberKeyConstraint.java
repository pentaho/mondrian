/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.rolap.sql;

import mondrian.olap.Evaluator;
import mondrian.olap.MondrianDef;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;
import mondrian.util.Pair;

import java.util.List;

/**
 * Restricts the SQL result set to members where particular columns have
 * particular values.
 *
 * @version $Id$
 */
public class MemberKeyConstraint
    implements TupleConstraint
{
    private final Pair<List<MondrianDef.Expression>, List<Comparable>> cacheKey;
    private final List<MondrianDef.Expression> columnList;
    private final List<Dialect.Datatype> datatypeList;
    private final List<Comparable> valueList;

    public MemberKeyConstraint(
        List<MondrianDef.Expression> columnList,
        List<Dialect.Datatype> datatypeList,
        List<Comparable> valueList)
    {
        this.columnList = columnList;
        this.datatypeList = datatypeList;
        this.valueList = valueList;
        cacheKey = Pair.of(columnList, valueList);
    }

    public void addConstraint(
        SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar)
    {
        for (int i = 0; i < columnList.size(); i++) {
            MondrianDef.Expression expression = columnList.get(i);
            final Comparable value = valueList.get(i);
            final Dialect.Datatype datatype = datatypeList.get(i);
            sqlQuery.addWhere(
                SqlConstraintUtils.constrainLevel2(
                    sqlQuery,
                    expression,
                    datatype,
                    value));
        }
    }

    public void addLevelConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level)
    {
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        RolapMember parent)
    {
        return null;
    }

    public String toString() {
        return "MemberKeyConstraint";
    }


    public Object getCacheKey() {
        return cacheKey;
    }

    public Evaluator getEvaluator() {
        return null;
    }

    @Override
    public boolean supportsAggTables() {
        return true;
    }
}

// End MemberKeyConstraint.java
