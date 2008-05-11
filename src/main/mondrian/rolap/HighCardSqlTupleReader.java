/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.util.*;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;

/**
 * Reads the members of a single level (level.members) or of multiple levels
 * (crossjoin).
 *
 * @author luis f. canals
 * @since Dec, 2007
 * @version $Id$
 */
public class HighCardSqlTupleReader implements TupleReader {
    private ResultLoader resultLoader;
    private boolean moreRows;

    private final TupleConstraint constraint;
    private final List<Target> targets = new ArrayList<Target>();
    int maxRows = 0;

    public HighCardSqlTupleReader(final TupleConstraint constraint) {
        this.constraint = constraint;
    }

    public void addLevelMembers(
        final RolapLevel level,
        final MemberBuilder memberBuilder,
        final List<RolapMember> srcMembers)
    {
        targets.add(
            new Target(level, memberBuilder, srcMembers, constraint, this));
    }

    public Object getCacheKey() {
        List<Object> key = new ArrayList<Object>();
        key.add(constraint.getCacheKey());
        key.add(SqlTupleReader.class);
        for (Target target : targets) {
            // don't include the level in the key if the target isn't
            // processed through native sql
            if (target.getSrcMembers() != null) {
                key.add(target.getLevel());
            }
        }
        return key;
    }

    /**
     * @return number of targets that contain enumerated sets with calculated
     * members
     */
    public int getEnumTargetCount() {
        int enumTargetCount = 0;
        for (Target target : targets) {
            if (target.getSrcMembers() != null) {
                enumTargetCount++;
            }
        }
        return enumTargetCount;
    }

    protected void prepareTuples(
        final DataSource dataSource,
        final List<List<RolapMember>> partialResult,
        final List<List<RolapMember>> newPartialResult)
    {
        String message = "Populating member cache with members for " + targets;
        SqlStatement stmt = null;
        final ResultSet resultSet;
        boolean execQuery = (partialResult == null);
        try {
            if (execQuery) {
                // we're only reading tuples from the targets that are
                // non-enum targets
                List<Target> partialTargets = new ArrayList<Target>();
                for (Target target : targets) {
                    if (target.getSrcMembers() == null) {
                        partialTargets.add(target);
                    }
                }
                String sql = makeLevelMembersSql(dataSource);
                stmt = RolapUtil.executeQuery(dataSource, sql, maxRows,
                        "HighCardSqlTupleReader.readTuples " + partialTargets,
                        message, -1, -1);
                resultSet = stmt.getResultSet();
            } else {
                resultSet = null;
            }

            for (Target target : targets) {
                target.open();
            }

            // determine how many enum targets we have
            int enumTargetCount = getEnumTargetCount();
            int[] srcMemberIdxes = null;
            if (enumTargetCount > 0) {
                srcMemberIdxes = new int[enumTargetCount];
            }

            int currPartialResultIdx = 0;
            if (execQuery) {
                this.moreRows = resultSet.next();
                if(this.moreRows) {
                    ++stmt.rowCount;
                }
            } else {
                this.moreRows = currPartialResultIdx < partialResult.size();
            }

            this.resultLoader = new ResultLoader(enumTargetCount,
                    targets, stmt, resultSet, execQuery, partialResult,
                    newPartialResult);

            // Read first and second elements if exists (or marks
            // source as having "no more rows")
            readNextTuple();
            readNextTuple();
        } catch(SQLException sqle) {
            if(stmt!=null) stmt.handle(sqle);
            else throw Util.newError(sqle, message);
        }
    }

    public List<RolapMember> readMembers(
        final DataSource dataSource,
        final List<List<RolapMember>> partialResult,
        final List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(dataSource, partialResult, newPartialResult);
        assert targets.size() == 1;
        return targets.get(0).close();
    }

    public List<RolapMember[]> readTuples(
        final DataSource jdbcConnection,
        final List<List<RolapMember>> partialResult,
        final List<List<RolapMember>> newPartialResult)
    {
        prepareTuples(jdbcConnection, partialResult, newPartialResult);

        // List of tuples
        final int n = targets.size();
        final List<Member>[] lists = new List[n];
        for (int i = 0; i < n; i++) {
            lists[i] = (List) targets.get(i).close();
        }

        final List<Member[]> tupleList =
            new TraversalList<Member>(lists, Member.class);

        // need to hierarchize the columns from the enumerated targets
        // since we didn't necessarily add them in the order in which
        // they originally appeared in the cross product
        int enumTargetCount = getEnumTargetCount();
        if (enumTargetCount > 0) {
            FunUtil.hierarchize(tupleList, false);
        }
        return (List) tupleList;
    }

    /**
     * Avoids loading of more results.
     */
    public void noMoreRows() {
        this.moreRows = false;
    }

    /**
     * Reads next tuple notifing all internal targets.
     */
    public boolean readNextTuple() {
        if (!this.moreRows) {
            return false;
        } else {
            try {
                if (this.moreRows) {
                    this.moreRows = this.resultLoader.loadResult();
                }
            } catch (SQLException sqle) {
                this.resultLoader.handle(sqle);
                this.moreRows = false;
            }
            if (!this.moreRows) {
                this.resultLoader.close();
            }
            return this.moreRows;
        }
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public int getMaxRows() {
        return maxRows;
    }

    //
    // Private stuff ------------------------------------------
    //

    private String makeLevelMembersSql(DataSource dataSource) {
        // In the case of a virtual cube, if we need to join to the fact
        // table, we do not necessarily have a single underlying fact table,
        // as the underlying base cubes in the virtual cube may all reference
        // different fact tables.
        //
        // Therefore, we need to gather the underlying fact tables by going
        // through the list of measures referenced in the query.  And then
        // we generate one sub-select per fact table, joining against each
        // underlying fact table, unioning the sub-selects.
        RolapCube cube = null;
        boolean virtualCube = false;
        if (constraint instanceof SqlContextConstraint) {
            SqlContextConstraint sqlConstraint =
                (SqlContextConstraint) constraint;
            if (sqlConstraint.isJoinRequired()) {
                Query query = constraint.getEvaluator().getQuery();
                cube = (RolapCube) query.getCube();
                virtualCube = cube.isVirtual();
            }
        }

        if (virtualCube) {
            final StringBuffer selectString = new StringBuffer();
            final Query query = constraint.getEvaluator().getQuery();
            final Set<RolapCube> baseCubes = query.getBaseCubes();

            int k = -1;
            for (RolapCube baseCube : baseCubes) {
                boolean finalSelect = (++k == baseCubes.size() - 1);
                WhichSelect whichSelect =
                    finalSelect ? WhichSelect.LAST : WhichSelect.NOT_LAST;
                selectString.append(
                    generateSelectForLevels(dataSource, baseCube, whichSelect));
                if (!finalSelect) {
                    selectString.append(" union ");
                }
            }
            return selectString.toString();
        } else {
            return generateSelectForLevels(dataSource, cube, WhichSelect.ONLY);
        }
    }

    /**
     * Generates the SQL string corresponding to the levels referenced.
     *
     * @param dataSource jdbc connection that they query will execute
     * against
     * @param baseCube this is the cube object for regular cubes, and the
     *   underlying base cube for virtual cubes
     * @param whichSelect Position of this select statement in a union
     * @return SQL statement string
     */
    private String generateSelectForLevels(
        DataSource dataSource,
        RolapCube baseCube,
        WhichSelect whichSelect) {

        String s = "while generating query to retrieve members of level(s) "
                + targets;
        SqlQuery sqlQuery = SqlQuery.newQuery(dataSource, s);

        for (Target target : targets) {
            if (target.getSrcMembers() == null) {
                addLevelMemberSql(
                    sqlQuery,
                    target.getLevel(),
                    baseCube,
                    whichSelect);
            }
        }

        // additional constraints
        constraint.addConstraint(sqlQuery, baseCube);

        return sqlQuery.toString();
    }

    /**
     * Generates the SQL statement to access members of <code>level</code>. For
     * example, <blockquote>
     * <pre>SELECT "country", "state_province", "city"
     * FROM "customer"
     * GROUP BY "country", "state_province", "city", "init", "bar"
     * ORDER BY "country", "state_province", "city"</pre>
     * </blockquote> accesses the "City" level of the "Customers"
     * hierarchy. Note that:<ul>
     *
     * <li><code>"country", "state_province"</code> are the parent keys;</li>
     *
     * <li><code>"city"</code> is the level key;</li>
     *
     * <li><code>"init", "bar"</code> are member properties.</li>
     * </ul>
     *
     * @param sqlQuery the query object being constructed
     * @param level level to be added to the sql query
     * @param baseCube this is the cube object for regular cubes, and the
     *   underlying base cube for virtual cubes
     * @param whichSelect describes whether this select belongs to a larger
     * select containing unions or this is a non-union select
     */
    private void addLevelMemberSql(
        SqlQuery sqlQuery,
        RolapLevel level,
        RolapCube baseCube,
        WhichSelect whichSelect)
    {
        RolapHierarchy hierarchy = level.getHierarchy();

        // lookup RolapHierarchy of base cube that matches this hierarchy
        if (hierarchy instanceof RolapCubeHierarchy) {
            RolapCubeHierarchy cubeHierarchy = (RolapCubeHierarchy)hierarchy;
            if (baseCube != null
                && !cubeHierarchy.getDimension().getCube().equals(baseCube)) {
                hierarchy = baseCube.findBaseCubeHierarchy(hierarchy);
            }
        }

        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int levelDepth = level.getDepth();
        for (int i = 0; i <= levelDepth; i++) {
            RolapLevel currLevel = levels[i];
            if (currLevel.isAll()) {
                continue;
            }

            MondrianDef.Expression keyExp = currLevel.getKeyExp();
            MondrianDef.Expression ordinalExp = currLevel.getOrdinalExp();
            MondrianDef.Expression captionExp = currLevel.getCaptionExp();

            String keySql = keyExp.getExpression(sqlQuery);
            String ordinalSql = ordinalExp.getExpression(sqlQuery);

            hierarchy.addToFrom(sqlQuery, keyExp);
            hierarchy.addToFrom(sqlQuery, ordinalExp);

            String captionSql = null;
            if (captionExp != null) {
                captionSql = captionExp.getExpression(sqlQuery);
                hierarchy.addToFrom(sqlQuery, captionExp);
            }

            sqlQuery.addSelect(keySql);
            sqlQuery.addGroupBy(keySql);

            if (!ordinalSql.equals(keySql)) {
                sqlQuery.addSelect(ordinalSql);
                sqlQuery.addGroupBy(ordinalSql);
            }

            if (captionSql != null) {
                sqlQuery.addSelect(captionSql);
                sqlQuery.addGroupBy(captionSql);
            }

            constraint.addLevelConstraint(sqlQuery, baseCube, null, currLevel);

            // If this is a select on a virtual cube, the query will be
            // a union, so the order by columns need to be numbers,
            // not column name strings or expressions.
            switch (whichSelect) {
            case LAST:
                sqlQuery.addOrderBy(
                    Integer.toString(
                        sqlQuery.getCurrentSelectListSize()),
                        true, false, true);
                break;
            case ONLY:
                sqlQuery.addOrderBy(ordinalSql, true, false, true);
                break;
            }

            RolapProperty[] properties = currLevel.getProperties();
            for (RolapProperty property : properties) {
                String propSql = property.getExp().getExpression(sqlQuery);
                sqlQuery.addSelect(propSql);
                sqlQuery.addGroupBy(propSql);
            }
        }
    }

    /**
     * Description of the position of a SELECT statement in a UNION. Queries
     * on virtual cubes tend to generate unions.
     */
    enum WhichSelect {
        /**
         * Select statement does not belong to a union.
         */
        ONLY,
        /**
         * Select statement belongs to a UNION, but is not the last. Typically
         * this occurs when querying a virtual cube.
         */
        NOT_LAST,
        /**
         * Select statement is the last in a UNION. Typically
         * this occurs when querying a virtual cube.
         */
        LAST
    }
}

// End HighCardSqlTupleReader.java
