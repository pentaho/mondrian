/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.aggmatcher.AggStar;
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
public class HighCardSqlTupleReader extends SqlTupleReader {
    private ResultLoader resultLoader;
    private boolean moreRows;

    private final List<Target> targets = new ArrayList<Target>();
    int maxRows = 0;

    public HighCardSqlTupleReader(final TupleConstraint constraint) {
        super(constraint);
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
                if (this.moreRows) {
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
        } catch (SQLException sqle) {
            if (stmt != null) {
                throw stmt.handle(sqle);
            } else {
                throw Util.newError(sqle, message);
            }
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
        final List<RolapMember>[] lists = new List[n];
        for (int i = 0; i < n; i++) {
            lists[i] = targets.get(i).close();
        }

        final List<RolapMember[]> tupleList =
            new TraversalList<RolapMember>(lists, RolapMember.class);

        // need to hierarchize the columns from the enumerated targets
        // since we didn't necessarily add them in the order in which
        // they originally appeared in the cross product
        int enumTargetCount = getEnumTargetCount();
        if (enumTargetCount > 0) {
            FunUtil.hierarchizeTupleList(
                Util.<Member[]>cast(tupleList), false, n);
        }
        return tupleList;
    }

    /**
     * Avoids loading of more results.
     */
    public void noMoreRows() {
        this.moreRows = false;
    }

    /**
     * Reads next tuple notifing all internal targets.
     *
     * @return whether there are any more rows
     */
    public boolean readNextTuple() {
        if (!this.moreRows) {
            return false;
        }
        try {
            this.moreRows = this.resultLoader.loadResult();
        } catch (SQLException sqle) {
            this.moreRows = false;
            throw this.resultLoader.handle(sqle);
        }
        if (!this.moreRows) {
            this.resultLoader.close();
        }
        return this.moreRows;
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
            final List<RolapCube> baseCubes = query.getBaseCubes();

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
        WhichSelect whichSelect)
    {
        String s = "while generating query to retrieve members of level(s) "
                + targets;
        SqlQuery sqlQuery = SqlQuery.newQuery(dataSource, s);

        Evaluator evaluator = getEvaluator(constraint);
        AggStar aggStar = chooseAggStar(evaluator);

        for (Target target : targets) {
            if (target.getSrcMembers() == null) {
                addLevelMemberSql(
                    sqlQuery,
                    target.getLevel(),
                    baseCube,
                    whichSelect,
                    aggStar);
            }
        }

        // additional constraints
        constraint.addConstraint(sqlQuery, baseCube, aggStar);

        return sqlQuery.toString();
    }

    /**
     * Obtain the AggStar instance which corresponds to an aggregate table
     * which can be used to support the member constraint
     * @param evaluator the current evaluator to obtain the cube and members to be queried
     * @return AggStar for aggregate table
     */
    private AggStar chooseAggStar(Evaluator evaluator) {
        if (evaluator == null) {
            return null;
        }

        // Current cannot support aggregate tables for virtual cubes
        RolapCube cube = (RolapCube) evaluator.getCube();
        if (cube.isVirtual()) {
            return null;
        }

        RolapStar star = cube.getStar();
        final int starColumnCount = star.getColumnCount();
        BitKey measureBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        BitKey levelBitKey = BitKey.Factory.makeBitKey(starColumnCount);

        // Convert global ordinal to cube based ordinal (the 0th dimension
        // is always [Measures]). In the case of filter constraint this will
        // be the measure on which the filter will be done.
        final Member[] members = evaluator.getMembers();

        // if measure is calculated, we can't continue
        if (!(members[0] instanceof RolapBaseCubeMeasure)) {
            return null;
        }

        RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)members[0];

        int bitPosition = ((RolapStar.Measure)measure.getStarMeasure()).getBitPosition();

        // set a bit for each level which is constrained in the context
        final CellRequest request =
            RolapAggregationManager.makeRequest(members);
        if (request == null) {
            // One or more calculated members. Cannot use agg table.
            return null;
        }
        // TODO: RME why is this using the array of constrained columns
        // from the CellRequest rather than just the constrained columns
        // BitKey (method getConstrainedColumnsBitKey)?
        RolapStar.Column[] columns = request.getConstrainedColumns();
        for (RolapStar.Column column1 : columns) {
            levelBitKey.set(column1.getBitPosition());
        }

        // set the masks
        for (Target target : targets) {
            RolapLevel level = target.getLevel();
            if (!level.isAll()) {
                RolapStar.Column column = ((RolapCubeLevel)level).getStarKeyColumn();
                levelBitKey.set(column.getBitPosition());
            }
        }

        measureBitKey.set(bitPosition);

        // find the aggstar using the masks
        AggStar aggStar = AggregationManager.instance().findAgg(
            star, levelBitKey, measureBitKey, new boolean[]{ false });

        if (aggStar == null) {
            return null;
        }

        // determine if any collapsed levels contain more than one column, if
        // so, the aggStar is not compatible.
        //
        // In the future, this feature could be improved by:
        // 1. changing the sql generation to join the collapsed level to its
        // dimension table(s) to select the additional columns.
        // 2. Create members that are missing these values and populate the
        // values at a later time.
        // 3. extend agg tables to support additional level columns

        for (Target target : targets) {
            RolapLevel level = target.getLevel();
            if (!level.isAll()) {
                if (isLevelCollapsed(aggStar, (RolapCubeLevel)level) &&
                    levelContainsMultipleColumns(level)) {
                    return null;
                }
            }
        }

        return aggStar;
    }
}

// End HighCardSqlTupleReader.java
