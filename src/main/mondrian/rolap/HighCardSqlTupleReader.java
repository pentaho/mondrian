/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Member;
import mondrian.olap.Query;
import mondrian.olap.Util;
import mondrian.olap.fun.FunUtil;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.util.TraversalList;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Reads the members of a single level (level.members) or of multiple levels
 * (crossjoin).
 *
 * @author luis f. canals
 * @version $Id$
 * @since Dec, 2007
 */
public class HighCardSqlTupleReader extends SqlTupleReader {
    private ResultLoader resultLoader;
    private boolean moreRows;

    int maxRows = 0;

    public HighCardSqlTupleReader(final TupleConstraint constraint) {
        super(constraint);
    }

    public void addLevelMembers(
        final RolapLevel level,
        final MemberBuilder memberBuilder,
        final List<RolapMember> srcMembers)
    {
        targets.add(new Target(
            level, memberBuilder, srcMembers, constraint, this));
    }

    protected void prepareTuples(
        final DataSource dataSource,
        final List<List<RolapMember>> partialResult,
        final List<List<RolapMember>> newPartialResult)
    {
        String message = "Populating member cache with members for " + targets;
        SqlStatement stmt = null;
        boolean execQuery = (partialResult == null);
        try {
            if (execQuery) {
                // we're only reading tuples from the targets that are
                // non-enum targets
                List<TargetBase> partialTargets = new ArrayList<TargetBase>();
                for (TargetBase target : targets) {
                    if (target.getSrcMembers() == null) {
                        partialTargets.add(target);
                    }
                }
                String sql = makeLevelMembersSql(dataSource);
                stmt = RolapUtil.executeQuery(
                    dataSource, sql, maxRows, 0,
                    "HighCardSqlTupleReader.readTuples " + partialTargets,
                    message, -1, -1);
            }

            for (TargetBase target : targets) {
                target.open();
            }

            // determine how many enum targets we have
            int enumTargetCount = getEnumTargetCount();

            int currPartialResultIdx = 0;
            if (execQuery) {
                this.moreRows = stmt.getResultSet().next();
                if (this.moreRows) {
                    ++stmt.rowCount;
                }
            } else {
                this.moreRows = currPartialResultIdx < partialResult.size();
            }

            this.resultLoader =
                new ResultLoader(
                    enumTargetCount,
                    targets, stmt, execQuery, partialResult,
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
        } finally {
            if (!moreRows) {
                if (stmt != null) {
                    stmt.close();
                }
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
        @SuppressWarnings({"unchecked"})
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

    Collection<RolapCube> getBaseCubeCollection(final Query query) {
        return query.getBaseCubes();
    }
}
// End HighCardSqlTupleReader.java
