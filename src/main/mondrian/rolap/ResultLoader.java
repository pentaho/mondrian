/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
//
// jhyde, Feb 21, 2003
*/

package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.olap.Util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loader to be iterated to load all results from database.
 *
 * @author luis f. canals
 */
public class ResultLoader {
    private final List<TargetBase> targets;
    private final int enumTargetCount;
    private final boolean execQuery;
    private final String message;
    private final TupleList partialResult;
    private final List<List<RolapMember>> newPartialResult;
    private final SqlStatement stmt;

    private final int[] srcMemberIdxes;

    int currPartialResultIdx = 0;

    public ResultLoader(
        final int enumTargetCount,
        final List<TargetBase> targets,
        final SqlStatement stmt,
        final boolean execQuery,
        final TupleList partialResult,
        final List<List<RolapMember>> newPartialResult)
        throws SQLException
    {
        assert (stmt != null) == execQuery;
        this.targets = targets;
        this.enumTargetCount = enumTargetCount;
        this.stmt = stmt;
        this.execQuery = execQuery;
        this.partialResult = partialResult;
        this.newPartialResult = newPartialResult;
        this.srcMemberIdxes =
            enumTargetCount > 0
                ? new int[enumTargetCount]
                : null;
        this.message = "Populating member cache with members for " + targets;
    }


    public boolean loadResult() throws SQLException {
/*
        if (limit > 0 && limit < ++fetchCount) {
            throw MondrianResource.instance().MemberFetchLimitExceeded
                    .ex((long) limit);
        }
*/
        if (enumTargetCount == 0) {
            int column = 0;
            for (TargetBase target : targets) {
                target.removeCurrMember();
                column = target.addRow(stmt, column);
            }
        } else {
            int firstEnumTarget = 0;
            for (; firstEnumTarget < targets.size(); firstEnumTarget++) {
                if (targets.get(firstEnumTarget).getSrcMembers() != null) {
                    break;
                }
            }
            List<RolapMember> partialRow;
            if (execQuery) {
                partialRow = null;
            } else {
                partialRow = Util.cast(partialResult.get(currPartialResultIdx));
            }
            resetCurrMembers(partialRow);
            addTargets(
                0, firstEnumTarget, enumTargetCount, srcMemberIdxes, message);
            if (newPartialResult != null) {
                savePartialResult(newPartialResult);
            }
        }

        boolean moreRows;
        if (execQuery) {
            moreRows = stmt.getResultSet().next();
            if (moreRows) {
                ++stmt.rowCount;
            }
        } else {
            currPartialResultIdx++;
            moreRows = currPartialResultIdx < partialResult.size();
        }
        return moreRows;
    }


    /**
     * Closes internal statement.
     */
    public void close() {
        if (this.stmt != null) {
            this.stmt.close();
        }
    }


    /**
     * Handles an error, and returns an exception that the caller should then
     * throw.
     *
     * @param e Exception
     * @return Wrapper exception
     */
    public RuntimeException handle(Exception e) {
        if (stmt != null) {
            return stmt.handle(e);
        } else {
            return Util.newError(e, message);
        }
    }

    //
    // Private stuff -------------------------------
    //

    /**
     * Sets the current member for those targets that retrieve their column
     * values from native sql.
     *
     * @param partialRow if set, previously cached result set
     */
    private void resetCurrMembers(List<RolapMember> partialRow) {
        int nativeTarget = 0;
        for (TargetBase target : targets) {
            if (target.getSrcMembers() == null) {
                if (partialRow != null) {
                    target.setCurrMember(partialRow.get(nativeTarget++));
                } else {
                    target.removeCurrMember();
                }
            }
        }
    }

    /**
     * Recursively forms the cross product of a row retrieved through sql
     * with each of the targets that contains an enumerated set of members.
     *
     * @param currEnumTargetIdx current enum target that recursion
     * is being applied on
     * @param currTargetIdx index within the list of a targets that
     * currEnumTargetIdx corresponds to
     * @param nEnumTargets number of targets that have enumerated members
     * @param srcMemberIdxes for each enumerated target, the current member
     * to be retrieved to form the current cross product row
     * @param message Message to issue on failure
     */
    private void addTargets(
        int currEnumTargetIdx,
        int currTargetIdx,
        int nEnumTargets,
        int[] srcMemberIdxes,
        String message)
    {
        TargetBase currTarget = targets.get(currTargetIdx);
        for (int i = 0; i < currTarget.getSrcMembers().size(); i++) {
            srcMemberIdxes[currEnumTargetIdx] = i;
            if (currEnumTargetIdx < nEnumTargets - 1) {
                int nextTargetIdx = currTargetIdx + 1;
                for (; nextTargetIdx < targets.size(); nextTargetIdx++) {
                    if (targets.get(nextTargetIdx).getSrcMembers() != null) {
                        break;
                    }
                }
                addTargets(
                    currEnumTargetIdx + 1, nextTargetIdx, nEnumTargets,
                    srcMemberIdxes, message);
            } else {
                int column = 0;
                int enumTargetIdx = 0;
                for (TargetBase target : targets) {
                    if (target.getSrcMembers() == null) {
                        try {
                            column = target.addRow(stmt, column);
                        } catch (Throwable e) {
                            throw Util.newError(e, message);
                        }
                    } else {
                        RolapMember member =
                            target.getSrcMembers().get(
                                srcMemberIdxes[enumTargetIdx++]);
                        target.add(member);
                    }
                }
            }
        }
    }

    /**
     * Retrieves the current members fetched from the targets executed
     * through sql and form tuples, adding them to partialResult
     *
     * @param partialResult list containing the columns and rows corresponding
     * to data fetched through sql
     */
    private void savePartialResult(List<List<RolapMember>> partialResult) {
        List<RolapMember> row = new ArrayList<RolapMember>();
        for (TargetBase target : targets) {
            if (target.getSrcMembers() == null) {
                row.add(target.getCurrMember());
            }
        }
        partialResult.add(row);
    }

}

// End ResultLoader.java
