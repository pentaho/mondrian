/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.olap.Member;
import mondrian.rolap.sql.TupleConstraint;

import java.sql.SQLException;
import java.util.List;
import java.util.RandomAccess;

/**
 * Base helper class for the SQL tuple readers
 * {@link mondrian.rolap.HighCardSqlTupleReader} and
 * {@link mondrian.rolap.SqlTupleReader}.
 *
 * <p>Keeps track of target levels and constraints for adding to the SQL query.
 * The real work is done in the extending classes,
 * {@link Target} and
 * {@link mondrian.rolap.SqlTupleReader.Target}.
 *
 * @author Kurtis Walker
 * @since July 23, 2009
 */
public abstract class TargetBase {
    final List<RolapMember> srcMembers;
    final RolapLevel level;
    private RolapMember currMember;
    private List<RolapMember> list;
    final Object cacheLock;
    final TupleReader.MemberBuilder memberBuilder;

    public TargetBase(
        List<RolapMember> srcMembers,
        RolapLevel level,
        TupleReader.MemberBuilder memberBuilder)
    {
        this.srcMembers = srcMembers;
        this.level = level;
        cacheLock = memberBuilder.getMemberCacheLock();
        this.memberBuilder = memberBuilder;
    }

    public void setList(final List<RolapMember> list) {
        assert list instanceof RandomAccess;
        this.list = list;
    }

    public List<RolapMember> getSrcMembers() {
        return srcMembers;
    }

    public RolapLevel getLevel() {
        return level;
    }

    public RolapMember getCurrMember() {
        return this.currMember;
    }

    public void removeCurrMember() {
        this.currMember = null;
    }

    public void setCurrMember(final RolapMember m) {
        this.currMember = m;
    }

    public List<RolapMember> getList() {
        return list;
    }

    public String toString() {
        return level.getUniqueName();
    }

    /**
     * Adds a row to the collection.
     *
     * @param stmt Statement
     * @param column Column ordinal (0-based)
     * @return Ordinal of next unconsumed column
     * @throws SQLException On error
     */
    public final int addRow(SqlStatement stmt, int column) throws SQLException {
        synchronized (cacheLock) {
            return internalAddRow(stmt, column);
        }
    }

    public abstract void open();

    public abstract List<Member> close();

    abstract int internalAddRow(SqlStatement stmt, int column)
        throws SQLException;

    public void add(final RolapMember member) {
        this.getList().add(member);
    }

    RolapNativeCrossJoin.NonEmptyCrossJoinConstraint
    castToNonEmptyCJConstraint(TupleConstraint constraint) {
        return (RolapNativeCrossJoin.NonEmptyCrossJoinConstraint) constraint;
    }
}

// End TargetBase.java
