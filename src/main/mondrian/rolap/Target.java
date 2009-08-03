/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

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

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.util.*;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Extracted Target from original place.
 * Unknown functonallity.
 */
public class Target {
    private final HighCardSqlTupleReader sqlTupleReader;
    private final RolapLevel level;
    private final MemberCache cache;
    private final TupleConstraint constraint;
    private final TupleReader.MemberBuilder memberBuilder;
    private final List<RolapMember> srcMembers;

    boolean parentChild;
    private RolapLevel[] levels;
    private RolapMember currMember;
    private List<List<RolapMember>> siblings;
    private List<RolapMember> members;
    private int levelDepth;
    private List<RolapMember> list;

    public Target(final RolapLevel level,
            final TupleReader.MemberBuilder memberBuilder,
            final List<RolapMember> srcMembers,
            final TupleConstraint constraint,
            final HighCardSqlTupleReader sqlTupleReader) {
        this.sqlTupleReader = sqlTupleReader;
        this.level = level;
        this.constraint = constraint;
        this.cache = memberBuilder.getMemberCache();
        this.memberBuilder = memberBuilder;
        this.srcMembers = srcMembers;
    }

    public List<RolapMember> getSrcMembers() {
        return this.srcMembers;
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

    public void add(final RolapMember member) {
        this.list.add(member);
    }

    public void open() {
        levels = (RolapLevel[]) level.getHierarchy().getLevels();
        list = new LinkedList<RolapMember>();
        levelDepth = level.getDepth();
        parentChild = level.isParentChild();
        // members[i] is the current member of level#i, and siblings[i]
        // is the current member of level#i plus its siblings
        members = new ArrayList<RolapMember>();
        for (int i = 0; i < levels.length; i++) {
            members.add(null);
        }
        siblings = new ArrayList<List<RolapMember>>();
        for (int i = 0; i < levels.length + 1; i++) {
            siblings.add(new ArrayList<RolapMember>());
        }
    }

    /**
     * Scans a row of the resultset and creates a member
     * for the result.
     *
     * @param resultSet result set to retrieve rows from
     * @param column the column index to start with
     *
     * @return index of the last column read + 1
     * @throws SQLException
     */
    public int addRow(ResultSet resultSet, int column) throws SQLException {
        synchronized (cache) {
            return internalAddRow(resultSet, column);
        }
    }

    private int internalAddRow(ResultSet resultSet, int column)
            throws SQLException {
        RolapMember member = null;
        if (currMember != null) {
            member = currMember;
        } else {
            boolean checkCacheStatus = true;
            for (int i = 0; i <= levelDepth; i++) {
                RolapLevel childLevel = levels[i];
                if (childLevel.isAll()) {
                    member = level.getHierarchy().getAllMember();
                    continue;
                }
                Object value = resultSet.getObject(++column);
                if (value == null) {
                    value = RolapUtil.sqlNullValue;
                }
                Object captionValue;
                if (childLevel.hasCaptionColumn()) {
                    captionValue = resultSet.getObject(++column);
                } else {
                    captionValue = null;
                }
                RolapMember parentMember = member;
                Object key = cache.makeKey(parentMember, value);
                member = cache.getMember(key, checkCacheStatus);
                checkCacheStatus = false; /* Only check the first time */
                if (member == null) {
                    member = memberBuilder.makeMember(
                        parentMember, childLevel, value, captionValue,
                        parentChild, resultSet, key, column);
                }

                // Skip over the columns consumed by makeMember
                if (!childLevel.getOrdinalExp().equals(
                    childLevel.getKeyExp()))
                {
                    ++column;
                }
                column += childLevel.getProperties().length;
            }
            currMember = member;
        }
        ((List) list).add(member);
        return column;
    }

    public List<RolapMember> close() {
        final boolean asList = this.constraint.getEvaluator() != null
                && this.constraint.getEvaluator().getQuery().getResultStyle()
                    == ResultStyle.LIST;
        final int limit = MondrianProperties.instance().ResultLimit.get();

        final List<RolapMember> l = new AbstractList<RolapMember>() {
            private boolean moreRows = true;
            private int offset = 0;
            private RolapMember first = null;
            private boolean firstMemberAssigned = false;

            /**
             * Performs a load of the whole result set.
             */
            public int size() {
                while (this.moreRows) {
                    this.moreRows = sqlTupleReader.readNextTuple();
                    if (limit > 0 && !asList && list.size() > limit) {
                        System.out.println("Target: 199, Ouch! Toooo big array..." + this.hashCode());
                        new Throwable().printStackTrace();
                    }
                }

                return list.size();
            }

            public RolapMember get(final int idx) {
                if (asList) {
                    return list.get(idx);
                }

                if (idx == 0 && this.firstMemberAssigned) {
                    return this.first;
                }
                int index = idx - offset;

                if (0 < limit && index < 0) {
                    // Cannot send NoSuchElementException since its intercepted
                    // by AbstractSequentialList to identify out of bounds.
                    throw new RuntimeException("Element " + idx
                            + " has been forgotten");
                }

                while (index >= list.size() && this.moreRows) {
                    this.moreRows = sqlTupleReader.readNextTuple();
                    if (!asList && limit > 0 && list.size() > limit) {
                        while (list.size() > limit) {
                            index--;
                            offset++;
                            ((LinkedList) list).removeFirst();
                        }
                    }
                }

                if (idx == 0) {
                    this.first = list.get(index);
                    this.firstMemberAssigned = true;
                    return this.first;
                } else {
                    return list.get(index);
                }
            }

            public RolapMember set(final int i, final RolapMember e) {
                if (asList) {
                    return list.set(i, e);
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            public boolean isEmpty() {
                try {
                    get(0);
                    return false;
                } catch (IndexOutOfBoundsException e) {
                    return true;
                }
            }

            public int hashCode() {
                return Target.this.hashCode();
            }

            public Iterator<RolapMember> iterator() {
                return new Iterator<RolapMember>() {
                    private int cursor = 0;

                    public boolean hasNext() {
                        try {
                            get(cursor);
                            return true;
                        } catch (IndexOutOfBoundsException ioobe) {
                            return false;
                        }
                    }

                    public RolapMember next() {
                        return get(cursor++);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };

        if (asList) {
            l.size();
        }

        return l;

/*
        synchronized (cache) {
            return internalClose();
        }
*/
    }


    //
    // Private Stuff --------------------------------------------------
    //

    /**
     * Cleans up after all rows have been processed, and returns the list of
     * members.
     *
     * @return list of members
     */
    private List<RolapMember> internalClose() {
        for (int i = 0; i < members.size(); i++) {
            RolapMember member = members.get(i);
            final List<RolapMember> children = siblings.get(i + 1);
            if (member != null && children != null) {
                // If we are finding the members of a particular level, and
                // we happen to find some of the children of an ancestor of
                // that level, we can't be sure that we have found all of
                // the children, so don't put them in the cache.
                if (member.getDepth() < level.getDepth()) {
                    continue;
                }
                MemberChildrenConstraint mcc =
                    constraint.getMemberChildrenConstraint(member);
                if (mcc != null) {
                    cache.putChildren(member, mcc, children);
                }
            }
        }
        return list;
    }

    /**
     * Adds <code>member</code> just before the first element in
     * <code>list</code> which has the same parent.
     */
    private void addAsOldestSibling(final List<RolapMember> list,
            final RolapMember member) {
        int i = list.size();
        while (--i >= 0) {
            RolapMember sibling = list.get(i);
            if (sibling.getParentMember() != member.getParentMember()) {
                break;
            }
        }
        list.add(i + 1, member);
    }

    public RolapLevel getLevel() {
        return level;
    }

    public String toString() {
        return level.getUniqueName();
    }
}

// End Target.java
