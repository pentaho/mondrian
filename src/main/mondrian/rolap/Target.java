/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.rolap.sql.TupleConstraint;

import java.sql.SQLException;
import java.util.*;

/**
 * Helper class for {@link mondrian.rolap.HighCardSqlTupleReader} that
 * keeps track of target levels and constraints for adding to SQL query.
 *
 * @author luis f. canals, Kurtis Walker
 * @since July 23, 2009
 */
public class Target extends TargetBase {
    private final HighCardSqlTupleReader sqlTupleReader;
    private final MemberCache cache;
    private final TupleConstraint constraint;

    boolean parentChild;
    private RolapLevel[] levels;
    private int levelDepth;

    SqlTupleReader.ColumnLayout columnLayout;

    public Target(
        final RolapLevel level,
        final TupleReader.MemberBuilder memberBuilder,
        final List<RolapMember> srcMembers,
        final TupleConstraint constraint,
        final HighCardSqlTupleReader sqlTupleReader)
    {
        super(srcMembers, level, memberBuilder);
        this.sqlTupleReader = sqlTupleReader;
        this.constraint = constraint;
        this.cache = memberBuilder.getMemberCache();
    }

    public void open() {
        levels = level.getHierarchy().getLevelList().toArray(
            new RolapLevel[level.getHierarchy().getLevelList().size()]);
        // REVIEW: ArrayDeque is preferable to LinkedList (JDK1.6 and up) but it
        //   doesn't implement List, so we can't easily interoperate the two.
        setList(new LinkedList<RolapMember>());
        levelDepth = level.getDepth();
        parentChild = level.isParentChild();
    }

    void internalAddRow(SqlStatement stmt)
        throws SQLException
    {
        RolapMember member = null;
        if (getCurrMember() != null) {
            member = getCurrMember();
        } else {
            boolean checkCacheStatus = true;
            final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
            for (int i = 0; i <= levelDepth; i++) {
                RolapLevel childLevel = levels[i];
                if (childLevel.isAll()) {
                    member = memberBuilder.allMember();
                    continue;
                }
                SqlTupleReader.LevelColumnLayout layout =
                    columnLayout.levelLayoutMap.get(childLevel);

                final Comparable[] keyValues =
                    new Comparable[layout.keyOrdinals.length];
                for (int j = 0, n = layout.keyOrdinals.length; j < n; j++) {
                    int keyOrdinal = layout.keyOrdinals[j];
                    Comparable value = accessors.get(keyOrdinal).get();
                    keyValues[j] = SqlMemberSource.toComparable(value);
                }
                final Comparable captionValue;
                if (layout.captionOrdinal >= 0) {
                    captionValue = accessors.get(layout.captionOrdinal).get();
                } else {
                    captionValue = null;
                }
                final String nameValue;
                if (layout.nameOrdinal >= 0) {
                    final Comparable nameObject =
                        accessors.get(layout.nameOrdinal).get();
                    nameValue =
                        nameObject == null
                            ? null
                            : String.valueOf(nameObject);
                } else {
                    nameValue = null;
                }
                RolapMember parentMember = member;
                final Object key = RolapMember.Key.quick(keyValues);
                member = cache.getMember(childLevel, key, checkCacheStatus);
                checkCacheStatus = false; /* Only check the first time */
                if (member == null) {
                    if (constraint instanceof
                        RolapNativeCrossJoin.NonEmptyCrossJoinConstraint
                        && childLevel.isParentChild())
                    {
                        member =
                            ((RolapNativeCrossJoin.NonEmptyCrossJoinConstraint)
                                constraint).findMember(key);
                    }
                    if (member == null) {
                        final Comparable keyClone =
                            RolapMember.Key.create(keyValues);
                        member = memberBuilder.makeMember(
                            parentMember, childLevel, keyClone, captionValue,
                            nameValue, parentChild, stmt, layout);
                    }
                }
            }
            setCurrMember(member);
        }
        getList().add(member);
    }

    public void setColumnLayout(SqlTupleReader.ColumnLayout layout) {
        this.columnLayout = layout;
    }

    public List<Member> close() {
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
                    if (limit > 0 && !asList && getList().size() > limit) {
                        System.out.println(
                            "Target: 199, Ouch! Toooo big array..."
                            + hashCode());
                        new Throwable().printStackTrace();
                    }
                }
                return getList().size();
            }

            public RolapMember get(final int idx) {
                if (asList) {
                    return getList().get(idx);
                }
                if (idx == 0 && this.firstMemberAssigned) {
                    return this.first;
                }
                int index = idx - offset;

                if (0 < limit && index < 0) {
                    // Cannot send NoSuchElementException since its intercepted
                    // by AbstractSequentialList to identify out of bounds.
                    throw new RuntimeException(
                        "Element " + idx
                        + " has been forgotten");
                }

                while (index >= getList().size() && this.moreRows) {
                    this.moreRows = sqlTupleReader.readNextTuple();
                    if (limit > 0 && getList().size() > limit) {
                        while (getList().size() > limit) {
                            index--;
                            offset++;
                            ((LinkedList) getList()).removeFirst();
                        }
                    }
                }

                if (idx == 0) {
                    this.first = getList().get(index);

                    // Above might run into exception which is caught in
                    // isEmpty(). So can change the state of the object after
                    // that.
                    this.firstMemberAssigned = true;
                    return this.first;
                } else {
                    return getList().get(index);
                }
            }

            public RolapMember set(final int i, final RolapMember e) {
                if (asList) {
                    return getList().set(i, e);
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

        return Util.cast(l);
    }
}

// End Target.java
