/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.olap.Access;
import mondrian.olap.Id;
import mondrian.olap.Member;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

import java.sql.SQLException;
import java.util.*;

/**
 * Implementation of {@link MemberReader} which replaces given members
 * with a substitute.
 *
 * <p>Derived classes must implement the {@link #substitute(RolapMember)} and
 * {@link #desubstitute(RolapMember)} methods.
 *
 * @author jhyde
 * @since Oct 5, 2007
 */
public abstract class SubstitutingMemberReader extends DelegatingMemberReader {
    private final TupleReader.MemberBuilder memberBuilder =
        new SubstitutingMemberBuilder();

    /**
     * Creates a SubstitutingMemberReader.
     *
     * @param memberReader Parent member reader
     */
    SubstitutingMemberReader(MemberReader memberReader) {
        super(memberReader);
    }

    // Helper methods

    private List<RolapMember> desubstitute(List<RolapMember> members) {
        List<RolapMember> list = new ArrayList<RolapMember>(members.size());
        for (RolapMember member : members) {
            list.add(desubstitute(member));
        }
        return list;
    }

    private List<RolapMember> substitute(List<RolapMember> members) {
        List<RolapMember> list = new ArrayList<RolapMember>(members.size());
        for (RolapMember member : members) {
            list.add(substitute(member));
        }
        return list;
    }

    // ~ -- Implementations of MemberReader methods ---------------------------

    @Override
    public RolapMember getLeadMember(RolapMember member, int n) {
        return substitute(
            memberReader.getLeadMember(desubstitute(member), n));
    }

    @Override
    public List<RolapMember> getMembersInLevel(
        RolapLevel level)
    {
        return substitute(memberReader.getMembersInLevel(level));
    }

    @Override
    public void getMemberRange(
        RolapLevel level,
        RolapMember startMember,
        RolapMember endMember,
        List<RolapMember> list)
    {
        memberReader.getMemberRange(
            level,
            desubstitute(startMember),
            desubstitute(endMember),
            new SubstitutingMemberList(list));
    }

    @Override
    public int compare(
        RolapMember m1,
        RolapMember m2,
        boolean siblingsAreEqual)
    {
        return memberReader.compare(
            desubstitute(m1),
            desubstitute(m2),
            siblingsAreEqual);
    }

    @Override
    public RolapHierarchy getHierarchy() {
        return memberReader.getHierarchy();
    }

    @Override
    public boolean setCache(MemberCache cache) {
        // cache semantics don't make sense if members are not comparable
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RolapMember> getMembers() {
        // might make sense, but I doubt it
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RolapMember> getRootMembers() {
        return substitute(memberReader.getRootMembers());
    }

    @Override
    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        memberReader.getMemberChildren(
            desubstitute(parentMember),
            new SubstitutingMemberList(children));
    }

    @Override
    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children)
    {
        memberReader.getMemberChildren(
            desubstitute(parentMembers),
            new SubstitutingMemberList(children));
    }

    @Override
    public int getMemberCount() {
        return memberReader.getMemberCount();
    }

    @Override
    public RolapMember lookupMember(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        return substitute(
            memberReader.lookupMember(uniqueNameParts, failIfNotFound));
    }

    public Map<? extends Member, Access> getMemberChildren(
        RolapMember member,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        return memberReader.getMemberChildren(
            desubstitute(member),
            new SubstitutingMemberList(children),
            constraint);
    }

    public Map<? extends Member, Access> getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        return memberReader.getMemberChildren(
            desubstitute(parentMembers),
            new SubstitutingMemberList(children),
            constraint);
    }

    @Override
    public List<RolapMember> getMembersInLevel(
        RolapLevel level, TupleConstraint constraint)
    {
        return substitute(
            memberReader.getMembersInLevel(
                level, constraint));
    }

    @Override
    public RolapMember getDefaultMember() {
        return substitute(memberReader.getDefaultMember());
    }

    @Override
    public RolapMember getMemberParent(RolapMember member) {
        return substitute(memberReader.getMemberParent(desubstitute(member)));
    }

    @Override
    public TupleReader.MemberBuilder getMemberBuilder() {
        return memberBuilder;
    }

    /**
     * List which writes through to an underlying list, substituting members
     * as they are written and desubstituting as they are read.
     */
    class SubstitutingMemberList extends AbstractList<RolapMember> {
        private final List<RolapMember> list;

        SubstitutingMemberList(List<RolapMember> list) {
            this.list = list;
        }

        @Override
        public RolapMember get(int index) {
            return desubstitute(list.get(index));
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public RolapMember set(int index, RolapMember element) {
            return desubstitute(list.set(index, substitute(element)));
        }

        @Override
        public void add(int index, RolapMember element) {
            list.add(index, substitute(element));
        }

        @Override
        public RolapMember remove(int index) {
            return list.remove(index);
        }
    }

    private class SubstitutingMemberBuilder
        implements TupleReader.MemberBuilder
    {
        public MemberCache getMemberCache() {
            return memberReader.getMemberBuilder().getMemberCache();
        }

        public Object getMemberCacheLock() {
            return memberReader.getMemberBuilder().getMemberCacheLock();
        }

        public RolapMember makeMember(
            RolapMember parentMember,
            RolapLevel childLevel,
            Object value,
            Object captionValue,
            boolean parentChild,
            SqlStatement stmt,
            Object key,
            int column) throws SQLException
        {
            return substitute(
                memberReader.getMemberBuilder().makeMember(
                    desubstitute(parentMember),
                    childLevel,
                    value,
                    captionValue,
                    parentChild,
                    stmt,
                    key,
                    column));
        }

        public RolapMember allMember() {
            return substitute(memberReader.getHierarchy().getAllMember());
        }
    }
}

// End SubstitutingMemberReader.java
