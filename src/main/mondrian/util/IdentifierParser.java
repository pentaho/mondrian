/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.util;

import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapCube;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.ParseRegion;

import java.util.*;

/**
 * Utilities for parsing fully-qualified member names, tuples, member lists,
 * and tuple lists.
 *
 * @version $Id$
 * @author jhyde
 */
public class IdentifierParser extends org.olap4j.impl.IdentifierParser {

    /**
     * Implementation of {@link org.olap4j.impl.IdentifierParser.Builder}
     * that collects the segments that make up the name of a member in a list.
     * It cannot handle tuples or lists of members.
     *
     * <p>Copied from olap4j. TODO: make olap4j class protected, and obsolete.
     */
    private static class MemberBuilder implements Builder {
        final List<IdentifierNode.NameSegment> subSegments =
            new ArrayList<IdentifierNode.NameSegment>();
        protected final List<IdentifierNode.Segment> segmentList =
            new ArrayList<IdentifierNode.Segment>();

        public MemberBuilder() {
        }

        public void tupleComplete() {
            throw new UnsupportedOperationException();
        }

        public void memberComplete() {
            flushSubSegments();
        }

        private void flushSubSegments() {
            if (!subSegments.isEmpty()) {
                segmentList.add(new IdentifierNode.KeySegment(subSegments));
                subSegments.clear();
            }
        }

        public void segmentComplete(
            ParseRegion region,
            String name,
            IdentifierNode.Quoting quoting,
            Syntax syntax)
        {
            final IdentifierNode.NameSegment segment =
                new IdentifierNode.NameSegment(
                    region, name, quoting);
            if (syntax != Syntax.NEXT_KEY) {
                // If we were building a previous key, write it out.
                // E.g. [Foo].&1&2.&3&4&5.
                flushSubSegments();
            }
            if (syntax == Syntax.NAME) {
                segmentList.add(segment);
            } else {
                subSegments.add(segment);
            }
        }
    }

    /**
     * Extension to {@link mondrian.util.IdentifierParser.MemberBuilder} that
     *
     */
    private static class _MemberListBuilder extends MemberBuilder {
        final List<List<IdentifierNode.Segment>> list =
            new ArrayList<List<IdentifierNode.Segment>>();

        public void memberComplete() {
            super.memberComplete();
            list.add(
                new ArrayList<IdentifierNode.Segment>(segmentList));
            segmentList.clear();
        }
    }

    /**
     * Implementation of Builder that resolves segment lists to members.
     */
    public static class BuilderImpl extends MemberBuilder {
        private final SchemaReader schemaReader;
        private final Cube cube;
        protected final List<Hierarchy> hierarchyList;
        private final boolean ignoreInvalid;

        BuilderImpl(
            SchemaReader schemaReader,
            Cube cube,
            List<Hierarchy> hierarchyList)
        {
            this.schemaReader = schemaReader;
            this.cube = cube;
            this.hierarchyList = hierarchyList;
            final MondrianProperties props = MondrianProperties.instance();
            final boolean load = ((RolapCube) cube).isLoadInProgress();
            this.ignoreInvalid =
                (load
                    ? props.IgnoreInvalidMembers.get()
                    : props.IgnoreInvalidMembersDuringQuery.get());
        }

        protected Member resolveMember(Hierarchy expectedHierarchy) {
            final List<Id.Segment> mondrianSegmentList =
                Util.convert(this.segmentList);
            Member member =
                (Member) Util.lookupCompound(
                    schemaReader, cube, mondrianSegmentList, !ignoreInvalid,
                    Category.Member);
            if (member == null) {
                assert ignoreInvalid;
                if (expectedHierarchy != null) {
                    return expectedHierarchy.getNullMember();
                } else {
                    // Guess the intended hierarchy from the largest valid
                    // prefix.
                    for (int i = mondrianSegmentList.size() - 1; i > 0; --i) {
                        List<Id.Segment> partialName =
                            mondrianSegmentList.subList(0, i);
                        OlapElement olapElement =
                            schemaReader.lookupCompound(
                                cube, partialName, false, Category.Unknown);
                        if (olapElement != null) {
                            return olapElement.getHierarchy().getNullMember();
                        }
                    }
                    throw MondrianResource.instance().MdxChildObjectNotFound.ex(
                        Util.implode(mondrianSegmentList),
                        cube.getQualifiedName());
                }
            }
            if (expectedHierarchy != null
                && member.getHierarchy() != expectedHierarchy)
            {
                // TODO: better error
                throw Util.newInternal("member is of wrong hierarchy");
            }
            return member;
        }
    }

    /**
     * Implementation of Builder that builds a tuple.
     */
    public static class TupleBuilder extends BuilderImpl {
        protected final List<Member> memberList = new ArrayList<Member>();

        public TupleBuilder(
            SchemaReader schemaReader,
            Cube cube,
            List<Hierarchy> hierarchyList)
        {
            super(schemaReader, cube, hierarchyList);
        }

        public void memberComplete() {
            super.memberComplete();
            if (memberList.size() >= hierarchyList.size()) {
                throw Util.newInternal("expected ')");
            }
            final Hierarchy hierarchy = hierarchyList.get(memberList.size());
            final Member member = resolveMember(hierarchy);
            memberList.add(member);
            segmentList.clear();
        }

        public void tupleComplete() {
            if (memberList.size() < hierarchyList.size()) {
                throw Util.newInternal("too few members");
            }
        }
    }

    /**
     * Implementation of Builder that builds a tuple list.
     */
    public static class TupleListBuilder extends TupleBuilder {
        public final List<Member[]> tupleList = new ArrayList<Member[]>();

        public TupleListBuilder(
            SchemaReader schemaReader, Cube cube, List<Hierarchy> hierarchyList)
        {
            super(schemaReader, cube, hierarchyList);
        }

        public void tupleComplete() {
            super.tupleComplete();
            final Member[] members =
                memberList.toArray(new Member[memberList.size()]);
            if (!FunUtil.tupleContainsNullMember(members)) {
                tupleList.add(members);
            }
            this.memberList.clear();
        }
    }

    /**
     * Implementation of Builder that builds a member list.
     */
    public static class MemberListBuilder extends BuilderImpl {
        public final List<Member> memberList = new ArrayList<Member>();

        public MemberListBuilder(
            SchemaReader schemaReader, Cube cube, Hierarchy hierarchy)
        {
            super(schemaReader, cube, Collections.singletonList(hierarchy));
        }

        public void memberComplete() {
            final Member member = resolveMember(hierarchyList.get(0));
            if (!member.isNull()) {
                memberList.add(member);
            }
            segmentList.clear();
        }

        @Override
        public void tupleComplete() {
            // nothing to do
        }
    }
}

// End IdentifierParser.java
