/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
//
// jhyde, 3 March, 2002
*/
package mondrian.util;

import mondrian.calc.TupleList;
import mondrian.calc.impl.ArrayTupleList;
import mondrian.olap.*;
import mondrian.olap.fun.FunUtil;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapCube;

import java.util.*;

/**
 * Utilities for parsing fully-qualified member names, tuples, member lists,
 * and tuple lists.
 *
 * @author jhyde
 */
public class IdentifierParser extends org.olap4j.impl.IdentifierParser {

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
        public final TupleList tupleList;

        public TupleListBuilder(
            SchemaReader schemaReader, Cube cube, List<Hierarchy> hierarchyList)
        {
            super(schemaReader, cube, hierarchyList);
            tupleList = new ArrayTupleList(hierarchyList.size());
        }

        public void tupleComplete() {
            super.tupleComplete();
            if (!FunUtil.tupleContainsNullMember(memberList)) {
                tupleList.add(memberList);
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
