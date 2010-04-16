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
import mondrian.resource.MondrianResource;

import java.util.*;

/**
 * Utilities for parsing fully-qualified member names, tuples, member lists,
 * and tuple lists.
 *
 * @version $Id$
 * @author jhyde
 */
public class IdentifierParser {
    public static final int START = 0;
    public static final int BEFORE_SEG = 1;
    public static final int IN_BRACKET_SEG = 2;
    public static final int AFTER_SEG = 3;
    public static final int IN_SEG = 4;

    private static char charAt(String s, int pos) {
        return pos < s.length() ? s.charAt(pos) : 0;
    }

    public static void parseTupleList(
        Builder builder,
        String string)
    {
        int i = 0;
        char c;
        while ((c = charAt(string, i++)) == ' ') {
        }
        if (c != '{') {
            throw fail(string, i, "{");
        }
        while (true) {
            i = parseTuple(builder, string, i);
            while ((c = charAt(string, i++)) == ' ') {
            }
            if (c == ',') {
                // fine
            } else if (c == '}') {
                // we're done
                return;
            } else {
                throw fail(string, i, ", or }");
            }
        }
    }

    /**
     * Parses a tuple, of the form '(member, member, ...)', and calls builder
     * methods when finding a segment, member or tuple.
     *
     * @param builder Builder
     * @param string String to parse
     * @param i Position to start parsing in string
     * @return Position where parsing ended in string
     */
    public static int parseTuple(
        Builder builder,
        String string,
        int i)
    {
        char c;
        while ((c = charAt(string, i++)) == ' ') {
        }
        if (c != '(') {
            throw fail(string, i, "(");
        }
        while (true) {
            i = parseMember(builder, string, i);
            while ((c = charAt(string, i++)) == ' ') {
            }
            if (c == ',') {
                // fine
            } else if (c == ')') {
                builder.tupleComplete();
                break;
            }
        }
        return i;
    }

    public static void parseMemberList(
        Builder builder,
        String string)
    {
        int i = 0;
        char c = charAt(string, i);
        while (c > 0 && c <= ' ') {
            c = charAt(string, ++i);
        }
        boolean leadingBrace = false;
        boolean trailingBrace = false;
        if (c == '{') {
            leadingBrace = true;
            ++i;
        }
        w:
        while (true) {
            i = parseMember(builder, string, i);
            c = charAt(string, i);
            while (c > 0 && c <= ' ') {
                c = charAt(string, ++i);
            }
            switch (c) {
            case 0:
                break w;
            case ',':
                // fine
                ++i;
                break;
            case '}':
                // we're done
                trailingBrace = true;
                break w;
            default:
                throw fail(string, i, ", or }");
            }
        }
        if (leadingBrace != trailingBrace) {
            throw Util.newInternal(
                "mismatched '{' and '}' in '" + string + "'");
        }
    }

    public static int parseMember(
        Builder builder,
        String string,
        int i)
    {
        int k = string.length();
        int state = START;
        int start = 0;
        char c;

        loop:
        while (i < k + 1) {
            c = charAt(string, i);
            switch (state) {
            case START:
            case BEFORE_SEG:
                switch (c) {
                case '[':
                    ++i;
                    start = i;
                    state = IN_BRACKET_SEG;
                    break;

                case ' ':
                    // Skip whitespace, don't change state.
                    ++i;
                    break;

                case ',':
                case '}':
                case 0:
                    break loop;

                case '.':
                    // TODO: test this, case: ".abc"
                    throw Util.newInternal("unexpected: '.'");

                default:
                    // Carry on reading.
                    state = IN_SEG;
                    start = i;
                    break;
                }
                break;

            case IN_SEG:
                switch (c) {
                case ',':
                case ')':
                case '}':
                case 0:
                    builder.segmentComplete(
                        new Id.Segment(
                            string.substring(start, i).trim(),
                            Id.Quoting.UNQUOTED));
                    state = AFTER_SEG;
                    break loop;
                case '.':
                    builder.segmentComplete(
                        new Id.Segment(
                            string.substring(start, i).trim(),
                            Id.Quoting.UNQUOTED));
                    state = BEFORE_SEG;
                    ++i;
                    break;
                default:
                    ++i;
                }
                break;

            case IN_BRACKET_SEG:
                switch (c) {
                case 0:
                    throw Util.newInternal(
                        "Expected ']', in member identifier '" + string + "'");
                case ']':
                    if (charAt(string, i + 1) == ']') {
                        ++i;
                        // fall through
                    } else {
                        builder.segmentComplete(
                            new Id.Segment(
                                Util.replace(
                                    string.substring(start, i), "]]", "]"),
                                Id.Quoting.QUOTED));
                        ++i;
                        state = AFTER_SEG;
                        break;
                    }
                default:
                    // Carry on reading.
                    ++i;
                }
                break;

            case AFTER_SEG:
                switch (c) {
                case ' ':
                    // Skip over any spaces
                    // TODO: test this case: '[foo]  .  [bar]'
                    ++i;
                    break;
                case '.':
                    state = BEFORE_SEG;
                    ++i;
                    break;

                default:
                    // We're not looking at the start of a segment. Parse
                    // the member we've seen so far, then return.
                    break loop;
                }
                break;

            default:
                throw Util.newInternal("unexpected state: " + state);
            }
        }

        switch (state) {
        case START:
            return i;
        case BEFORE_SEG:
            throw Util.newInternal(
                "Expected identifier after '.', in member identifier '" + string
                + "'");
        case IN_BRACKET_SEG:
            throw Util.newInternal(
                "Expected ']', in member identifier '" + string + "'");
        }
        // End of member.
        builder.memberComplete();
        return i;
    }

    public static RuntimeException fail(
        String string,
        int i,
        String expecting)
    {
        throw Util.newInternal(
            "expected '" + expecting + "' at position " + i + " in '"
            + string + "'");
    }

    /**
     * Parses an MDX identifier such as <code>[Foo].[Bar].Baz.&Key&Key2</code>
     * and returns the result as a list of segments.
     *
     * @param s MDX identifier
     * @return List of segments
     */
    public static List<Id.Segment> parseIdentifier(String s)  {
        final List<Id.Segment> list = new ArrayList<Id.Segment>();
        final Builder builder =
            new Builder() {
                public void tupleComplete() {
                    throw new UnsupportedOperationException();
                }

                public void memberComplete() {
                    // nothing
                }

                public void segmentComplete(Id.Segment segment) {
                    list.add(segment);
                }
            };
        int i = parseMember(builder, s, 0);
        if (i < s.length()) {
            throw MondrianResource.instance().MdxInvalidMember.ex(s);
        }
        return list;
    }

    /**
     * Parses a string consisting of a sequence of MDX identifiers and returns
     * the result as a list of compound identifiers, each of which is a list
     * of segments.
     *
     * <p>For example, parseIdentifierList("{foo.bar, baz}") returns
     * { {"foo", "bar"}, {"baz"} }.
     *
     * <p>The braces around the list are optional;
     * parseIdentifierList("foo.bar, baz") returns the same result as the
     * previous example.
     *
     * @param s MDX identifier list
     * @return List of lists of segments
     */
    public static List<List<Id.Segment>> parseIdentifierList(String s)  {
        final List<List<Id.Segment>> list =
            new ArrayList<List<Id.Segment>>();
        final Builder builder =
            new Builder() {
                final List<Id.Segment> segmentList =
                    new ArrayList<Id.Segment>();

                public void tupleComplete() {
                    throw new UnsupportedOperationException();
                }

                public void memberComplete() {
                    list.add(new ArrayList<Id.Segment>(segmentList));
                    segmentList.clear();
                }

                public void segmentComplete(Id.Segment segment) {
                    segmentList.add(segment);
                }
            };
        parseMemberList(builder, s);
        return list;
    }

    /**
     * Callback that is called on completion of a structural element like a
     * member or tuple.
     *
     * <p>Implementations might create a list of members or just create a list
     * of unresolved names.
     */
    public interface Builder {
        void tupleComplete();
        void memberComplete();
        void segmentComplete(Id.Segment segment);
    }

    /**
     * Implementation of Builder that resolves segment lists to members.
     */
    public static class BuilderImpl implements Builder {
        private final SchemaReader schemaReader;
        private final Cube cube;
        protected final List<Hierarchy> hierarchyList;
        protected final List<Id.Segment> nameList = new ArrayList<Id.Segment>();

        BuilderImpl(
            SchemaReader schemaReader, Cube cube, List<Hierarchy> hierarchyList)
        {
            this.schemaReader = schemaReader;
            this.cube = cube;
            this.hierarchyList = hierarchyList;
        }

        public void segmentComplete(Id.Segment segment) {
            nameList.add(segment);
        }

        public void tupleComplete() {
            throw new UnsupportedOperationException();
        }

        public void memberComplete() {
            throw new UnsupportedOperationException();
        }

        protected Member resolveMember(Hierarchy hierarchy) {
            final Member member =
                (Member) Util.lookupCompound(
                    schemaReader, cube, nameList, true, Category.Member);
            if (member.getHierarchy() != hierarchy) {
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
            SchemaReader schemaReader, Cube cube, List<Hierarchy> hierarchyList)
        {
            super(schemaReader, cube, hierarchyList);
        }

        public void memberComplete() {
            if (memberList.size() >= hierarchyList.size()) {
                throw Util.newInternal("expected ')");
            }
            final Hierarchy hierarchy = hierarchyList.get(memberList.size());
            final Member member = resolveMember(hierarchy);
            memberList.add(member);
            nameList.clear();
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
            tupleList.add(
                this.memberList.toArray(new Member[this.memberList.size()]));
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
            memberList.add(member);
            nameList.clear();
        }

        @Override
        public void tupleComplete() {
            // nothing to do
        }
    }
}

// End IdentifierParser.java
