/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;
import mondrian.olap.type.Type;
import mondrian.mdx.MdxVisitor;

import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Multi-part identifier.
 */
public class Id
    extends ExpBase
    implements Cloneable {

    private final List<Segment> segments;

    /**
     * Creates an identifier containing a single part.
     *
     * @param segment Segment, consisting of a name and quoting style
     */
    public Id(Segment segment) {
        segments = Collections.singletonList(segment);
    }

    private Id(List<Segment> segments) {
        this.segments = segments;
    }

    public Id clone() {
        // This is immutable, so no need to clone.
        return this;
    }

    public int getCategory() {
        return Category.Unknown;
    }

    public Type getType() {
        // Can't give the type until we have resolved.
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return Util.quoteMdxIdentifier(toStringArray());
    }

    public String[] toStringArray() {
        String[] names = new String[segments.size()];
        int k = 0;
        for (Segment segment : segments) {
            names[k++] = segment.name;
        }
        return names;
    }

    public String getElement(int i) {
        return segments.get(i).name;
    }

    /**
     * Returns a new Identifier consisting of this one with another segment
     * appended. Does not modify this Identifier.
     *
     * @param segment Name of segment
     * @return New identifier
     */
    public Id append(Segment segment) {
        List<Segment> newSegments = new ArrayList<Segment>(segments);
        newSegments.add(segment);
        return new Id(newSegments);
    }

    public Exp accept(Validator validator) {
        if (segments.size() == 1) {
            final Segment s = segments.get(0);
            if (s.quoting == Quoting.UNQUOTED &&
                validator.getFunTable().isReserved(s.name)) {
                return Literal.createSymbol(s.name.toUpperCase());
            }
        }
        final String[] names = toStringArray();
        final Exp element = Util.lookup(validator.getQuery(), names, true);
        if (element == null) {
            return null;
        }
        return element.accept(validator);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }

    public void unparse(PrintWriter pw) {
        int k = 0;
        for (Segment s : segments) {
            if (k++ > 0) {
                pw.print(".");
            }
            switch (s.quoting.ordinal) {
            case Quoting.UNQUOTED_ORDINAL:
                pw.print(s.name);
                break;
            case Quoting.KEY_ORDINAL:
                pw.print("&[" + Util.mdxEncodeString(s.name) + "]");
                break;
            case Quoting.QUOTED_ORDINAL:
                pw.print("[" + Util.mdxEncodeString(s.name) + "]");
                break;
            }
        }
    }

    /**
     * Component in a compound identifier. It is described by its name and how
     * the name is quoted.
     *
     * <p>For example, the identifier
     * <code>[Store].USA.[New Mexico].&[45]</code> has four segments:<ul>
     * <li>"Store", {@link mondrian.olap.Id.Quoting#QUOTED}</li>
     * <li>"USA", {@link mondrian.olap.Id.Quoting#UNQUOTED}</li>
     * <li>"New Mexico", {@link mondrian.olap.Id.Quoting#QUOTED}</li>
     * <li>"45", {@link mondrian.olap.Id.Quoting#KEY}</li>
     * </ul>
     */
    public static class Segment {
        public final String name;
        public final Quoting quoting;

        public Segment(String name, Quoting quoting) {
            this.name = name;
            this.quoting = quoting;
        }
    }

    public static class Quoting extends EnumeratedValues.BasicValue {

        public static final int UNQUOTED_ORDINAL = 0;
        /**
         * Unquoted identifier, for example "Measures".
         */
        public static final Quoting UNQUOTED = new Quoting("UNQUOTED", UNQUOTED_ORDINAL);

        public static final int QUOTED_ORDINAL = 1;
        /**
         * Quoted identifier, for example "[Measures]".
         */
        public static final Quoting QUOTED = new Quoting("QUOTED", QUOTED_ORDINAL);

        public static final int KEY_ORDINAL = 2;
        /**
         * Identifier quoted with an ampersand to indicate a key value, for example
         * the second segment in "[Employees].&[89]".
         */
        public static final Quoting KEY = new Quoting("KEY", KEY_ORDINAL);

        private Quoting(String name, int ordinal) {
            super(name, ordinal, null);
        }
    }
}

// End Id.java
