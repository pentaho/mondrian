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
 *
 * @version $Id$
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

    public Id(List<Segment> segments) {
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
        final StringBuilder buf = new StringBuilder();
        Util.quoteMdxIdentifier(segments, buf);
        return buf.toString();
    }

    public String[] toStringArray() {
        String[] names = new String[segments.size()];
        int k = 0;
        for (Segment segment : segments) {
            names[k++] = segment.name;
        }
        return names;
    }

    public List<Segment> getSegments() {
        return Collections.unmodifiableList(this.segments);
    }

    public Id.Segment getElement(int i) {
        return segments.get(i);
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
        final Exp element =
            Util.lookup(
                validator.getQuery(), segments, true);

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
            switch (s.quoting) {
            case UNQUOTED:
                pw.print(s.name);
                break;
            case KEY:
                pw.print("&[" + Util.mdxEncodeString(s.name) + "]");
                break;
            case QUOTED:
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

        public String toString() {
            switch (quoting) {
            case UNQUOTED: //return name; Disabled to pass old tests...
            case QUOTED: return "[" + name + "]";
            case KEY: return "&[" + name + "]";
            default: return "UNKNOWN:" + name;
            }
        }

        /**
         * Appends this segment to a StringBuffer
         *
         * @param buf StringBuffer
         */
        public void toString(StringBuilder buf) {
            switch (quoting) {
            case UNQUOTED:
                buf.append(name);
                return;
            case QUOTED:
                Util.quoteMdxIdentifier(name, buf);
                return;
            case KEY:
                buf.append('&');
                Util.quoteMdxIdentifier(name, buf);
                return;
            default:
                throw Util.unexpected(quoting);
            }
        }

        public boolean equals(final Object o) {
            if (o instanceof Segment) {
                Segment that = (Segment) o;
                return that.name.equals(this.name) &&
                    that.quoting == this.quoting;
            } else {
                return false;
            }
        }

        public int hashCode() {
            return name.hashCode();
        }

        /**
         * Converts an array of names to a list of segments.
         *
         * @param nameParts Array of names
         * @return List of segments
         */
        public static List<Segment> toList(String... nameParts) {
            final List<Segment> segments =
                new ArrayList<Segment>(nameParts.length);
            for (String namePart : nameParts) {
                segments.add(new Segment(namePart, Id.Quoting.QUOTED));
            }
            return segments;
        }

        /**
         * Returns whether this segment matches a given name according to
         * the rules of case-sensitivity and quoting.
         *
         * @param name Name to match
         * @return Whether matches
         */
        public boolean matches(String name) {
            switch (quoting) {
            case UNQUOTED:
                return Util.equalName(this.name, name);
            case QUOTED:
                return this.name.equals(name);
            default:
                return false;
            }
        }
    }

    public enum Quoting {

        /**
         * Unquoted identifier, for example "Measures".
         */
        UNQUOTED,

        /**
         * Quoted identifier, for example "[Measures]".
         */
        QUOTED,

        /**
         * Identifier quoted with an ampersand to indicate a key value, for
         * example the second segment in "[Employees].&[89]".
         */
        KEY
    }
}

// End Id.java
