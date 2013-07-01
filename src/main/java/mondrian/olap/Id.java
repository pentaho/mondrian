/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.mdx.MdxVisitor;
import mondrian.olap.type.Type;

import org.olap4j.impl.UnmodifiableArrayList;

import java.io.PrintWriter;
import java.util.*;

/**
 * Multi-part identifier.
 *
 * @author jhyde, 21 January, 1999
 */
public class Id
    extends ExpBase
    implements Cloneable
{

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
        if (segments.size() <= 0) {
            throw new IllegalArgumentException();
        }
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
            names[k++] = ((NameSegment) segment).getName();
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
            if (s.quoting == Quoting.UNQUOTED) {
                NameSegment nameSegment = (NameSegment) s;
                if (validator.getFunTable().isReserved(nameSegment.getName())) {
                    return Literal.createSymbol(
                        nameSegment.getName().toUpperCase());
                }
            }
        }
        final Exp element =
            Util.lookup(
                validator.getQuery(),
                validator.getSchemaReader().withLocus(),
                segments,
                true);

        if (element == null) {
            return null;
        }
        return element.accept(validator);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }

    public void unparse(PrintWriter pw) {
        pw.print(toString());
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
    public static abstract class Segment {
        public final Quoting quoting;

        protected Segment(Quoting quoting) {
            this.quoting = quoting;
        }

        public String toString() {
            final StringBuilder buf = new StringBuilder();
            toString(buf);
            return buf.toString();
        }

        public Quoting getQuoting() {
            return quoting;
        }

        public abstract List<NameSegment> getKeyParts();

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
                segments.add(new NameSegment(namePart));
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
        public abstract boolean matches(String name);

        /**
         * Appends this segment to a StringBuilder.
         *
         * @param buf String builder to write to
         */
        public abstract void toString(StringBuilder buf);
    }

    /**
     * Component in a compound identifier that describes the name of an object.
     * Optionally, the name is quoted in brackets.
     *
     * @see KeySegment
     */
    public static class NameSegment extends Segment {
        public final String name;

        /**
         * Creates a name segment with the given quoting.
         *
         * @param name Name
         * @param quoting Quoting style
         */
        public NameSegment(String name, Quoting quoting) {
            super(quoting);
            this.name = name;
            if (name == null) {
                throw new NullPointerException();
            }
            if (!(quoting == Quoting.QUOTED || quoting == Quoting.UNQUOTED)) {
                throw new IllegalArgumentException();
            }
        }

        /**
         * Creates a quoted name segment.
         *
         * @param name Name
         */
        public NameSegment(String name) {
            this(name, Quoting.QUOTED);
        }

        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof NameSegment)) {
                return false;
            }
            NameSegment that = (NameSegment) o;
            return that.name.equals(this.name);
        }

        public int hashCode() {
            return name.hashCode();
        }

        public String getName() {
            return name;
        }

        public List<NameSegment> getKeyParts() {
            return null;
        }

        public void toString(StringBuilder buf) {
            switch (quoting) {
            case UNQUOTED:
                buf.append(name);
                return;
            case QUOTED:
                Util.quoteMdxIdentifier(name, buf);
                return;
            default:
                throw Util.unexpected(quoting);
            }
        }

        public boolean matches(String name) {
            switch (quoting) {
            case UNQUOTED:
                return Util.equalName(this.name, name);
            case QUOTED:
                return Util.equalName(this.name, name);
            default:
                return false;
            }
        }
    }

    /**
     * Identifier segment representing a key, possibly composite.
     */
    public static class KeySegment extends Segment {
        public final List<NameSegment> subSegmentList;

        /**
         * Creates a KeySegment with one or more sub-segments.
         *
         * @param subSegments Array of sub-segments
         */
        public KeySegment(NameSegment... subSegments) {
            super(Quoting.KEY);
            if (subSegments.length < 1) {
                throw new IllegalArgumentException();
            }
            this.subSegmentList = UnmodifiableArrayList.asCopyOf(subSegments);
        }

        /**
         * Creates a KeySegment a list of sub-segments.
         *
         * @param subSegmentList List of sub-segments
         */
        public KeySegment(List<NameSegment> subSegmentList) {
            super(Quoting.KEY);
            if (subSegmentList.size() < 1) {
                throw new IllegalArgumentException();
            }
            this.subSegmentList =
                new UnmodifiableArrayList<NameSegment>(
                    subSegmentList.toArray(
                        new NameSegment[subSegmentList.size()]));
        }

        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KeySegment)) {
                return false;
            }
            KeySegment that = (KeySegment) o;
            return this.subSegmentList.equals(that.subSegmentList);
        }

        public int hashCode() {
            return subSegmentList.hashCode();
        }

        public void toString(StringBuilder buf) {
            for (NameSegment segment : subSegmentList) {
                buf.append('&');
                segment.toString(buf);
            }
        }

        public List<NameSegment> getKeyParts() {
            return subSegmentList;
        }

        public boolean matches(String name) {
            return false;
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
         * example the second segment in "[Employees].&amp;[89]".
         */
        KEY
    }
}

// End Id.java
