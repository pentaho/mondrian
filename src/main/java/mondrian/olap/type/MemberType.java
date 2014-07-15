/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.type;

import mondrian.olap.*;

/**
 * The type of an expression which represents a member.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class MemberType implements Type {
    private final Hierarchy hierarchy;
    private final Dimension dimension;
    private final Level level;
    private final Member member;
    private final String digest;

    public static final MemberType Unknown =
        new MemberType(null, null, null, null);

    /**
     * Creates a type representing a member.
     *
     * @param dimension Dimension the member belongs to, or null if not known
     * @param hierarchy Hierarchy the member belongs to, or null if not known
     * @param level Level the member belongs to, or null if not known
     * @param member The precise member, or null if not known
     */
    public MemberType(
        Dimension dimension,
        Hierarchy hierarchy,
        Level level,
        Member member)
    {
        this.dimension = dimension;
        this.hierarchy = hierarchy;
        this.level = level;
        this.member = member;
        if (member != null) {
            Util.assertPrecondition(level != null);
            Util.assertPrecondition(member.getLevel() == level);
        }
        if (level != null) {
            Util.assertPrecondition(hierarchy != null);
            Util.assertPrecondition(level.getHierarchy() == hierarchy);
        }
        if (hierarchy != null) {
            Util.assertPrecondition(dimension != null);
            Util.assertPrecondition(hierarchy.getDimension() == dimension);
        }
        StringBuilder buf = new StringBuilder("MemberType<");
        if (member != null) {
            buf.append("member=").append(member.getUniqueName());
        } else if (level != null) {
            buf.append("level=").append(level.getUniqueName());
        } else if (hierarchy != null) {
            buf.append("hierarchy=").append(hierarchy.getUniqueName());
        } else if (dimension != null) {
            buf.append("dimension=").append(dimension.getUniqueName());
        }
        buf.append(">");
        this.digest = buf.toString();
    }

    public static MemberType forDimension(Dimension dimension) {
        return new MemberType(dimension, null, null, null);
    }

    public static MemberType forHierarchy(Hierarchy hierarchy) {
        final Dimension dimension;
        if (hierarchy == null) {
            dimension = null;
        } else {
            dimension = hierarchy.getDimension();
        }
        return new MemberType(dimension, hierarchy, null, null);
    }

    public static MemberType forLevel(Level level) {
        final Dimension dimension;
        final Hierarchy hierarchy;
        if (level == null) {
            dimension = null;
            hierarchy = null;
        } else {
            dimension = level.getDimension();
            hierarchy = level.getHierarchy();
        }
        return new MemberType(dimension, hierarchy, level, null);
    }

    public static MemberType forMember(Member member) {
        final Dimension dimension;
        final Hierarchy hierarchy;
        final Level level;
        if (member == null) {
            dimension = null;
            hierarchy = null;
            level = null;
        } else {
            dimension = member.getDimension();
            hierarchy = member.getHierarchy();
            level = member.getLevel();
        }
        return new MemberType(dimension, hierarchy, level, member);
    }

    public String toString() {
        return digest;
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public Level getLevel() {
        return level;
    }

    public Member getMember() {
        return member;
    }

    public boolean usesDimension(Dimension dimension, boolean definitely) {
        return this.dimension == dimension
            || (!definitely && this.dimension == null);
    }

    public boolean usesHierarchy(Hierarchy hierarchy, boolean definitely) {
        return this.hierarchy == hierarchy
            || (!definitely
                && this.hierarchy == null
                && (this.dimension == null
                    || this.dimension == hierarchy.getDimension()));
    }

    public Type getValueType() {
        // todo: when members have more type information (double vs. integer
        // vs. string), return better type if member != null.
        return new ScalarType();
    }

    public Dimension getDimension() {
        return dimension;
    }

    public static MemberType forType(Type type) {
        if (type instanceof MemberType) {
            return (MemberType) type;
        } else {
            return new MemberType(
                type.getDimension(),
                type.getHierarchy(),
                type.getLevel(),
                null);
        }
    }

    public Type computeCommonType(Type type, int[] conversionCount) {
        if (type instanceof ScalarType) {
            return getValueType().computeCommonType(type, conversionCount);
        }
        if (type instanceof TupleType) {
            return type.computeCommonType(this, conversionCount);
        }
        if (!(type instanceof MemberType)) {
            return null;
        }
        MemberType that = (MemberType) type;
        if (this.getMember() != null
            && this.getMember().equals(that.getMember()))
        {
            return this;
        }
        if (this.getLevel() != null
            && this.getLevel().equals(that.getLevel()))
        {
            return new MemberType(
                this.getDimension(),
                this.getHierarchy(),
                this.getLevel(),
                null);
        }
        if (this.getHierarchy() != null
            && this.getHierarchy().equals(that.getHierarchy()))
        {
            return new MemberType(
                this.getDimension(),
                this.getHierarchy(),
                null,
                null);
        }
        if (this.getDimension() != null
            && this.getDimension().equals(that.getDimension()))
        {
            return new MemberType(
                this.getDimension(),
                null,
                null,
                null);
        }
        return MemberType.Unknown;
    }

    public boolean isInstance(Object value) {
        return value instanceof Member
            && (level == null
            || ((Member) value).getLevel().equals(level))
            && (hierarchy == null
            || ((Member) value).getHierarchy().equals(hierarchy))
            && (dimension == null
            || ((Member) value).getDimension().equals(dimension));
    }

    public int getArity() {
        return 1;
    }
}

// End MemberType.java
