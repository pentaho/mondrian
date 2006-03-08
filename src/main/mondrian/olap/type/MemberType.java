/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.*;

/**
 * The type of an expression which represents a member.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class MemberType implements Type {
    private final Hierarchy hierarchy;
    private final Dimension dimension;
    private final Level level;
    private final Member member;
    private final String digest;

    public static final MemberType Unknown = new MemberType(null, null, null, null);

    /**
     * Creates a type representing a member.
     *
     * @param dimension
     * @param hierarchy Hierarchy the member belongs to, or null if not known.
     * @param level Level the member belongs to, or null if not known
     * @param member The precise member, or null if not known
     */
    public MemberType(
            Dimension dimension,
            Hierarchy hierarchy,
            Level level,
            Member member) {
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
        StringBuffer buf = new StringBuffer("MemberType<");
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
        return new MemberType(hierarchy.getDimension(), hierarchy, null, null);
    }

    public static MemberType forLevel(Level level) {
        return new MemberType(level.getDimension(), level.getHierarchy(), level, null);
    }

    public static MemberType forMember(Member member) {
        return new MemberType(member.getDimension(), member.getHierarchy(), member.getLevel(), member);
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

    public boolean usesDimension(Dimension dimension, boolean maybe) {
        if (this.dimension == null) {
            return maybe;
        } else {
            return this.dimension == dimension ||
                    (maybe && this.dimension == null);
        }
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
}

// End MemberType.java
