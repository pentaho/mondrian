/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
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
    private final Level level;
    private final Member member;

    /**
     * Creates a type representing a member.
     *
     * @param hierarchy Hierarchy the member belongs to, or null if not known.
     * @param level
     * @param member
     */
    public MemberType(Hierarchy hierarchy, Level level, Member member) {
        this.level = level;
        this.member = member;
        this.hierarchy = hierarchy;
        if (member != null) {
            Util.assertPrecondition(level != null);
            Util.assertPrecondition(member.getLevel() == level);
        }
        if (level != null) {
            Util.assertPrecondition(hierarchy != null);
            Util.assertPrecondition(level.getHierarchy() == hierarchy);
        }
    }

    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    public Level getLevel() {
        return level;
    }

    public boolean usesDimension(Dimension dimension) {
        return hierarchy != null && hierarchy.getDimension() == dimension;
    }
}

// End HierarchyType.java
