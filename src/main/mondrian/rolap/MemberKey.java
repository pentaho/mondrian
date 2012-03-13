/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap;

import mondrian.olap.Util;

/**
 * <code>MemberKey</code> todo:
 *
 * @author jhyde
 * @since 21 March, 2002
 */
class MemberKey {
    private final RolapMember parent;
    private final Object value;

    MemberKey(RolapMember parent, Object value) {
        this.parent = parent;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MemberKey)) {
            return false;
        }
        MemberKey other = (MemberKey) o;
        return Util.equals(this.parent, other.parent)
            && Util.equals(this.value, other.value);
    }

    @Override
    public int hashCode() {
        int h = 0;
        if (value != null) {
            h = value.hashCode();
        }
        if (parent != null) {
            h = (h * 31) + parent.hashCode();
        }
        return h;
    }

    /**
     * Returns the level of the member that this key represents.
     *
     * @return Member level, or null if is root member
     */
    public RolapLevel getLevel() {
        if (parent == null) {
            return null;
        }
        final RolapLevel level = parent.getLevel();
        if (level.isParentChild()) {
            return level;
        }
        return (RolapLevel) level.getChildLevel();
    }
}

// End MemberKey.java
