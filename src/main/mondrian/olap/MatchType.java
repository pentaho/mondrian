/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.olap;

/**
 * <code>MatchType</code> enumerates the allowable match modes when
 * searching for a member based on its unique name.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public enum MatchType {
    /** Match the unique name exactly, do not query database for members */
    EXACT_SCHEMA,
    /** Match the unique name exactly */
    EXACT,
    /** If no exact match, return the preceding member */
    BEFORE,
    /** If no exact match, return the next member */
    AFTER;

    /**
     * Return true if either Exact or Exact Schema value
     * is selected.
     *
     * @return true if exact
     */
    public boolean isExact() {
        return this == EXACT || this == EXACT_SCHEMA;
    }
}

// End MatchType.java
