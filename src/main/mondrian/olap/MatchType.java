/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
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
public class MatchType extends EnumeratedValues {
    /** The singleton instance of <code>MatchType</code>. */
    public static final MatchType instance = new MatchType();

    private MatchType() {
        super(
                new String[] {
                    "exact", "before", "after"
                },
                new int[] {
                    EXACT, BEFORE, AFTER
                }
        );
    }

    /** Returns the singleton instance of <code>MatchType</code>. */
    public static final MatchType instance() {
        return instance;
    }
    /** Match the unique name exactly */
    public static final int EXACT = 1;
    /** If no exact match, return the preceding member */
    public static final int BEFORE = 2;
    /** If no exact match, return the next member */
    public static final int AFTER = 3;
}

// End MatchType.java
