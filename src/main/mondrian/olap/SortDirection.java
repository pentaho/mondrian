/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.olap;

/**
 * <code>SortDirection</code> describes the valid codes for sort direction.
 *
 * @author jhyde
 * @since Feb 21, 2003
 * @version $Id$
 **/
public class SortDirection extends EnumeratedValues {
    /** The singleton instance of <code>SortDirection</code>. **/
    public static final SortDirection instance = new SortDirection();

    private SortDirection() {
        super(
                new String[] {
                    "NONE", "ASC", "DESC", "BASC", "BDESC",
                },
                new int[] {
                    NONE, ASC, DESC, BASC, BDESC,
                },
                new String[] {
                    "none", "ascending", "descending", "nonhierarchized ascending", "nonhierarchized descending"}
        );
    }

    /** Returns the singleton instance of <code>SortDirection</code>. **/
    public static final SortDirection instance() {
        return instance;
    }

    /** Not sorted. */
  public static final int NONE = -1;
    /** Ascending inside hierarchy. */
  public static final int ASC = 0;
    /** Descending inside hierarchy */
  public static final int DESC = 1;
    /** Ascending disregarding hierarchy */
  public static final int BASC = 2;
    /** Descending disregarding hierarchy */
  public static final int BDESC = 3;
}

// End SortDirection.java