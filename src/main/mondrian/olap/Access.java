/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.olap;

/**
 * <code>Access</code> enumerates the allowable access rights.
 *
 * @author jhyde
 * @since Feb 21, 2003
 * @version $Id$
 **/
public class Access extends EnumeratedValues {
	/** The singleton instance of <code>Access</code>. **/
	public static final Access instance = new Access();

	private Access() {
		super(
				new String[] {
					"none", "custom", "all_dimensions", "all",
				},
				new int[] {
					NONE, CUSTOM, ALL_DIMENSIONS, ALL,
				}
		);
	}

	/** Returns the singleton instance of <code>Access</code>. **/
	public static final Access instance() {
		return instance;
	}
	/** No access to an object. **/
	public static final int NONE = 1;
	/** Custom access to an object (described by other parameters). **/
	public static final int CUSTOM = 2;
	/** Access to all shared dimensions (applies to schema grant). **/
	public static final int ALL_DIMENSIONS = 3;
	/** All access to an object. **/
	public static final int ALL = 4;
}

// End Access.java