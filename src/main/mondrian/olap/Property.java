/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 September, 2002
*/

package mondrian.olap;

/**
 * <code>Property</code> is the definition of a member property.
 */
public abstract class Property {
	private String name;
	/** The datatype; one of {@link #TYPE_STRING}, {@link #TYPE_NUMERIC},
	 * {@link #TYPE_BOOLEAN}. */
	private int type;
	public static final int TYPE_STRING = 0;
	public static final int TYPE_NUMERIC = 1;
	public static final int TYPE_BOOLEAN = 2;

	protected Property(String name, int type) {
		this.name = name;
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public int getType() {
		return type;
	}
}

// End Property.java
