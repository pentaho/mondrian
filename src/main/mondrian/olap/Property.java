/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
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

	/** The name of the property which holds the parsed format string (an object
	 * of type {@link Exp}). Internal. **/
	public static final String PROPERTY_FORMAT_EXP = "$format_exp";
	/** The name of the property which holds the aggregation type. This is
	 * automatically set for stored measures, based upon their SQL
	 * aggregation. **/
	public static final String PROPERTY_AGGREGATION_TYPE = "$aggregation_type";
	/** The name of the property which holds a member's name. */
	public static final String PROPERTY_NAME = "$name";
	/** The name of the property which holds a member's caption. */
	public static final String PROPERTY_CAPTION = "$caption";
	/** The name of the property which, for a member of a parent-child hierarchy,
	holds a {@link java.util.List} of its data member and all of its children
	(including non-visible children). */
	public static final String PROPERTY_CONTRIBUTING_CHILDREN = "$contributingChildren";
    /** Cell property for XML/A. */
    public static final String PROPERTY_VALUE = "VALUE";
    /** Cell property for XML/A. */
    public static final String PROPERTY_FORMATTED_VALUE = "FORMATTED_VALUE";
    /** Cell property for XML/A. */
    public static final String PROPERTY_FORMAT_STRING = "FORMAT_STRING";
	/**
	 * A list of the names of properties which have special meaning to the
	 * Mondrian system.
	 */
	public static final String[] systemPropertyNames = {
		PROPERTY_FORMAT_EXP,
		PROPERTY_AGGREGATION_TYPE,
		PROPERTY_NAME,
		PROPERTY_CAPTION,
		PROPERTY_CONTRIBUTING_CHILDREN,
	};
    /**
	 * The various property names which define a format string.
	 */
	static final String[] FORMAT_PROPERTIES = {
		"format", "format_string", "FORMAT", PROPERTY_FORMAT_STRING,
	};
    /** Member property for XML/A. */
    public static final String PROPERTY_MEMBER_UNIQUE_NAME = "MEMBER_UNIQUE_NAME";
    /** Member property for XML/A. */
    public static final String PROPERTY_MEMBER_CAPTION = "MEMBER_CAPTION";
    /** Member property for XML/A. */
    public static final String PROPERTY_LEVEL_UNIQUE_NAME = "LEVEL_UNIQUE_NAME";
    /** Member property for XML/A. */
    public static final String PROPERTY_LEVEL_NUMBER = "LEVEL_NUMBER";

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
	public PropertyFormatter getFormatter() {
		return null;
	}
}

// End Property.java
