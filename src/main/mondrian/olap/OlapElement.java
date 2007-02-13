/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;

/**
 * An <code>OlapElement</code> is a catalog object (dimension, hierarchy,
 * level, member).
 */
public interface OlapElement {
    String getUniqueName();
    String getName();
    String getDescription();

    /**
     * Looks up a child element, returning null if it does not exist.
     */
    OlapElement lookupChild(SchemaReader schemaReader, String s);
    OlapElement lookupChild(
        SchemaReader schemaReader, String s, MatchType matchType);

    /**
     * Returns the name of this element qualified by its class, for example
     * "hierarchy 'Customers'".
     */
    String getQualifiedName();

    String getCaption();
    Hierarchy getHierarchy();

    /**
     * Returns the dimension of a this expression, or null if no dimension is
     * defined. Applicable only to set expressions.
     *
     * <p>Example 1:
     * <blockquote><pre>
     * [Sales].children
     * </pre></blockquote>
     * has dimension <code>[Sales]</code>.</p>
     *
     * <p>Example 2:
     * <blockquote><pre>
     * order(except([Promotion Media].[Media Type].members,
     *              {[Promotion Media].[Media Type].[No Media]}),
     *       [Measures].[Unit Sales], DESC)
     * </pre></blockquote>
     * has dimension [Promotion Media].</p>
     *
     * <p>Example 3:
     * <blockquote><pre>
     * CrossJoin([Product].[Product Department].members,
     *           [Gender].members)
     * </pre></blockquote>
     * has no dimension (well, actually it is [Product] x [Gender], but we
     * can't represent that, so we return null);</p>
     */
    Dimension getDimension();
}

// End OlapElement.java
