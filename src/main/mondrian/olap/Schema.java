/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 October, 2002
*/
package mondrian.olap;

/**
 * A <code>Schema</code> is a collection of cubes, shared dimensions, and roles.
 */
public interface Schema {

    /**
     * returns the next available ordinal. Ths should not be part of the public
     * API but this is temporary, will be removed with the "compiled expressions".
     */
    int getNextDimensionOrdinal();

    /**
     * Returns the name of this schema.
     * @post return != null
     * @post return.length() > 0
     */
    String getName();
    /**
     * Finds a cube called <code>cube</code> in this schema; if no cube
     * exists, <code>failIfNotFound</code> controls whether to raise an error
     * or return <code>null</code>.
     */
    Cube lookupCube(String cube,boolean failIfNotFound);

    /**
     * Returns a list of all cubes in this schema.
     */
    Cube[] getCubes();

    /**
     * Returns a list of shared dimensions in this schema.
     */
    Hierarchy[] getSharedHierarchies();

    /**
     * Creates a dimension in the given cube by parsing an XML string. The XML
     * string must be either a &lt;Dimension&gt; or a &lt;DimensionUsage&gt;.
     * Returns the dimension created.
     */
    Dimension createDimension(Cube cube, String xml);

    /**
     * Creates a cube by parsing an XML string. Returns the cube created.
     */
    Cube createCube(String xml);

    /**
     * Creates a {@link SchemaReader} without any access control.
     */
    SchemaReader getSchemaReader();

    /**
     * Finds a role with a given name in the current catalog, or returns
     * <code>null</code> if no such role exists.
     */
    Role lookupRole(String role);

    /**
     * Returns this schema's function table.
     */
    FunTable getFunTable();
}

// End Schema.java
