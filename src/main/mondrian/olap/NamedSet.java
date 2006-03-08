/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2000-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap;

import mondrian.olap.type.Type;

/**
 * A named set of members or tuples.
 *
 * <p>A set can be defined in a query, using a <code>WITH SET</code> clause,
 * or in a schema. Named sets in a schema can be defined against a particular
 * cube or virtual cube, or shared between all cubes.</p>
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public interface NamedSet extends OlapElement {
    /**
     * Sets the name of this named set.
     */
    void setName(String newName);

    /**
     * Returns the type of this named set.
     */
    Type getType();

    /**
     * Returns the expression used to derive this named set.
     */
    Exp getExp();

    NamedSet validate(Validator validator);
}

// End NamedSet.java
