/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/

package mondrian.olap.type;

/**
 * The type of a null expression.
 *
 * @author medstat
 * @since Aug 21, 2006
 */
public class NullType extends ScalarType
{
    /**
     * Creates a null type.
     */
    public NullType()
    {
        super("<NULLTYPE>");
    }

    public boolean equals(Object obj) {
        return obj instanceof NullType;
    }
}

// End NullType.java
