/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

/**
 * The type of a empty expression.
 *
 * <p>An example of an empty expression is the third argument to the call
 * <code>DrilldownLevelTop({[Store].[USA]}, 2, , [Measures].[Unit
 * Sales])</code>.
 * </p>
 *
 * @author medstat
 * @version $Id$
 * @since Jan 26, 2009
 */
public class EmptyType extends ScalarType
{
    /**
     * Creates an empty type.
     */
    public EmptyType()
    {
        super("<EMPTY>");
    }

    public boolean equals(Object obj) {
        return obj instanceof EmptyType;
    }
}

// End EmptyType.java
