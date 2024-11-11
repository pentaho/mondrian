/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
