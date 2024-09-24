/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
