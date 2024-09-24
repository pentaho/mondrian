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

package mondrian.olap;

import junit.framework.TestCase;

/**
 * Test for <code>Cell Property<code>.
 *
 * @author Shishir
 * @since 08 May, 2007
 */
public class CellPropertyTest extends TestCase {
    private CellProperty cellProperty;

    protected void setUp() throws Exception {
        super.setUp();
        cellProperty = new CellProperty(Id.Segment.toList("Format_String"));
    }

    public void testIsNameEquals() {
        assertTrue(cellProperty.isNameEquals("Format_String"));
    }

    public void testIsNameEqualsDoesCaseInsensitiveMatch() {
        assertTrue(cellProperty.isNameEquals("format_string"));
    }

    public void testIsNameEqualsParameterShouldNotBeQuoted() {
        assertFalse(cellProperty.isNameEquals("[Format_String]"));
    }

}

// End CellPropertyTest.java
