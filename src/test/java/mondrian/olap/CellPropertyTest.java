/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
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
