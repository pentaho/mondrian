/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.olap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for <code>Cell Property<code>.
 *
 * @author Shishir
 * @since 08 May, 2007
 */
public class CellPropertyTest{
    private CellProperty cellProperty;

    @BeforeEach
    protected void setUp() throws Exception {

        cellProperty = new CellProperty(Id.Segment.toList("Format_String"));
    }

    @Test
    public void testIsNameEquals() {
        assertTrue(cellProperty.isNameEquals("Format_String"));
    }

    @Test
    public void testIsNameEqualsDoesCaseInsensitiveMatch() {
        assertTrue(cellProperty.isNameEquals("format_string"));
    }

    @Test
    public void testIsNameEqualsParameterShouldNotBeQuoted() {
        assertFalse(cellProperty.isNameEquals("[Format_String]"));
    }

}

// End CellPropertyTest.java
