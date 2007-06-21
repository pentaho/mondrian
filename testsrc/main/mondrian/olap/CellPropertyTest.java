/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.olap;

import junit.framework.TestCase;

/**
 * Test for <code>Cell Property<code>
 *
 * @author Shishir
 * @version $Id$
 * @since 08 May, 2007
 */

public class CellPropertyTest extends TestCase {
    private CellProperty cellProperty;

    protected void setUp() throws Exception {
        super.setUp();
        cellProperty = new CellProperty("[Format_String]");
    }

    public void testIsNameEquals(){
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