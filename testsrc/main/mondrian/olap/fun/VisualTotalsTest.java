/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;

import junit.framework.TestCase;

/**
 * <code>VisualTotalsTest</code> tests the internal functions defined in
 * {@link VisualTotalsFunDef}. Right now, only tests substitute().
 *
 * @author efine
 * @version $Id$
 */
public class VisualTotalsTest extends TestCase {
    public void testSubstituteEmpty() {
        final String actual = VisualTotalsFunDef.substitute("", "anything");
        final String expected = "";
        assertEquals(expected, actual);
    }
    
    public void testSubstituteOneStarOnly() {
        final String actual = VisualTotalsFunDef.substitute("*", "anything");
        final String expected = "anything";
        assertEquals(expected, actual);
    }
    
    public void testSubstituteOneStarBegin() {
        final String actual = VisualTotalsFunDef.substitute("* is the word.", "Grease");
        final String expected = "Grease is the word.";
        assertEquals(expected, actual);
    }
    
    public void testSubstituteOneStarEnd() {
        final String actual = VisualTotalsFunDef.substitute("Lies, damned lies, and *!", "statistics");
        final String expected = "Lies, damned lies, and statistics!";
        assertEquals(expected, actual);
    }
    
    public void testSubstituteTwoStars() {
        final String actual = VisualTotalsFunDef.substitute("**", "anything");
        final String expected = "*";
        assertEquals(expected, actual);
    }
    
    public void testSubstituteCombined() {
        final String actual = VisualTotalsFunDef.substitute("*: see small print**** for *", "disclaimer");
        final String expected = "disclaimer: see small print** for disclaimer";
        assertEquals(expected, actual);
    }
}

// End VisualTotalsTest.java
