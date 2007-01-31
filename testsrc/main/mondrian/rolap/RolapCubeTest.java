/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// mkambol, 25 January, 2007
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.olap.*;

/**
 * Unit test for {@link RolapCubeTest}.
 *
 * @author mkambol
 * @since 25 January, 2007
 * @version $Id$
 */
public class RolapCubeTest extends FoodMartTestCase {

    public void testProcessFormatStringAttributeToIgnoreNullFormatString(){
        RolapCube cube = (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
        StringBuilder builder = new StringBuilder();
        cube.processFormatStringAttribute(new MondrianDef.CalculatedMember(), builder);        
        assertEquals(0, builder.length());
    }

    public void testProcessFormatStringAttribute(){
        RolapCube cube = (RolapCube) getConnection().getSchema().lookupCube("Sales", false);
        StringBuilder builder = new StringBuilder();
        MondrianDef.CalculatedMember xmlCalcMember = new MondrianDef.CalculatedMember();
        String format = "FORMAT";
        xmlCalcMember.formatString = format;
        cube.processFormatStringAttribute(xmlCalcMember, builder);
        assertEquals(","+ Util.nl+"FORMAT_STRING = \""+format+"\"", builder.toString());
    }
}
