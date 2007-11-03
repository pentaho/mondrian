/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.test.FoodMartTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

/**
 * <code>NullMemberEmptyRepresentationTest</code> tests the null member
 * custom representation feature with EMPTY value supported via
 * {@link mondrian.olap.MondrianProperties#NullMemberRepresentation} property.
 * Each test runs in a forked jvm to overcome limitations due to static
 * variable initialization
 *
 * @author ajogleka
 * @version $Id$
 */
public class NullMemberEmptyRepresentationTest extends BaseNullMemberRepresentationTest {

    protected String getNullMemberRepresentation() {
        return "";
    }

}
