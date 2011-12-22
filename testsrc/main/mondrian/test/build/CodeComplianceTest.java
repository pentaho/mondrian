/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test.build;

/**
 * Omnibus code compliance test to wrap various ant tasks that check the code
 * base, such checkFile, as macker, Javadoc, preambles, and so on.
 *
 * @author Chard Nelson
 * @since Sep 8, 2009
 * @version $Id$
 */
public class CodeComplianceTest
    extends AntTestBase
{
    /**
     * Creates a CodeComplianceTest.
     *
     * @param name Test name
     */
    public CodeComplianceTest(String name)
    {
        super(name);
    }

    /**
     * Checks source code file formatting.
     */
    public void testCodeFormatting()  throws Exception
    {
        runAntTest("checkCodeFormatting");
    }

    /**
     * Checks that javadoc can be generated without errors.
     */
    public void testJavadoc() throws Exception
    {
        runAntTest("checkJavadoc");
    }
}

// End CodeComplianceTest.java
