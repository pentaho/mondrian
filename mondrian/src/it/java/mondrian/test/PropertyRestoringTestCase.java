/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.test;

import junit.framework.TestCase;

/**
 * @author Andrey Khayrutdinov
 */
public class PropertyRestoringTestCase extends TestCase {

    public PropertyRestoringTestCase() {
    }

    public PropertyRestoringTestCase(String name) {
        super(name);
    }

    /**
     * Access properties via this object and their values will be reset on
     * {@link #tearDown()}.
     */
    protected final PropertySaver propSaver = new PropertySaver();

    @Override
    protected void tearDown() throws Exception {
        // revert any properties that have been set during this test
        propSaver.reset();
    }
}

// End PropertyRestoringTestCase.java
