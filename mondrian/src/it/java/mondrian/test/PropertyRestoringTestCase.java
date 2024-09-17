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
