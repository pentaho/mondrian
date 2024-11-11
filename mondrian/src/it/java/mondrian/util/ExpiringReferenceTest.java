/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.util;

import junit.framework.TestCase;

public class ExpiringReferenceTest extends TestCase
{
    public void testSimpleExpiryMode() throws Exception {
        final Object referent = new Object();
        final ExpiringReference<Object> reference =
            new ExpiringReference<Object>(referent, "1s");
        Thread.sleep(500);
        assertNotNull(reference.hardRef);
        Thread.sleep(600);
        assertNull(reference.hardRef);
    }

    public void testExpiryModeReAccess() throws Exception {
        final Object referent = new Object();
        final ExpiringReference<Object> reference =
            new ExpiringReference<Object>(referent, "1s");
        Thread.sleep(500);
        assertNotNull(reference.get("1s"));
        assertNotNull(reference.hardRef);
        Thread.sleep(500);
        assertNotNull(reference.get("1s"));
        assertNotNull(reference.hardRef);
        Thread.sleep(500);
        assertNotNull(reference.get("1s"));
        assertNotNull(reference.hardRef);
        Thread.sleep(1200);
        assertNull(reference.hardRef);
    }

    public void testExpiryModeReAccessWithEmptyGet() throws Exception {
        final Object referent = new Object();
        final ExpiringReference<Object> reference =
            new ExpiringReference<Object>(referent, "1s");
        assertNotNull(reference.hardRef);
        Thread.sleep(500);
        assertNotNull(reference.get());
        assertNotNull(reference.hardRef);
        Thread.sleep(600);
        assertNull(reference.hardRef);
    }

    public void testSimpleSoftMode() throws Exception {
        final Object referent = new Object();
        final ExpiringReference<Object> reference =
            new ExpiringReference<Object>(referent, "-1s");
        assertNull(reference.hardRef);
        assertNotNull(reference.get());
        assertNull(reference.hardRef);
    }

    public void testSimplePermMode() throws Exception {
        final Object referent = new Object();
        final ExpiringReference<Object> reference =
            new ExpiringReference<Object>(referent, "0s");
        assertNotNull(reference.hardRef);
        Thread.sleep(500);
        assertNotNull(reference.hardRef);
        assertNotNull(reference.get());
        assertNotNull(reference.hardRef);
    }

    public void testPermModeFollowedByNonPermGet() throws Exception {
        final Object referent = new Object();
        final ExpiringReference<Object> reference =
            new ExpiringReference<Object>(referent, "0s");
        assertNotNull(reference.hardRef);
        Thread.sleep(500);
        assertNotNull(reference.hardRef);
        assertNotNull(reference.get("1s"));
        Thread.sleep(1100);
        assertNotNull(reference.hardRef);
    }
}

// End ExpiringReferenceTest.java
