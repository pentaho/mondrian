/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

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
