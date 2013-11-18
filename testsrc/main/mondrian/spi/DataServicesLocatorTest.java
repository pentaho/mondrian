/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.rolap.DefaultDataServicesProvider;

import junit.framework.TestCase;

import static mondrian.spi.DataServicesLocator.*;

public class DataServicesLocatorTest extends TestCase {
    public void testEmptyNameReturnsDefaultProvider() {
        assertDefaultProvider(null);
        assertDefaultProvider("");
    }

    public void testUnrecognizedNameThrowsException() {
        try {
            getDataServicesProvider("somename");
            fail("Expected exception");
        } catch (Exception e) {
            assertEquals(
                "Unrecognized Service Provider: somename", e.getMessage());
        }
    }

    public void testLocatesValidProvider() {
        DataServicesProvider provider =
            getDataServicesProvider("mondrian.spi.FakeDataServicesProvider");
        assertTrue(provider instanceof FakeDataServicesProvider);
    }

    private void assertDefaultProvider(String providerName) {
        DataServicesProvider provider = getDataServicesProvider(providerName);
        assertTrue(
            "Expected Default implementation",
            provider instanceof DefaultDataServicesProvider);
    }
}
// End DataServicesLocatorTest.java