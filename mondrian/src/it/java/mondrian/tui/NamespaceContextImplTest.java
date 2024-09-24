/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.tui;

import junit.framework.TestCase;

import javax.xml.namespace.NamespaceContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NamespaceContextImplTest extends TestCase {

    Map<String, String> namespaces = new HashMap<>();
    NamespaceContext namespaceContext;

    public void setUp() {
        namespaces.put("FOO", "http://foo.bar.baz");
        namespaces.put("BAR", "http://foo.bar.baz");
        namespaces.put("BAZ", "http://schema.other");
        namespaces.put("BOP", "http://schema.other2");
        namespaceContext =  new NamespaceContextImpl(namespaces);
    }

    public void testGetNamespaceURI() throws Exception
    {
        assertEquals(
            "http://foo.bar.baz",
            namespaceContext.getNamespaceURI("FOO"));
        assertEquals(
            "http://foo.bar.baz",
            namespaceContext.getNamespaceURI("BAR"));
        assertEquals(
            "http://schema.other",
            namespaceContext.getNamespaceURI("BAZ"));
    }

    public void testGetPrefix() throws Exception {
        String prefix = namespaceContext.getPrefix("http://foo.bar.baz");
        // will arbitrarily get one when more than one prefix maps
        assertTrue(prefix.equals("FOO") || prefix.equals("BAR"));
    }

    public void testGetPrefixNullArg() throws Exception {
        try {
            namespaceContext.getPrefix(null);
            fail("expected exception");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    public void testGetPrefixes() throws Exception {
        Iterator iter = namespaceContext.getPrefixes("http://foo.bar.baz");
        assertTrue(iter.hasNext());
        List<String> list = new ArrayList<>();
        list.add((String)iter.next());
        list.add((String)iter.next());
        assertFalse(iter.hasNext());
        assertTrue(list.contains("FOO"));
        assertTrue(list.contains("BAR"));
    }

}
// End NamespaceContextImplTest.java