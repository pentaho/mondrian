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