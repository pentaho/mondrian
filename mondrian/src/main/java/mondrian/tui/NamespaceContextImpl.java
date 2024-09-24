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

import java.util.*;
import javax.xml.namespace.NamespaceContext;

public class NamespaceContextImpl implements NamespaceContext {

    private final Map<String, String> prefixUriMap;

    public NamespaceContextImpl(Map<String, String> prefixUriMap) {
        if (prefixUriMap == null) {
            throw new IllegalArgumentException("Must define map");
        }
        this.prefixUriMap = Collections.unmodifiableMap(prefixUriMap);
    }

    @Override
    public String getNamespaceURI(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("Prefix cannot be null.");
        }
        return prefixUriMap.get(prefix);
    }

    @Override
    public String getPrefix(String namespaceURI) {
        // arbitrarily returns the first associated w/ uri
        // (consistent w/ spec)
        Iterator iter = getPrefixes(namespaceURI);
        return iter.hasNext() ? (String) iter.next() : null;
    }

    @Override
    public Iterator getPrefixes(String namespaceURI) {
        if (namespaceURI == null) {
            throw new IllegalArgumentException("Namespace URI cannot be null.");
        }
        List<String> uris = new ArrayList<>();
        for (Map.Entry<String, String> entry : prefixUriMap.entrySet()) {
            if (namespaceURI.equals(entry.getValue())) {
                uris.add(entry.getKey());
            }
        }
        return uris.iterator();
    }
}
// End NamespaceContextImpl.java
