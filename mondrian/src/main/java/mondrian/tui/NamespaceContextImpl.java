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
