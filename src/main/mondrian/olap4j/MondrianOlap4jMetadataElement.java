/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.olap.OlapElement;

import org.olap4j.OlapWrapper;

import java.sql.SQLException;

/**
 * Basic features of metadata elements in Mondrian's olap4j driver.
 *
 * @author jhyde
 */
abstract class MondrianOlap4jMetadataElement
    implements OlapWrapper
{
    /**
     * Helper for {@link #unwrap(Class)} and {@link #isWrapperFor(Class)}.
     *
     * @param iface Desired interface
     * @param <T> Type
     * @return This as desired interface, or null
     */
    protected <T> T unwrapImpl(Class<T> iface) {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        final OlapElement element = getOlapElement();
        if (element != null && iface.isInstance(element)) {
            return iface.cast(element);
        } else {
            return null;
        }
    }

    /**
     * Returns the Mondrian metadata element inside this wrapper, or null if
     * there is none.
     *
     * @return The Mondrian metadata element, if any
     */
    protected abstract OlapElement getOlapElement();

    public <T> T unwrap(Class<T> iface) throws SQLException {
        final T t = unwrapImpl(iface);
        if (t == null) {
            throw new SQLException("not a wrapper for " + iface);
        }
        return t;
    }

    public boolean isWrapperFor(Class<?> iface) {
        return unwrapImpl(iface) != null;
    }
}

// End MondrianOlap4jMetadataElement.java
