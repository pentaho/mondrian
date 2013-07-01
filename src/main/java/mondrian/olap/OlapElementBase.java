/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * <code>OlapElementBase</code> is an abstract base class for implementations of
 * {@link OlapElement}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public abstract class OlapElementBase implements OlapElement {
    protected boolean visible = true;

    // cache hash-code because it is often used and elements are immutable
    private int hash;

    protected OlapElementBase() {
    }

    protected abstract Logger getLogger();

    public boolean equals(Object o) {
        return (o == this)
           || ((o instanceof OlapElement)
               && equals((OlapElement) o));
    }

    public boolean equals(OlapElement mdxElement) {
        return mdxElement != null
           && getClass() == mdxElement.getClass()
           && getUniqueName().equalsIgnoreCase(mdxElement.getUniqueName());
    }

    public int hashCode() {
        if (hash == 0) {
            hash = computeHashCode();
        }
        return hash;
    }

    /**
     * Computes this object's hash code. Called at most once.
     *
     * @return hash code
     */
    protected int computeHashCode() {
        return (getClass().hashCode() << 8) ^ getUniqueName().hashCode();
    }

    public String toString() {
        return getUniqueName();
    }

    public Object clone() {
        return this;
    }

    public String getCaption() {
        return Larders.getCaption(this, getLarder());
    }

    public boolean isVisible() {
        return visible;
    }

    public String getLocalized(LocalizedProperty prop, Locale locale) {
        return Larders.get(this, getLarder(), prop, locale);
    }

    public Map<String, Annotation> getAnnotationMap() {
        return getLarder().getAnnotationMap();
    }

    public abstract Larder getLarder();
}

// End OlapElementBase.java
