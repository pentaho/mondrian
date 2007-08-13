/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import org.apache.log4j.Logger;

/**
 * <code>OlapElementBase</code> is an abstract base class for implementations of
 * {@link OlapElement}.
 *
 * @author jhyde
 * @version $Id$
 * @since 6 August, 2001
 */
public abstract class OlapElementBase
        implements OlapElement {

    private String caption = null;

    // cache hash-code because it is often used and elements are immutable
    private int hash;

    protected OlapElementBase() {
    }

    protected abstract Logger getLogger();

    public boolean equals(Object o) {
        return (o == this) ||
            ((o instanceof OlapElement) && equals((OlapElement) o));
    }

    public boolean equals(OlapElement mdxElement) {
        return mdxElement != null &&
                getClass() == mdxElement.getClass() &&
                getUniqueName().equalsIgnoreCase(mdxElement.getUniqueName());
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

    /**
     * Returns the display name of this catalog element.
     * If no caption is defined, the name is returned.
     */
    public String getCaption() {
        if (caption != null) {
            return caption;
        } else {
            return getName();
        }
    }

    /**
     * Sets the display name of this catalog element.
     */
    public void setCaption(String caption) {
        this.caption = caption;
    }
}

// End OlapElementBase.java
