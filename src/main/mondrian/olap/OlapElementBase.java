/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
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
        extends ExpBase
        implements OlapElement {

    private String caption = null;

    protected OlapElementBase() {
    }

    protected abstract Logger getLogger();

    public boolean equals(Object o) {
        return (o instanceof OlapElement) &&
                equals((OlapElement) o);
    }

    public boolean equals(OlapElement mdxElement) {
        return getClass() == mdxElement.getClass() &&
                getUniqueName().equalsIgnoreCase(mdxElement.getUniqueName());
    }

    public int hashCode() {
        int i = (getClass().hashCode() << 8),
                j = getUniqueName().hashCode(),
                k = i ^ j;
        return k;
    }


    public String toString() {
        return getUniqueName();
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.visit(this);
    }

    public Exp accept(Validator validator) {
        return this;
    }

    // implement ExpBase
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

    public boolean dependsOn(Dimension dimension) {
        // A catalog element is constant, and therefore will evaluate to the
        // same result regardless of the current evaluation context. For
        // example, the member [Gender].[M] does not 'depend on' the [Gender]
        // dimension.
        return false;
    }
}

// End OlapElementBase.java
