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
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class OlapElementBase
    extends ExpBase
    implements OlapElement
{

    static {
      Util.assertTrue(System.getProperty("java.version").compareTo("1.1") > 0,
      "require at least JDK 1.2, because JDK 1.1 had a severe performance bug when hashing long, similar strings");
    }

    private String caption = null;

    protected OlapElementBase() {
    }           

    protected abstract Logger getLogger();
        
    public boolean equals(Object o)
    {
        return (o instanceof OlapElement) &&
            equals((OlapElement) o);
    }

    public boolean equals(OlapElement mdxElement)
    {
        return getClass() == mdxElement.getClass() &&
            getUniqueName().equalsIgnoreCase(mdxElement.getUniqueName());
    }

    public int hashCode()
    {
        int i = (getClass().hashCode() << 8),
            j = getUniqueName().hashCode(),
            k = i ^ j;
        return k;
    }


    public String toString() {
        return getUniqueName();
    }

    public Object evaluate(Evaluator evaluator)
    {
        return evaluator.xx(this);
    }

    public Exp resolve(Resolver resolver)
    {
        return this;
    }

    // implement ExpBase
    public Object clone()
    {
        return this;
    }

    // return name as default for caption
    public String getCaption() {
        if (caption != null)
            return caption;
        else
            return getName();
    }
    /**
     * @param caption The caption to set.
     */
    public void setCaption(String caption) {
        this.caption = caption;
    }
}

// End OlapElementBase.java
