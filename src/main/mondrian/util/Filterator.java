/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/
package mondrian.util;

import java.util.*;


/**
 * Filtered iterator class: an iterator that includes only elements that are
 * instanceof a specified class.
 *
 * @author jason
 * @version $Id$
 */
public class Filterator<E>
    implements Iterator<E>
{
    //~ Instance fields --------------------------------------------------------

    Class<E> includeFilter;
    Iterator<?> iterator;
    E lookAhead;
    boolean ready;

    //~ Constructors -----------------------------------------------------------

    public Filterator(Iterator<?> iterator, Class<E> includeFilter)
    {
        this.iterator = iterator;
        this.includeFilter = includeFilter;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean hasNext()
    {
        if (ready) {
            // Allow hasNext() to be called repeatedly.
            return true;
        }

        // look ahead to see if there are any additional elements
        try {
            lookAhead = next();
            ready = true;
            return true;
        } catch (NoSuchElementException e) {
            ready = false;
            return false;
        }
    }

    public E next()
    {
        if (ready) {
            E o = lookAhead;
            ready = false;
            return o;
        }

        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (includeFilter.isInstance(o)) {
                return includeFilter.cast(o);
            }
        }
        throw new NoSuchElementException();
    }

    public void remove()
    {
        iterator.remove();
    }
}

// End Filterator.java
