/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import java.util.AbstractList;
import java.util.List;

/**
 * List composed of several lists.
 *
 * @param <T> element type
 *
 * @author jhyde
 */
public class CompositeList<T> extends AbstractList<T> {
    private final List<? extends T>[] lists;

    /**
     * Creates a composite list.
     *
     * @param lists Component lists
     */
    public CompositeList(
        List<? extends T>... lists)
    {
        this.lists = lists;
    }

    /**
     * Creates a composite list, inferring the element type from the arguments.
     *
     * @param lists One or more lists
     * @param <T> element type
     * @return composite list
     */
    public static <T> CompositeList<T> of(
        List<? extends T>... lists)
    {
        return new CompositeList<T>(lists);
    }

    public T get(int index) {
        int n = 0;
        for (List<? extends T> list : lists) {
            int next = n + list.size();
            if (index < next) {
                return list.get(index - n);
            }
            n = next;
        }
        throw new IndexOutOfBoundsException(
            "index" + index + " out of bounds in list of size " + n);
    }

    public int size() {
        int n = 0;
        for (List<? extends T> array : lists) {
            n += array.size();
        }
        return n;
    }
}

// End CompositeList.java
