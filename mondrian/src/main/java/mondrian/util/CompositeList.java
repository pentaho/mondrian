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
