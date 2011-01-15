/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.*;

/**
 * List that generates the cartesian product of its component lists.
 *
 * @version $Id$
 * @author jhyde
 */
public class CartesianProductList<T>
    extends AbstractList<List<T>>
{
    private final List<List<T>> lists;

    public CartesianProductList(List<List<T>> lists) {
        super();
        this.lists = lists;
    }

    @Override
    public List<T> get(int index) {
        final List<T> result = new ArrayList<T>();
        for (int i = lists.size(); --i >= 0;) {
            final List<T> list = lists.get(i);
            final int size = list.size();
            int y = index % size;
            index /= size;
            result.add(0, list.get(y));
        }
        return result;
    }

    @Override
    public int size() {
        int n = 1;
        for (List<T> list : lists) {
            n *= list.size();
        }
        return n;
    }

    public void getIntoArray(int index, Object[] a) {
        int n = 0;
        for (int i = lists.size(); --i >= 0;) {
            final List<T> list = lists.get(i);
            final int size = list.size();
            int y = index % size;
            index /= size;
            Object t = list.get(y);
            if (t instanceof List) {
                List tList = (List) t;
                for (int j = tList.size(); --j >= 0;) {
                    a[n++] = tList.get(j);
                }
            } else {
                a[n++] = t;
            }
        }
        reverse(a, n);
    }

    private void reverse(Object[] a, int size) {
        for (int i = 0, j = size - 1; i < j; ++i, --j) {
            Object t = a[i];
            a[i] = a[j];
            a[j] = t;
        }
    }
}

// End CartesianProductList.java
