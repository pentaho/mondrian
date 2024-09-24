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

import java.util.ArrayList;
import java.util.EmptyStackException;

/**
 * Stack implementation based on {@link java.util.ArrayList}.
 *
 * <p>More efficient than {@link java.util.Stack}, which extends
 * {@link java.util.Vector} and is
 * therefore synchronized whether you like it or not.
 *
 * @param <E> Element type
 *
 * @author jhyde
 */
public class ArrayStack<E> extends ArrayList<E> {
    /**
     * Default constructor.
     */
    public ArrayStack() {
        super();
    }

    /**
     * Copy Constructor
     * @param toCopy Instance of {@link ArrayStack} to copy.
     */
    public ArrayStack(ArrayStack<E> toCopy) {
        super();
        this.addAll(toCopy);
    }

    /**
     * Analogous to {@link java.util.Stack#push}.
     */
    public E push(E item) {
        add(item);
        return item;
    }

    /**
     * Analogous to {@link java.util.Stack#pop}.
     */
    public E pop() {
        int len = size();
        E obj = peek();
        remove(len - 1);
        return obj;
    }

    /**
     * Analogous to {@link java.util.Stack#peek}.
     */
    public E peek() {
        int len = size();
        if (len <= 0) {
            throw new EmptyStackException();
        }
        return get(len - 1);
    }
}

// End ArrayStack.java
