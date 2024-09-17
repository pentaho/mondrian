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

package mondrian.calc;

import mondrian.olap.Member;

import java.util.List;

/**
 * Extension to {@link Iterable} that returns a {@link TupleIterator}.
 *
 * <p>If efficiency is important, call {@link #tupleCursor()} rather than
 * {@link #tupleIterator()} if possible. Because {@link TupleCursor} is a
 * simpler API to implement than {@link TupleIterator}, in some cases the
 * implementation may be more efficient.
 *
 * @author jhyde
 */
public interface TupleIterable extends Iterable<List<Member>> {
    /**
     * Creates an iterator over the contents of this iterable.
     *
     * <p>Always has the same effect as calling {@link #iterator()}.
     *
     * @see #tupleCursor()
     *
     * @return cursor over the tuples returned by this iterable
     */
    TupleIterator tupleIterator();

    /**
     * Creates a cursor over the contents of this iterable.
     *
     * <p>The contents of the cursor will always be the same as those returned
     * by {@link #tupleIterator()}. Because {@link TupleCursor} is a simpler API
     * to implement than {@link TupleIterator}, in some cases the implementation
     * may be more efficient.
     *
     * @return cursor over the tuples returned by this iterable
     */
    TupleCursor tupleCursor();

    /**
     * Returns the number of members in each tuple.
     *
     * @return The number of members in each tuple
     */
    int getArity();

    /**
     * Returns an iterable over the members at a given column.
     *
     * <p>The iteratble returns an interator that is modifiable if and only if
     * this TupleIterable is modifiable.
     *
     * <p>If this {@code TupleIterable} happens to be a {@link TupleList},
     * the method is overridden to return a {@link List}&lt;{@link Member}&gt;.
     *
     * @param column Ordinal of the member in each tuple to project
     * @return Iterable that returns an iterator over members
     * @throws IllegalArgumentException if column is not less than arity
     */
    Iterable<Member> slice(int column);
}

// End TupleIterable.java
