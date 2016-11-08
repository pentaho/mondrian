/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.calc;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;

import java.util.List;

/**
 * Cheap interface for iterating through the contents of a {@link TupleList}.
 *
 * <p>Stops short of the full {@link java.util.Iterator} interface. If you want
 * that, see {@link TupleIterator}.
 *
 * @author Julian Hyde
 */
public interface TupleCursor {
    void setContext(Evaluator evaluator);

    /**
     * Moves the iterator forward one position.
     *
     * <p>Returns false only when end of data has been reached.
     *
     * <p>Similar to calling the {@link java.util.Iterator} methods
     * {@link java.util.Iterator#hasNext()} followed by
     * {@link java.util.Iterator#next()} but
     * does not construct an object, and is therefore cheaper.
     *
     * <p>If you want to use an Iterator, see {@link TupleIterator}.
     *
     * @return Whether was able to move forward a position
     */
    boolean forward();

    /**
     * Returns the tuple that this cursor is positioned on.
     *
     * <p>This method never returns null, and may safely be called multiple
     * times (or not all) for each position in the iteration.
     *
     * <p>Invalid to call this method when the cursor is has not been
     * positioned, for example, if {@link #forward()} has not been called or
     * if the most recent call to {@code forward} returned {@code false}.
     *
     * @return Current tuple
     */
    List<Member> current();

    /**
     * Returns the number of members in each tuple.
     *
     * @return The number of members in each tuple
     */
    int getArity();

    Member member(int column);

    /**
     * Writes the member(s) of the next tuple to a given offset in an array.
     *
     * <p>This method saves the overhead of a memory allocation when the
     * resulting tuple will be written immediately to an array. The effect of
     * {@code currentToArray(members, 0)} is the same as calling
     * {@code current().toArray(members)}.
     *
     * <p>Before calling this method, you must position the iterator at a valid
     * position. Typically you would call hasNext followed by next; or forward.
     *
     * @param members Members
     * @param offset Offset in the array to write to
     */
    void currentToArray(Member[] members, int offset);
}

// End TupleCursor.java
