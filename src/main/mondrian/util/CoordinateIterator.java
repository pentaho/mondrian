/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2009-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.Iterator;

/**
 * Iterator over the coordinates of a hyper-rectangle.
 *
 * <p>For example, {@code new CoordinateIterator(new int[] {3, 2})} generates
 * the pairs {@code {0, 0}, {0, 1}, {1, 0}, {1, 1}, {2, 0}, {2, 1} }.
 *
 * @author jhyde
 * @version $Id$
 * @since Apr 7, 2009
 */
public class CoordinateIterator implements Iterator<int[]> {
    private final int[] dimensions;
    private final int[] current;
    private boolean hasNext;

    /**
     * Creates a coordinate iterator.
     *
     * @param dimensions Array containing the number of elements of each
     * coordinate axis
     */
    public CoordinateIterator(int[] dimensions) {
        this.dimensions = dimensions;
        this.current = new int[dimensions.length];
        this.hasNext = true;
        for (int dimension : dimensions) {
            if (dimension <= 0) {
                // an axis is empty. no results will be produced
                hasNext = false;
                break;
            }
        }
    }

    public boolean hasNext() {
        return hasNext;
    }

    public int[] next() {
        final int[] result = current.clone();
        moveToNext();
        return result;
    }

    private void moveToNext() {
        int offset = dimensions.length;
        while (offset > 0) {
            --offset;
            int k = ++current[offset];
            if (k < dimensions[offset]) {
                return;
            }
            current[offset] = 0;
        }
        hasNext = false;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

// End CoordinateIterator.java
