/*
 *
 * // This software is subject to the terms of the Eclipse Public License v1.0
 * // Agreement, available at the following URL:
 * // http://www.eclipse.org/legal/epl-v10.html.
 * // You must accept the terms of that agreement to use this software.
 * //
 * // Copyright (C) 2001-2005 Julian Hyde
 * // Copyright (C) 2005-2020 Hitachi Vantara and others
 * // All Rights Reserved.
 * /
 *
 */

package mondrian.olap.fun.sort;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Comparator;

/**
 * A functional for {@link Sorter#partialSort}. Sorts or partially sorts an array in ascending order, using a
 * Comparator.
 *
 * <p>Algorithm: quicksort, or partial quicksort (alias
 * "quickselect"), Hoare/Singleton.  Partial quicksort is quicksort that recurs only on one side, which is thus
 * tail-recursion.  Picks pivot as median of three; falls back on insertion sort for small "subfiles".  Partial
 * quicksort is O(n + m log m), instead of O(n log n), where n is the input size, and m is the desired output size.
 *
 * <p>See D Knuth, Art of Computer Programming, 5.2.2 (Algorithm
 * Q); R. Sedgewick, Algorithms in C, ch 5.  Good summary in http://en.wikipedia.org/wiki/Selection_algorithm
 *
 * <P>TODO: What is the time-cost of this functor and of the nested
 * Comparators?
 */
class Quicksorter<T> {
  // size of smallest set worth a quicksort
  public static final int TOO_SMALL = 8;

  private static final Logger LOGGER =
    LogManager.getLogger( Quicksorter.class );
  private final T[] vec;
  private final Comparator<T> comp;
  private final boolean traced;
  private long partitions, comparisons, exchanges; // stats

  public Quicksorter( T[] vec, Comparator<T> comp ) {
    this.vec = vec;
    this.comp = comp;
    partitions = comparisons = exchanges = 0;
    traced = LOGGER.isDebugEnabled();
  }

  private void traceStats( String prefix ) {
    String sb = prefix + ": "
      + partitions + " partitions, "
      + comparisons + " comparisons, "
      + exchanges + " exchanges.";
    LOGGER.debug( sb );
  }

  // equivalent to operator <
  private boolean less( T x, T y ) {
    comparisons++;
    return comp.compare( x, y ) < 0;
  }

  // equivalent to operator >
  private boolean more( T x, T y ) {
    comparisons++;
    return comp.compare( x, y ) > 0;
  }

  // equivalent to operator >
  private boolean equal( T x, T y ) {
    comparisons++;
    return comp.compare( x, y ) == 0;
  }

  // swaps two items (identified by index in vec[])
  private void swap( int i, int j ) {
    exchanges++;
    T temp = vec[ i ];
    vec[ i ] = vec[ j ];
    vec[ j ] = temp;
  }

  // puts into ascending order three items
  // (identified by index in vec[])
  // REVIEW: use only 2 comparisons??
  private void order3( int i, int j, int k ) {
    if ( more( vec[ i ], vec[ j ] ) ) {
      swap( i, j );
    }
    if ( more( vec[ i ], vec[ k ] ) ) {
      swap( i, k );
    }
    if ( more( vec[ j ], vec[ k ] ) ) {
      swap( j, k );
    }
  }

  // runs a selection sort on the array segment VEC[START .. END]
  private void selectionSort( int start, int end ) {
    for ( int i = start; i < end; ++i ) {
      // pick the min of vec[i, end]
      int pmin = i;
      for ( int j = i + 1; j <= end; ++j ) {
        if ( less( vec[ j ], vec[ pmin ] ) ) {
          pmin = j;
        }
      }
      if ( pmin != i ) {
        swap( i, pmin );
      }
    }
  }

  /**
   * Runs one pass of quicksort on array segment VEC[START .. END], dividing it into two parts, the left side VEC[START
   * .. P] none greater than the pivot value VEC[P], and the right side VEC[P+1 .. END] none less than the pivot value.
   * Returns P, the index of the pivot element in VEC[].
   */

  private int partition( int start, int end ) {
    partitions++;
    assert start <= end;

    // Find median of three (both ends and the middle).
    // TODO: use pseudo-median of nine when array segment is big enough.
    int mid = ( start + end ) / 2;
    order3( start, mid, end );
    if ( end - start <= 2 ) {
      return mid;        // sorted!
    }

    // Now the left and right ends are in place (ie in the correct
    // partition), and will serve as sentinels for scanning. Pick middle
    // as pivot and set it aside, in penultimate position.
    final T pivot = vec[ mid ];
    swap( mid, end - 1 );

    // Scan inward from both ends, swapping misplaced items.
    int left = start + 1;       // vec[start] is in place
    int right = end - 2;        // vec[end - 1] is pivot
    while ( left < right ) {
      // scan past items in correct place, but stop at a pivot value
      // (Sedgewick's idea).
      while ( less( vec[ left ], pivot ) ) {
        ++left;
      }
      while ( less( pivot, vec[ right ] ) ) {
        --right;
      }
      if ( left < right ) {     // found a misplaced pair
        swap( left, right );
        ++left;
        --right;
      }
    }
    if ( ( left == right ) && less( vec[ left ], pivot ) ) {
      ++left;
    }

    // All scanned. Restore pivot to its rightful place.
    swap( left, end - 1 );

    return left;
  }


  // Runs quicksort on VEC[START, END]. Recursive version,
  // TODO: exploit tail recursion
  private void sort( int start, int end ) {
    if ( end - start < TOO_SMALL ) {
      selectionSort( start, end );
      return;
    }

    // Split data, so that left side dominates the right side
    // (but neither is sorted):
    int mid = partition( start, end );
    sort( start, mid - 1 );
    sort( mid + 1, end );
  }

  // Runs quickselect(LIMIT) on VEC[START, END]. Recursive version,
  // TODO: exploit tail recursion, unfold.
  private void select( int limit, int start, int end ) {
    if ( limit == 0 ) {
      return;
    }
    if ( end - start < TOO_SMALL ) {
      selectionSort( start, end );
      return;
    }
    int mid = partition( start, end );
    int leftSize = mid - start + 1;
    if ( limit < leftSize ) {
      // work on the left side, and ignore the right side
      select( limit, start, mid );
    } else {
      limit -= leftSize;
      // work on the right side, but keep the left side
      select( limit, mid + 1, end );
    }
  }

  public void sort() {
    int n = vec.length - 1;
    sort( 0, n );
    if ( traced ) {
      traceStats( "quicksort on " + n + "items" );
    }
  }

  /**
   * puts the LIMIT biggest items at the head, not sorted
   */
  public void select( int limit ) {
    int n = vec.length - 1;
    select( limit, 0, n );
    if ( traced ) {
      traceStats( "quickselect for " + limit + " from" + n + "items" );
    }
  }

  public void partialSort( int limit ) {
    int n = vec.length - 1;
    select( limit, 0, n );
    if ( traced ) {
      traceStats(
        "partial sort: quickselect phase for "
          + limit + "from " + n + "items" );
    }
    sort( 0, limit - 1 );
    if ( traced ) {
      traceStats( "partial sort: quicksort phase on " + n + "items" );
    }
  }
}
