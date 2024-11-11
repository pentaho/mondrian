/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.calc;

import mondrian.olap.Member;

import java.util.Iterator;
import java.util.List;

/**
 * Extension to {@link java.util.Iterator} that returns tuples.
 *
 * <p>Extends {@link TupleCursor} to support the standard Java iterator
 * API. For some implementations, using the iterator API (in particular the
 * {@link #next} and {@link #hasNext} methods) may be more expensive than using
 * cursor's {@link #forward} method.
 *
 * @author jhyde
 */
public interface TupleIterator extends Iterator<List<Member>>, TupleCursor {

}

// End TupleIterator.java
