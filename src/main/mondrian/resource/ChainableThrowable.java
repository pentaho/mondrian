/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 December, 2001
*/

package mondrian.resource;

/**
 * A {@link Throwable} which implements <code>ChainableThrowable</code> can
 * have another {@link Throwable} chained after it.
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public interface ChainableThrowable {
	/**
	 * Returns the next {@link Throwable} in the chain.  The returned {@link
	 * Throwable} does not necessarily implement {@link ChainableThrowable}.
	 **/
	Throwable getCause();
}

// End ChainableThrowable.java
