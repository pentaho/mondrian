/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 25, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;

/**
 * Provides methods for cursor-like navigation ({@link #next}, {@link
 * #previous}, {@link #isBeforeFirst}, and so forth) over an array of objects.
 *
 * @author jhyde
 * @since Dec 25, 2002
 * @version $Id$
 **/
class ArrayNavigator {
	private Object[] a;
	private int position;

	public ArrayNavigator(Object[] a) {
		super();
		this.a = a;
		this.position = -1;
	}

	public boolean next() throws OLAPException {
		position++;
		if (position < a.length) {
			return true;
		} else {
			position = a.length;
			return false;
		}
	}

	public boolean previous() throws OLAPException {
		position--;
		if (position >= 0) {
			return true;
		} else {
			position = -1;
			return false;
		}
	}

	public boolean relative(int arg0) throws OLAPException {
		position += arg0;
		if (position >= a.length) {
			position = a.length;
			return false;
		} else if (position < 0) {
			position = 0;
			return false;
		} else {
			return true;
		}
	}

	public boolean first() throws OLAPException {
		if (a.length > 0) {
			position = 0;
			return true;
		} else {
			return false;
		}
	}

	public boolean last() throws OLAPException {
		if (a.length > 0) {
			position = a.length - 1;
			return true;
		} else {
			return false;
		}
	}

	public boolean isBeforeFirst() {
		return position < 0;
	}

	public boolean isAfterLast() throws OLAPException {
		return position >= a.length;
	}

	public boolean isFirst() throws OLAPException {
		return position == 0;
	}

	public boolean isLast() throws OLAPException {
		return position == a.length - 1;
	}

	public void afterLast() throws OLAPException {
		position = a.length;
	}

	public void beforeFirst() throws OLAPException {
		position = -1;
	}

	// todo: spec: what happens if position is out of range?
	public void setPosition(long position) throws OLAPException {
		this.position = (int) position;
	}

	public long getPosition() throws OLAPException {
		return position;
	}

	/**
	 * Returns the object representing the current row. Throws an exception if
	 * the cursor is not positioned properly.
	 *
	 * todo: spec: what should happen if they call say getString(4) and the
	 * cursor is not positioned
	 */
	protected Object current() {
		return a[position];
	}
}

// End ArrayCursor.java
