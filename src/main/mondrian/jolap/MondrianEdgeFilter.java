/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 26, 2002
*/
package mondrian.jolap;

import javax.olap.query.enumerations.EdgeFilterType;
import javax.olap.OLAPException;

/**
 * A <code>MondrianEdgeFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 26, 2002
 * @version $Id$
 **/
class MondrianEdgeFilter {
	/**
	 * Factory method.
	 */
	static MondrianEdgeFilter create(EdgeFilterType type) throws OLAPException {
		throw new UnsupportedOperationException("Unknown type " + type);
	}
}

// End MondrianEdgeFilter.java