/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.cursor.DimensionCursor;
import javax.olap.cursor.EdgeCursor;
import javax.olap.OLAPException;
import javax.olap.query.querycoremodel.DimensionStepManager;

/**
 * A <code>MondrianDimensionCursor</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDimensionCursor extends CursorSupport implements DimensionCursor {
	public MondrianDimensionCursor(MondrianDimensionView dimensionView) {
		super();
	}

	public long getEdgeStart() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setEdgeStart(long input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public long getEdgeEnd() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setEdgeEnd(long input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setEdgeCursor(EdgeCursor input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public EdgeCursor getEdgeCursor() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setCurrentDimensionStepManager(DimensionStepManager input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DimensionStepManager getCurrentDimensionStepManager() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianDimensionCursor.java