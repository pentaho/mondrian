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

import javax.olap.cursor.EdgeCursor;
import javax.olap.cursor.DimensionCursor;
import javax.olap.cursor.CubeCursor;
import javax.olap.query.EdgeView;
import javax.olap.query.querycoremodel.Segment;
import javax.olap.OLAPException;
import java.util.Collection;
import java.util.List;
import java.util.Iterator;

/**
 * A <code>MondrianEdgeCursor</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianEdgeCursor extends CursorSupport implements EdgeCursor {
	private MondrianCubeCursor pageOwner;
	private MondrianCubeCursor ordinateOwner;
	private OrderedRelationshipList dimensionCursor = new OrderedRelationshipList(Meta.dimensionCursor);

	static class Meta {
		static Relationship dimensionCursor = new Relationship(MondrianEdgeCursor.class, "dimensionCursor", DimensionCursor.class);
	}

	public MondrianEdgeCursor(MondrianCubeCursor cubeCursor, boolean isPage, MondrianEdgeView edgeView) throws OLAPException {
		super();
		if (isPage) {
			this.pageOwner = cubeCursor;
		} else {
			this.ordinateOwner = cubeCursor;
		}
		for (Iterator dimensionViews = edgeView.getDimensionView().iterator(); dimensionViews.hasNext();) {
			MondrianDimensionView dimensionView = (MondrianDimensionView) dimensionViews.next();
			dimensionCursor.add(new MondrianDimensionCursor(dimensionView));
		}
	}

	public void setDimensionCursor(Collection input) throws OLAPException {
		dimensionCursor.set(input);
	}

	public List getDimensionCursor() throws OLAPException {
		return dimensionCursor.get();
	}

	public void addDimensionCursor(DimensionCursor input) throws OLAPException {
		dimensionCursor.add(input);
	}

	public void removeDimensionCursor(DimensionCursor input) throws OLAPException {
		dimensionCursor.remove(input);
	}

	public void addDimensionCursorBefore(DimensionCursor before, DimensionCursor input) throws OLAPException {
		dimensionCursor.addBefore(before, input);
	}

	public void addDimensionCursorAfter(DimensionCursor before, DimensionCursor input) throws OLAPException {
		dimensionCursor.addAfter(before, input);
	}

	public void moveDimensionCursorBefore(DimensionCursor before, DimensionCursor input) throws OLAPException {
		dimensionCursor.moveBefore(before, input);
	}

	public void moveDimensionCursorAfter(DimensionCursor before, DimensionCursor input) throws OLAPException {
		dimensionCursor.moveAfter(before, input);
	}

	public void setPageOwner(CubeCursor input) throws OLAPException {
		this.pageOwner = (MondrianCubeCursor) input;
	}

	public CubeCursor getPageOwner() throws OLAPException {
		return pageOwner;
	}

	public void setOrdinateOwner(CubeCursor input) throws OLAPException {
		this.ordinateOwner = (MondrianCubeCursor) input;
	}

	public CubeCursor getOrdinateOwner() throws OLAPException {
		return ordinateOwner;
	}

	public void setCurrentSegment(Segment input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Segment getCurrentSegment() throws OLAPException {
		throw new UnsupportedOperationException();
	}

}

// End MondrianEdgeCursor.java