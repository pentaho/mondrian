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

import javax.olap.query.querycoremodel.*;
import javax.olap.query.enumerations.EdgeFilterType;
import javax.olap.query.edgefilters.CurrentEdgeMember;
import javax.olap.query.querytransaction.TransactionalObject;
import javax.olap.OLAPException;
import javax.olap.cursor.EdgeCursor;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

/**
 * A <code>MondrianEdgeView</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianEdgeView extends OrdinateSupport implements EdgeView {
	private RelationshipList edgeCursors = new RelationshipList(Meta.edgeCursor);
	private OrderedRelationshipList dimensionViews = new OrderedRelationshipList(Meta.dimensionView);
	private MondrianCubeView ordinateOwner;
	private MondrianCubeView pageOwner;

	static abstract class Meta {
		static final Relationship dimensionView = new Relationship(MondrianEdgeView.class, "dimensionView", DimensionView.class);
		static final Relationship edgeCursor = new Relationship(MondrianEdgeView.class, "edgeCursor", EdgeCursor.class);
	}

	public MondrianEdgeView(MondrianCubeView owner, boolean isPage) {
		if (isPage) {
			this.pageOwner = owner;
		} else {
			this.ordinateOwner = owner;
		}
	}

	public CubeView getPageOwner() throws OLAPException {
		return pageOwner;
	}

	public CubeView getOrdinateOwner() throws OLAPException {
		return ordinateOwner;
	}

	public void setDimensionView(Collection input) throws OLAPException {
		dimensionViews.set(input);
	}

	public List getDimensionView() throws OLAPException {
		return dimensionViews.get();
	}

	public void addDimensionView(DimensionView input) throws OLAPException {
		dimensionViews.add(input);
	}

	public void removeDimensionView(DimensionView input) throws OLAPException {
		dimensionViews.remove(input);
	}

	public void addDimensionViewBefore(DimensionView before, DimensionView input) throws OLAPException {
		dimensionViews.addBefore(before, input);
	}

	public void addDimensionViewAfter(DimensionView before, DimensionView input) throws OLAPException {
		dimensionViews.addAfter(before, input);
	}

	public void moveDimensionViewBefore(DimensionView before, DimensionView input) throws OLAPException {
		dimensionViews.moveBefore(before, input);
	}

	public void moveDimensionViewAfter(DimensionView before, DimensionView input) throws OLAPException {
		dimensionViews.moveAfter(before, input);
	}

	public void setEdgeCursor(Collection input) throws OLAPException {
		edgeCursors.set(input);
	}

	public Collection getEdgeCursor() throws OLAPException {
		return edgeCursors.get();
	}

	public void removeEdgeCursor(EdgeCursor input) throws OLAPException {
		edgeCursors.remove(input);
	}

	public void setSegment(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getSegment() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeSegment(Segment input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setEdgeFilter(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public List getEdgeFilter() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeEdgeFilter(EdgeFilter input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void moveEdgeFilterBefore(EdgeFilter before, EdgeFilter input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void moveEdgeFilterAfter(EdgeFilter before, EdgeFilter input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setTuple(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getTuple() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeTuple(Tuple input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public EdgeCursor createCursor() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Segment createSegment() throws OLAPException {
		return new MondrianSegment();
	}

	public Segment createSegmentBefore(Segment member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Segment createSegmentAfter(Segment member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public EdgeFilter createEdgeFilter(EdgeFilterType type) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public EdgeFilter createEdgeFilterBefore(EdgeFilterType type, EdgeFilter member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public EdgeFilter createEdgeFilterAfter(EdgeFilterType type, EdgeFilter member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Tuple createTuple() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public CurrentEdgeMember createCurrentEdgeMember() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianEdgeView.java