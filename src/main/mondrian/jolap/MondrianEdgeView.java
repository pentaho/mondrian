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

import javax.olap.OLAPException;
import javax.olap.cursor.EdgeCursor;
import javax.olap.query.edgefilters.CurrentEdgeMember;
import javax.olap.query.enumerations.EdgeFilterType;
import javax.olap.query.querycoremodel.*;
import java.util.Collection;
import java.util.List;

/**
 * A <code>MondrianEdgeView</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianEdgeView extends OrdinateSupport implements EdgeView {
	private RelationshipList edgeCursor = new RelationshipList(Meta.edgeCursor);
	private OrderedRelationshipList dimensionView = new OrderedRelationshipList(Meta.dimensionView);
	private OrderedRelationshipList segment = new OrderedRelationshipList(Meta.segment);
	private OrderedRelationshipList edgeFilter = new OrderedRelationshipList(Meta.edgeFilter);
	private MondrianCubeView ordinateOwner;
	private MondrianCubeView pageOwner;

	static abstract class Meta {
		static final Relationship dimensionView = new Relationship(MondrianEdgeView.class, "dimensionView", DimensionView.class);
		static final Relationship edgeCursor = new Relationship(MondrianEdgeView.class, "edgeCursor", EdgeCursor.class);
		static final Relationship segment = new Relationship(MondrianEdgeView.class, "segment", Segment.class);
		static final Relationship edgeFilter = new Relationship(MondrianEdgeView.class, "edgeFilter", EdgeFilter.class);
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
		dimensionView.set(input);
	}

	public List getDimensionView() throws OLAPException {
		return dimensionView.get();
	}

	public void addDimensionView(DimensionView input) throws OLAPException {
		dimensionView.add(input);
	}

	public void removeDimensionView(DimensionView input) throws OLAPException {
		dimensionView.remove(input);
	}

	public void addDimensionViewBefore(DimensionView before, DimensionView input) throws OLAPException {
		dimensionView.addBefore(before, input);
	}

	public void addDimensionViewAfter(DimensionView before, DimensionView input) throws OLAPException {
		dimensionView.addAfter(before, input);
	}

	public void moveDimensionViewBefore(DimensionView before, DimensionView input) throws OLAPException {
		dimensionView.moveBefore(before, input);
	}

	public void moveDimensionViewAfter(DimensionView before, DimensionView input) throws OLAPException {
		dimensionView.moveAfter(before, input);
	}

	public void setEdgeCursor(Collection input) throws OLAPException {
		edgeCursor.set(input);
	}

	public Collection getEdgeCursor() throws OLAPException {
		return edgeCursor.get();
	}

	public void removeEdgeCursor(EdgeCursor input) throws OLAPException {
		edgeCursor.remove(input);
	}

	public void setSegment(Collection input) throws OLAPException {
		segment.set(input);
	}

	public Collection getSegment() throws OLAPException {
		return segment.get();
	}

	public void removeSegment(Segment input) throws OLAPException {
		segment.remove(input);
	}

	public void setEdgeFilter(Collection input) throws OLAPException {
		edgeFilter.set(input);
	}

	public List getEdgeFilter() throws OLAPException {
		return edgeFilter.get();
	}

	public void removeEdgeFilter(EdgeFilter input) throws OLAPException {
		edgeFilter.remove(input);
	}

	public void moveEdgeFilterBefore(EdgeFilter before, EdgeFilter input) throws OLAPException {
		edgeFilter.moveBefore(before, input);
	}

	public void moveEdgeFilterAfter(EdgeFilter before, EdgeFilter input) throws OLAPException {
		edgeFilter.moveAfter(before, input);
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
		return (Segment) segment.addNew(new MondrianSegment());
	}

	public Segment createSegmentBefore(Segment member) throws OLAPException {
		return (Segment) segment.addBefore(member, new MondrianSegment());
	}

	public Segment createSegmentAfter(Segment member) throws OLAPException {
		return (Segment) segment.addAfter(member, new MondrianSegment());
	}

	public EdgeFilter createEdgeFilter(EdgeFilterType type) throws OLAPException {
		return (EdgeFilter) edgeFilter.addNew(MondrianEdgeFilter.create(type));
	}

	public EdgeFilter createEdgeFilterBefore(EdgeFilterType type, EdgeFilter member) throws OLAPException {
		return (EdgeFilter) edgeFilter.addBefore(member, MondrianEdgeFilter.create(type));
	}

	public EdgeFilter createEdgeFilterAfter(EdgeFilterType type, EdgeFilter member) throws OLAPException {
		return (EdgeFilter) edgeFilter.addAfter(member, MondrianEdgeFilter.create(type));
	}

	public Tuple createTuple() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public CurrentEdgeMember createCurrentEdgeMember() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianEdgeView.java