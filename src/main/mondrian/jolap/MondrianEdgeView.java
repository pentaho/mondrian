/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
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
 * Implementation of {@link EdgeView}.
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

    public void setPageOwner(CubeView value) throws OLAPException {
        pageOwner = (MondrianCubeView) value;
    }

	public CubeView getOrdinateOwner() throws OLAPException {
		return ordinateOwner;
	}

    public void setOrdinateOwner(CubeView value) throws OLAPException {
        ordinateOwner = (MondrianCubeView) value;
    }

    public List getDimensionView() throws OLAPException {
        return dimensionView;
	}

    public Collection getEdgeCursor() throws OLAPException {
        return edgeCursor;
	}

    public List getSegment() throws OLAPException {
        return segment;
	}

    public List getEdgeFilter() throws OLAPException {
        return edgeFilter;
	}

    public Collection getTuple() throws OLAPException {
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
