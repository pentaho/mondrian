/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import mondrian.olap.Axis;

import javax.olap.OLAPException;
import javax.olap.cursor.CubeCursor;
import javax.olap.cursor.DimensionCursor;
import javax.olap.cursor.EdgeCursor;
import javax.olap.query.querycoremodel.Segment;
import java.util.List;

/**
 * Implementation of {@link EdgeCursor}.
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

    public MondrianEdgeCursor(MondrianCubeCursor cubeCursor, boolean isPage, MondrianEdgeView edgeView, Axis axis) throws OLAPException {
        super(new ArrayNavigator(axis.positions), new Accessor() {
            public Object getObject(int i) throws OLAPException {
                throw new UnsupportedOperationException();
            }
        });
        if (isPage) {
            this.pageOwner = cubeCursor;
        } else {
            this.ordinateOwner = cubeCursor;
        }
        for (int i = 0; i < edgeView.getDimensionView().size(); i++) {
            dimensionCursor.add(new MondrianDimensionCursor(this, axis, i));
        }
    }

    public List getDimensionCursor() throws OLAPException {
        return dimensionCursor;
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
