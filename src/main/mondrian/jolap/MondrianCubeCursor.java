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
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;

import javax.olap.OLAPException;
import javax.olap.cursor.CubeCursor;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link CubeCursor}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianCubeCursor extends CursorSupport implements CubeCursor {
    private RelationshipList pageEdge = new RelationshipList(Meta.pageEdge);
    private OrderedRelationshipList ordinateEdge = new OrderedRelationshipList(Meta.pageEdge);
    private MondrianCubeView cubeView;

    static abstract class Meta {
        static final Relationship pageEdge = new Relationship(MondrianCubeCursor.class, "pageEdge", MondrianEdgeCursor.class, "pageOwner");
        static final Relationship ordinateEdge = new Relationship(MondrianCubeCursor.class, "ordinateEdge", MondrianEdgeCursor.class, "ordinateOwner");
    }

    MondrianCubeCursor(MondrianCubeView cubeView) throws OLAPException {
        super(null, null);
        this.cubeView = cubeView;
        Query query = new Converter().createQuery(cubeView);
        Result result = query.getConnection().execute(query);
        Axis slicerAxis = result.getSlicerAxis();
        Util.assertTrue(cubeView.getPageEdge().size() == 1);
        for (Iterator pageEdges = cubeView.getPageEdge().iterator(); pageEdges.hasNext();) {
            MondrianEdgeView edgeView = (MondrianEdgeView) pageEdges.next();
            pageEdge.add(new MondrianEdgeCursor(this, true, edgeView, slicerAxis));
        }
        Axis[] axes = result.getAxes();
        int axisOffset = 0;
        Util.assertTrue(cubeView.getOrdinateEdge().size() == axes.length);
        for (Iterator ordinateEdges = cubeView.getOrdinateEdge().iterator(); ordinateEdges.hasNext();) {
            MondrianEdgeView edgeView = (MondrianEdgeView) ordinateEdges.next();
            ordinateEdge.add(new MondrianEdgeCursor(this, false, edgeView, axes[axisOffset++]));
        }
    }

    public List getOrdinateEdge() throws OLAPException {
        return ordinateEdge;
    }

    public Collection getPageEdge() throws OLAPException {
        return pageEdge;
    }

    public void synchronizePages() throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianCubeCursor.java
