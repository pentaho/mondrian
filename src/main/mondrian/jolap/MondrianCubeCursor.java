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
import javax.olap.cursor.CubeCursor;
import javax.olap.cursor.EdgeCursor;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A <code>MondrianCubeCursor</code> is ...
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
		this.cubeView = cubeView;
		for (Iterator pageEdges = cubeView.getPageEdge().iterator(); pageEdges.hasNext();) {
			MondrianEdgeView edgeView = (MondrianEdgeView) pageEdges.next();
			addPageEdge(new MondrianEdgeCursor(this, true, edgeView));
		}
		for (Iterator ordinateEdges = cubeView.getOrdinateEdge().iterator(); ordinateEdges.hasNext();) {
			MondrianEdgeView edgeView = (MondrianEdgeView) ordinateEdges.next();
			addOrdinateEdge(new MondrianEdgeCursor(this, false, edgeView));
		}
	}

	public void setOrdinateEdge(Collection input) throws OLAPException {
		ordinateEdge.set(input);
	}

	public List getOrdinateEdge() throws OLAPException {
		return ordinateEdge.get();
	}

	public void addOrdinateEdge(EdgeCursor input) throws OLAPException {
		ordinateEdge.add(input);
	}

	public void removeOrdinateEdge(EdgeCursor input) throws OLAPException {
		ordinateEdge.remove(input);
	}

	public void addOrdinateEdgeBefore(EdgeCursor before, EdgeCursor input) throws OLAPException {
		ordinateEdge.addBefore(before, input);
	}

	public void addOrdinateEdgeAfter(EdgeCursor before, EdgeCursor input) throws OLAPException {
		ordinateEdge.addAfter(before, input);
	}

	public void moveOrdinateEdgeBefore(EdgeCursor before, EdgeCursor input) throws OLAPException {
		ordinateEdge.moveBefore(before, input);
	}

	public void moveOrdinateEdgeAfter(EdgeCursor before, EdgeCursor input) throws OLAPException {
		ordinateEdge.moveAfter(before, input);
	}

	public void setPageEdge(Collection input) throws OLAPException {
		pageEdge.set(input);
	}

	public Collection getPageEdge() throws OLAPException {
		return pageEdge.get();
	}

	public void addPageEdge(EdgeCursor input) throws OLAPException {
		pageEdge.add(input);
	}

	public void removePageEdge(EdgeCursor input) throws OLAPException {
		pageEdge.remove(input);
	}

	public void synchronizePages() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianCubeCursor.java