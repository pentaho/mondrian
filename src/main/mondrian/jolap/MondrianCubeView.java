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
import javax.olap.metadata.Cube;
import javax.olap.query.calculatedmembers.CalculationRelationship;
import javax.olap.query.querycoremodel.CubeView;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.EdgeView;
import javax.olap.query.querycoremodel.Ordinate;
import java.util.Collection;
import java.util.List;

/**
 * A <code>MondrianCubeView</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianCubeView extends QueryObjectSupport implements CubeView {
	private OrderedRelationshipList pageEdge = new OrderedRelationshipList(Meta.pageEdge);
	private OrderedRelationshipList ordinateEdge = new OrderedRelationshipList(Meta.ordinateEdge);
	private OrderedRelationshipList defaultOrdinatePrecedence = new OrderedRelationshipList(Meta.defaultOrdinatePrecedence);
	private RelationshipList cubeCursor = new RelationshipList(Meta.cubeCursor);
	MondrianJolapConnection connection;
	Cube cube;

	static abstract class Meta {
		static final Relationship pageEdge = new Relationship(MondrianCubeCursor.class, "pageEdge", EdgeView.class, "pageOwner");
		public static Relationship ordinateEdge = new Relationship(MondrianCubeView.class, "ordinateEdge", EdgeView.class, "ordinateOwner");
		public static Relationship defaultOrdinatePrecedence = new Relationship(MondrianCubeView.class, "defaultOrdinatePrecedence", Ordinate.class);
		public static Relationship cubeCursor = new Relationship(MondrianCubeView.class, "cubeCursor", CubeCursor.class);
	}

	public MondrianCubeView(MondrianJolapConnection connection, Cube cube) {
		super(true);
		this.connection = connection;
		this.cube = cube;
	}

	public void setOrdinateEdge(Collection input) throws OLAPException {
		ordinateEdge.set(input);
	}

	public List getOrdinateEdge() throws OLAPException {
		return ordinateEdge.get();
	}

	public void removeOrdinateEdge(EdgeView input) throws OLAPException {
		ordinateEdge.remove(input);
	}

	public void moveOrdinateEdgeBefore(EdgeView before, EdgeView input) throws OLAPException {
		ordinateEdge.moveBefore(before, input);
	}

	public void moveOrdinateEdgeAfter(EdgeView before, EdgeView input) throws OLAPException {
		ordinateEdge.moveAfter(before, input);
	}

	public void setPageEdge(Collection input) throws OLAPException {
		pageEdge.set(input);
	}

	public Collection getPageEdge() throws OLAPException {
		return pageEdge.get();
	}

	public void removePageEdge(EdgeView input) throws OLAPException {
		pageEdge.remove(input);
	}

	public void setDefaultOrdinatePrecedence(Collection input) throws OLAPException {
		defaultOrdinatePrecedence.set(input);
	}

	public List getDefaultOrdinatePrecedence() throws OLAPException {
		return defaultOrdinatePrecedence.get();
	}

	public void addDefaultOrdinatePrecedence(Ordinate input) throws OLAPException {
		defaultOrdinatePrecedence.add(input);
	}

	public void removeDefaultOrdinatePrecedence(Ordinate input) throws OLAPException {
		defaultOrdinatePrecedence.remove(input);
	}

	public void addDefaultOrdinatePrecedenceBefore(Ordinate before, Ordinate input) throws OLAPException {
		defaultOrdinatePrecedence.addBefore(before, input);
	}

	public void addDefaultOrdinatePrecedenceAfter(Ordinate before, Ordinate input) throws OLAPException {
		defaultOrdinatePrecedence.addAfter(before, input);
	}

	public void moveDefaultOrdinatePrecedenceBefore(Ordinate before, Ordinate input) throws OLAPException {
		defaultOrdinatePrecedence.moveBefore(before, input);
	}

	public void moveDefaultOrdinatePrecedenceAfter(Ordinate before, Ordinate input) throws OLAPException {
		defaultOrdinatePrecedence.moveAfter(before, input);
	}

	public void setCubeCursor(Collection input) throws OLAPException {
		cubeCursor.set(input);
	}

	public Collection getCubeCursor() throws OLAPException {
		return cubeCursor.get();
	}

	public void removeCubeCursor(CubeCursor input) throws OLAPException {
		cubeCursor.remove(input);
	}

	public CubeCursor createCursor() throws OLAPException {
		return (CubeCursor) cubeCursor.addNew(new MondrianCubeCursor(this));
	}

	public EdgeView createOrdinateEdge() throws OLAPException {
		return (EdgeView) ordinateEdge.addNew(new MondrianEdgeView(this, false));
	}

	public EdgeView createPageEdge() throws OLAPException {
		return (EdgeView) pageEdge.addNew(new MondrianEdgeView(this, true));
	}

	public EdgeView createPageEdgeBefore(EdgeView member) throws OLAPException {
		return (EdgeView) pageEdge.addBefore(member, new MondrianEdgeView(this, true));
	}

	public EdgeView createPageEdgeAfter(EdgeView member) throws OLAPException {
		return (EdgeView) pageEdge.addAfter(member, new MondrianEdgeView(this, true));
	}

	public CalculationRelationship createCalculationRelationship() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void pivot(DimensionView dv, EdgeView source, EdgeView target) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void pivot(DimensionView dv, EdgeView source, EdgeView target, int position) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void rotate(EdgeView edv1, EdgeView edv2) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianCubeView.java