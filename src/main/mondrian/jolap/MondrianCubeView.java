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
 * Implementation of {@link CubeView}.
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

    public List getOrdinateEdge() throws OLAPException {
        return ordinateEdge;
    }

    public Collection getPageEdge() throws OLAPException {
        return pageEdge;
    }

    public List getDefaultOrdinatePrecedence() throws OLAPException {
        return defaultOrdinatePrecedence;
    }

    public Collection getCubeCursor() throws OLAPException {
        return cubeCursor;
    }

    public CubeCursor createCursor() throws OLAPException {
        return (CubeCursor) cubeCursor.addNew(new MondrianCubeCursor(this));
    }

    public EdgeView createOrdinateEdge() throws OLAPException {
        return (EdgeView) ordinateEdge.addNew(new MondrianEdgeView(this, false));
    }

    public EdgeView createOrdinateEdgeBefore(EdgeView member) throws OLAPException {
        return (EdgeView) ordinateEdge.addBefore(member, new MondrianEdgeView(this, false));
    }

    public EdgeView createOrdinateEdgeAfter(EdgeView member) throws OLAPException {
        return (EdgeView) ordinateEdge.addAfter(member, new MondrianEdgeView(this, false));
    }

    public EdgeView createPageEdge() throws OLAPException {
        return (EdgeView) pageEdge.addNew(new MondrianEdgeView(this, true));
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

    public Collection getCalculationRelationship() throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianCubeView.java
