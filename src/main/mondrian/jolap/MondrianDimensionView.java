/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;
import javax.olap.cursor.DimensionCursor;
import javax.olap.metadata.Dimension;
import javax.olap.query.derivedattribute.DerivedAttribute;
import javax.olap.query.enumerations.SelectedObjectType;
import javax.olap.query.querycoremodel.*;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link DimensionView}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
class MondrianDimensionView extends OrdinateSupport
        implements DimensionView, MeasureView {
    MondrianJolapDimension dimension;
    private OrderedRelationshipList selectedObject = new OrderedRelationshipList(Meta.selectedObject);
    private RelationshipList dimensionStepManager = new RelationshipList(Meta.dimensionStepManager);

    static class Meta {
        static final Relationship selectedObject = new Relationship(MondrianDimensionView.class, "selectedObject", SelectedObject.class);
        static final Relationship dimensionStepManager = new Relationship(MondrianDimensionView.class, "dimensionStepManager", MondrianDimensionStepManager.class);
    }

    public MondrianDimensionView(Dimension dimension) {
        this.dimension = (MondrianJolapDimension) dimension;
    }

    public boolean isDistinct() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setDistinct(boolean input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setEdgeView(EdgeView input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public EdgeView getEdgeView() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setDimension(Dimension dimension) throws OLAPException {
        this.dimension = (MondrianJolapDimension) dimension;
    }

    public Dimension getDimension() throws OLAPException {
        return dimension;
    }

    public Collection getDimensionStepManager() throws OLAPException {
        return dimensionStepManager;
    }

    public Collection getDimensionCursor() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public List getSelectedObject() throws OLAPException {
        return selectedObject;
    }

    public Collection getDerivedAttribute() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public DimensionCursor createCursor() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public SelectedObject createSelectedObject(SelectedObjectType type) throws OLAPException {
        return (SelectedObject) selectedObject.addNew(
                QueryObjectSupport.createSelectedObject(this, type));
    }

    public SelectedObject createSelectedObjectBefore(SelectedObjectType type, SelectedObject member) throws OLAPException {
        return (SelectedObject) selectedObject.addBefore(member,
                QueryObjectSupport.createSelectedObject(this, type));
    }

    public SelectedObject createSelectedObjectAfter(SelectedObjectType type, SelectedObject member) throws OLAPException {
        return (SelectedObject) selectedObject.addAfter(member,
                QueryObjectSupport.createSelectedObject(this, type));
    }

    public DimensionStepManager createDimensionStepManager() throws OLAPException {
        return (DimensionStepManager) dimensionStepManager.addNew(
                new MondrianDimensionStepManager(this));
    }

    public DerivedAttribute createDerivedAttribute() throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianDimensionView.java
