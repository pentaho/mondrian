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

import mondrian.olap.Util;

import javax.olap.query.querycoremodel.*;
import javax.olap.query.derivedattribute.DerivedAttribute;
import javax.olap.query.enumerations.SelectedObjectType;
import javax.olap.query.enumerations.SelectedObjectTypeEnum;
import javax.olap.OLAPException;
import javax.olap.cursor.DimensionCursor;
import javax.olap.metadata.Dimension;
import java.util.Collection;
import java.util.List;
import java.util.Iterator;

/**
 * A <code>MondrianDimensionView</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianDimensionView extends OrdinateSupport
		implements DimensionView, MeasureView {
	MondrianJolapDimension dimension;
	private OrderedRelationshipList selectedObject = new OrderedRelationshipList(Meta.selectedObject);
	private RelationshipList dimensionStepManager = new RelationshipList(Meta.dimensionStepManager);

	static class Meta {
		static final Relationship selectedObject = new Relationship(MondrianDimensionView.class, "selectedObject", SelectedObject.class);
		static final Relationship dimensionStepManager = new Relationship(MondrianDimensionView.class, "dimensionStepManager", MondrianDimensionStepManager.class);
	}

	public MondrianDimensionView() {
	}

	public Boolean getIsDistinct() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setIsDistinct(Boolean input) throws OLAPException {
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

	public void setDimensionStepManager(Collection input) throws OLAPException {
		dimensionStepManager.set(input);
	}

	public Collection getDimensionStepManager() throws OLAPException {
		return dimensionStepManager.get();
	}

	public void removeDimensionStepManager(DimensionStepManager input) throws OLAPException {
		dimensionStepManager.remove(input);
	}

	public void setDimensionCursor(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getDimensionCursor() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeDimensionCursor(DimensionCursor input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setSelectedObject(Collection input) throws OLAPException {
		selectedObject.set(input);
	}

	public List getSelectedObject() throws OLAPException {
		return selectedObject.get();
	}

	public void removeSelectedObject(SelectedObject input) throws OLAPException {
		selectedObject.remove(input);
	}

	public void moveSelectedObjectBefore(SelectedObject before, SelectedObject input) throws OLAPException {
		selectedObject.moveBefore(before, input);
	}

	public void moveSelectedObjectAfter(SelectedObject before, SelectedObject input) throws OLAPException {
		selectedObject.moveAfter(before, input);
	}

	public void setDerivedAttribute(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getDerivedAttribute() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeDerivedAttribute(DerivedAttribute input) throws OLAPException {
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