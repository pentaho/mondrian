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

import org.omg.cwm.objectmodel.core.Attribute;

import javax.olap.query.querycoremodel.SelectedObject;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.AttributeReference;
import javax.olap.query.querycoremodel.Ordinate;
import javax.olap.query.calculatedmembers.OrdinateOperator;
import javax.olap.query.dimensionfilters.DataBasedMemberFilter;
import javax.olap.OLAPException;

/**
 * A <code>MondrianAttributeReference</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianAttributeReference extends QueryObjectSupport
		implements AttributeReference {
	private MondrianDimensionView dimensionView;
	private Attribute attribute;

	public MondrianAttributeReference(MondrianDimensionView dimensionView) {
		super(false);
		this.dimensionView = dimensionView;
	}

	public DimensionView getOwner() throws OLAPException {
		return dimensionView;
	}

	public void setAttribute(Attribute attribute) throws OLAPException {
		this.attribute = attribute;
	}

	public Attribute getAttribute() throws OLAPException {
		return attribute;
	}

	public Ordinate getOrdinate() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setOrdinateOperator(OrdinateOperator input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public OrdinateOperator getOrdinateOperator() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setDataBasedMemberFilter(DataBasedMemberFilter input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DataBasedMemberFilter getDataBasedMemberFilter() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianAttributeReference.java