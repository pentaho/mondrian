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

import javax.olap.OLAPException;
import javax.olap.query.dimensionfilters.AttributeFilter;
import javax.olap.query.enumerations.OperatorType;

/**
 * A <code>MondrianAttributeFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianAttributeFilter extends MondrianDimensionFilter
		implements AttributeFilter {
	private OperatorType op;
	private Object rhs;
	private Attribute attribute;

	public MondrianAttributeFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	public OperatorType getOp() throws OLAPException {
		return op;
	}

	public void setOp(OperatorType input) throws OLAPException {
		this.op = input;
	}

	public Object getRhs() throws OLAPException {
		return rhs;
	}

	public void setRhs(Object input) throws OLAPException {
		this.rhs = input;
	}

	public void setAttribute(Attribute input) throws OLAPException {
		this.attribute = input;
	}

	public Attribute getAttribute() throws OLAPException {
		return attribute;
	}
}

// End MondrianAttributeFilter.java