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

import javax.olap.query.dimensionfilters.ExceptionMemberFilter;
import javax.olap.query.dimensionfilters.DataBasedMemberFilterInput;
import javax.olap.query.enumerations.OperatorType;
import javax.olap.query.enumerations.DataBasedMemberFilterInputType;
import javax.olap.query.enumerations.DataBasedMemberFilterInputTypeEnum;
import javax.olap.OLAPException;

/**
 * A <code>MondrianExceptionMemberFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianExceptionMemberFilter extends MondrianDataBasedMemberFilter implements ExceptionMemberFilter {
	private OperatorType op;
	private Object rhs;

	public MondrianExceptionMemberFilter(MondrianDimensionStepManager manager) {
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

}

// End MondrianExceptionMemberFilter.java