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

import javax.olap.query.querycoremodel.Ordinate;
import javax.olap.query.calculatedmembers.CalculatedMember;
import javax.olap.query.calculatedmembers.OperatorInput;
import javax.olap.query.enumerations.OperatorInputType;
import javax.olap.OLAPException;
import java.util.Collection;

/**
 * A <code>OrdinateSupport</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class OrdinateSupport extends QueryObjectSupport implements Ordinate {
	OrdinateSupport() {
		super(false);
	}

	public void setCalculatedMember(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getCalculatedMember() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeCalculatedMember(CalculatedMember input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setOperatorInputs(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getOperatorInputs() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeOperatorInputs(OperatorInput input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public CalculatedMember createCalculatedMember() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public CalculatedMember createCalculatedMemberBefore(CalculatedMember member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public CalculatedMember createCalculatedMemberAfter(CalculatedMember member) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public OperatorInput createOperatorInput(OperatorInputType type) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End OrdinateSupport.java