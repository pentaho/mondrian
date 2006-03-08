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
import javax.olap.query.calculatedmembers.CalculatedMember;
import javax.olap.query.calculatedmembers.OperatorInput;
import javax.olap.query.enumerations.OperatorInputType;
import javax.olap.query.querycoremodel.Ordinate;
import java.util.Collection;
import java.util.List;

/**
 * Abtract implementation of {@link Ordinate}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
abstract class OrdinateSupport extends QueryObjectSupport implements Ordinate {
    OrdinateSupport() {
        super(false);
    }

    public List getCalculatedMember() throws OLAPException {
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

    public Collection getOperatorInput() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public OperatorInput createOperatorInput(OperatorInputType type) throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End OrdinateSupport.java
