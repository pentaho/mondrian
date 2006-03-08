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
import javax.olap.query.calculatedmembers.OperatorInput;
import javax.olap.query.calculatedmembers.OrdinateOperator;
import javax.olap.query.dimensionfilters.DataBasedMemberFilter;
import javax.olap.query.dimensionfilters.DataBasedMemberFilterInput;
import javax.olap.query.querycoremodel.Ordinate;

/**
 * Implementation of {@link DataBasedMemberFilterInput}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
abstract class MondrianDataBasedMemberFilterInput
        implements DataBasedMemberFilterInput, OperatorInput {
    private DataBasedMemberFilter dataBasedMemberFilter;
    private Ordinate ordinate;
    private OrdinateOperator ordinateOperator;

    public void setDataBasedMemberFilter(DataBasedMemberFilter input) throws OLAPException {
        this.dataBasedMemberFilter = input;
    }

    public DataBasedMemberFilter getDataBasedMemberFilter() throws OLAPException {
        return dataBasedMemberFilter;
    }

    public Ordinate getOrdinate() throws OLAPException {
        return ordinate;
    }

    public void setOrdinateOperator(OrdinateOperator input) throws OLAPException {
        this.ordinateOperator = input;
    }

    public OrdinateOperator getOrdinateOperator() throws OLAPException {
        return ordinateOperator;
    }
}

// End MondrianDataBasedMemberFilterInput.java
