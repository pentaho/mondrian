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
import javax.olap.query.dimensionfilters.DataBasedMemberFilterInput;
import javax.olap.query.dimensionfilters.DataBasedMemberFilter;
import javax.olap.query.enumerations.DataBasedMemberFilterInputType;
import javax.olap.query.enumerations.DataBasedMemberFilterInputTypeEnum;

/**
 * A <code>MondrianDataBasedMemberFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class MondrianDataBasedMemberFilter extends MondrianDimensionFilter
		implements DataBasedMemberFilter {
	private DataBasedMemberFilterInput input;
	private Boolean basedOnPercent;

	MondrianDataBasedMemberFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	public Boolean getBasedOnPercent() throws OLAPException {
		return basedOnPercent;
	}

	public void setBasedOnPercent(Boolean input) throws OLAPException {
		this.basedOnPercent = input;
	}

	public void setInput(DataBasedMemberFilterInput input) throws OLAPException {
		this.input = input;
	}

	public DataBasedMemberFilterInput getInput() throws OLAPException {
		return input;
	}

	public DataBasedMemberFilterInput createDataBasedMemberFilterInput(DataBasedMemberFilterInputType type) throws OLAPException {
		if (type == DataBasedMemberFilterInputTypeEnum.QUALIFIEDMEMBERREFERENCE) {
			return new MondrianQualifiedMemberReference();
		} else {
			throw new UnsupportedOperationException();
		}
	}
}

// End MondrianDataBasedMemberFilter.java