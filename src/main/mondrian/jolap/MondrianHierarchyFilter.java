/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import mondrian.olap.Exp;

import javax.olap.OLAPException;
import javax.olap.metadata.Hierarchy;
import javax.olap.query.dimensionfilters.HierarchyFilter;
import javax.olap.query.enumerations.HierarchyFilterType;

/**
 * Implementation of {@link HierarchyFilter}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianHierarchyFilter extends MondrianDimensionFilter
		implements HierarchyFilter {
	private HierarchyFilterType hierarchyFilterType;
	private Hierarchy hierarchy;

	public MondrianHierarchyFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	Exp convert(Exp exp) throws OLAPException {
		Exp newExp = _convert();
		return combine(exp, newExp);
	}

	private Exp _convert() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	// object model methods

	public HierarchyFilterType getHierarchyFilterType() throws OLAPException {
		return hierarchyFilterType;
	}

	public void setHierarchyFilterType(HierarchyFilterType input) throws OLAPException {
		this.hierarchyFilterType = input;
	}

	public void setHierarchy(Hierarchy input) throws OLAPException {
		this.hierarchy = input;
	}

	public Hierarchy getHierarchy() throws OLAPException {
		return hierarchy;
	}
}

// End MondrianHierarchyFilter.java
