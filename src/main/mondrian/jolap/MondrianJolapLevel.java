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

import javax.olap.metadata.Level;
import javax.olap.metadata.HierarchyLevelAssociation;
import javax.olap.metadata.Dimension;
import javax.olap.OLAPException;
import java.util.Collection;

/**
 * A <code>MondrianJolapLevel</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianJolapLevel extends ClassifierSupport implements Level {
	private mondrian.olap.Level level;
	private Dimension dimension;

	public MondrianJolapLevel(mondrian.olap.Level level, Dimension dimension) {
		this.level = level;
	}

	public void setHierarchyLevelAssociation(Collection input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Collection getHierarchyLevelAssociation() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void addHierarchyLevelAssociation(HierarchyLevelAssociation input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void removeHierarchyLevelAssociation(HierarchyLevelAssociation input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Dimension getDimension() throws OLAPException {
		return dimension;
	}
}

// End MondrianJolapLevel.java