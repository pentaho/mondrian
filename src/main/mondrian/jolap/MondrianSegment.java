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

import javax.olap.query.querycoremodel.Segment;
import javax.olap.query.querycoremodel.EdgeView;
import javax.olap.query.querycoremodel.DimensionStepManager;
import javax.olap.OLAPException;
import java.util.Collection;

/**
 * A <code>MondrianSegment</code> is ...
 *
 * <p>This class is <em>not</em> related to {@link mondrian.rolap.agg.Segment}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianSegment extends RefObjectSupport implements Segment {
	RelationshipList dimensionStepManager = new RelationshipList(Meta.dimensionStepManager);

	static abstract class Meta {
		static Relationship dimensionStepManager = new Relationship(MondrianSegment.class, "dimensionStepManager", DimensionStepManager.class);
	}

	public MondrianSegment() {
	}

	public EdgeView getEdgeView() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setDimensionStepManager(Collection input) throws OLAPException {
		dimensionStepManager.set(input);
	}

	public Collection getDimensionStepManager() throws OLAPException {
		return dimensionStepManager.get();
	}

	public void addDimensionStepManager(DimensionStepManager input) throws OLAPException {
		dimensionStepManager.add(input);
	}

	public void removeDimensionStepManager(DimensionStepManager input) throws OLAPException {
		dimensionStepManager.remove(input);
	}

	public String getName() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setName(String input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public String getId() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setId(String input) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianSegment.java