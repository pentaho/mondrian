/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 25, 2002
*/
package mondrian.jolap;

import mondrian.olap.Dimension;

import javax.olap.metadata.Cube;
import javax.olap.metadata.CubeDimensionAssociation;
import javax.olap.metadata.Schema;
import javax.olap.metadata.Hierarchy;
import javax.olap.OLAPException;
import java.util.Collection;

/**
 * A <code>MondrianJolapCube</code> is ...
 *
 * @author jhyde
 * @since Dec 25, 2002
 * @version $Id$
 **/
public class MondrianJolapCube extends ClassifierSupport implements Cube {
	private Schema schema;
	mondrian.olap.Cube cube;
	private RelationshipList cubeDimensionAssociation = new RelationshipList(Meta.cubeDimensionAssociation);

	static class Meta {
		static Relationship cubeDimensionAssociation = new Relationship(MondrianJolapCube.class, "cubeDimensionAssociation", CubeDimensionAssociation.class);
	}

	MondrianJolapCube(Schema schema, mondrian.olap.Cube cube) {
		this.schema = schema;
		this.cube = cube;
		final Dimension[] dimensions = cube.getDimensions();
		for (int i = 0; i < dimensions.length; i++) {
			Dimension dimension = dimensions[i];
			final MondrianJolapDimension jolapDimension =
					new MondrianJolapDimension(schema, dimension);
			cubeDimensionAssociation.add(
					new MondrianCubeDimensionAssociation(this,jolapDimension));
		}
	}

	public String getName() {
		return cube.getName();
	}

	// object model methods

	public Boolean getIsVirtual() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setIsVirtual(Boolean input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setCubeDimensionAssociation(Collection input) throws OLAPException {
		cubeDimensionAssociation.set(input);
	}

	public Collection getCubeDimensionAssociation() throws OLAPException {
		return cubeDimensionAssociation.get();
	}

	public void removeCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException {
		cubeDimensionAssociation.remove(input);
	}

	public Schema getSchema() throws OLAPException {
		return schema;
	}
}

class MondrianCubeDimensionAssociation extends ClassifierSupport
		implements CubeDimensionAssociation {
	private MondrianJolapCube cube;
	private MondrianJolapDimension dimension;

	MondrianCubeDimensionAssociation(MondrianJolapCube cube, MondrianJolapDimension dimension) {
		this.cube = cube;
		this.dimension = dimension;
	}

	public void setDimension(javax.olap.metadata.Dimension input) throws OLAPException {
		this.dimension = (MondrianJolapDimension) input;
	}

	public javax.olap.metadata.Dimension getDimension() throws OLAPException {
		return dimension;
	}

	public Cube getCube() throws OLAPException {
		return cube;
	}

	public void setCalcHierarchy(Hierarchy input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Hierarchy getCalcHierarchy() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}
// End MondrianJolapCube.java