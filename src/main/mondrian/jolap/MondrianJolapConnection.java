/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.jolap;

import mondrian.olap.Hierarchy;

import javax.olap.resource.Connection;
import javax.olap.resource.ConnectionMetaData;
import javax.olap.OLAPException;
import javax.olap.query.querycoremodel.CubeView;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.EdgeView;
import javax.olap.query.querycoremodel.MeasureView;
import javax.olap.metadata.Schema;
import javax.olap.metadata.Cube;
import javax.jmi.reflect.RefPackage;
import java.util.List;
import java.util.ArrayList;

/**
 * A <code>MondrianJolapConnection</code> is a JOLAP connection to a
 * Mondrian database.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 **/
class MondrianJolapConnection extends RefObjectSupport implements Connection {
	mondrian.olap.Connection mondrianConnection;

	MondrianJolapConnection(mondrian.olap.Connection connection) {
		this.mondrianConnection = connection;
	}
	public void close() throws OLAPException {
		mondrianConnection.close();
	}

	public ConnectionMetaData getMetaData() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public RefPackage getTopLevelPackage() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public List getSchema() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Schema getCurrentSchema() throws OLAPException {
		return null; // todo:
	}

	public void setCurrentSchema(Schema schema) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public List getDimensions() throws OLAPException {
		final mondrian.olap.Schema schema = mondrianConnection.getSchema();
		final Hierarchy[] sharedHierarchies = schema.getSharedHierarchies();
		final ArrayList list = new ArrayList();
		for (int i = 0; i < sharedHierarchies.length; i++) {
			list.add(new MondrianJolapDimension(
					getCurrentSchema(), sharedHierarchies[i].getDimension()));
		}
		return list;
	}

	public List getCubes() throws OLAPException {
		final mondrian.olap.Schema schema = mondrianConnection.getSchema();
		final mondrian.olap.Cube[] cubes = schema.getCubes();
		final ArrayList list = new ArrayList();
		for (int i = 0; i < cubes.length; i++) {
			mondrian.olap.Cube cube = cubes[i];
			list.add(new MondrianJolapCube(getCurrentSchema(), cube));
		}
		return list;
	}

	public CubeView createCubeView() throws OLAPException {
		return createCubeView(null);
	}

	public CubeView createCubeView(Cube cube) throws OLAPException {
		return new MondrianCubeView(this, cube);
	}

	public DimensionView createDimensionView() throws OLAPException {
		return new MondrianDimensionView();
	}

	public EdgeView createEdgeView() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public MeasureView createMeasureView() throws OLAPException {
		return new MondrianDimensionView();
	}
}

// End MondrianJolapConnection.java