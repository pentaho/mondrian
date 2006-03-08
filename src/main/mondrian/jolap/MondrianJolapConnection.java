/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.jolap;

import mondrian.olap.Hierarchy;

import javax.jmi.reflect.RefPackage;
import javax.olap.OLAPException;
import javax.olap.metadata.Dimension;
import javax.olap.metadata.MemberObjectFactories;
import javax.olap.metadata.Schema;
import javax.olap.query.querycoremodel.Constant;
import javax.olap.query.querycoremodel.CubeView;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.EdgeView;
import javax.olap.query.querytransaction.QueryTransactionManager;
import javax.olap.resource.Connection;
import javax.olap.resource.ConnectionMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * JOLAP connection to a Mondrian database.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 */
class MondrianJolapConnection extends RefObjectSupport implements Connection {
    mondrian.olap.Connection mondrianConnection;
    private final MondrianMemberObjectFactories memberObjectFactories = new MondrianMemberObjectFactories();

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

    public Collection getDimensions() throws OLAPException {
        final mondrian.olap.Schema schema = mondrianConnection.getSchema();
        final Hierarchy[] sharedHierarchies = schema.getSharedHierarchies();
        final ArrayList list = new ArrayList();
        for (int i = 0; i < sharedHierarchies.length; i++) {
            list.add(new MondrianJolapDimension(
                    getCurrentSchema(), sharedHierarchies[i].getDimension()));
        }
        return list;
    }

    public Collection getCubes() throws OLAPException {
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
        return new MondrianCubeView(this, null);
    }

    public DimensionView createDimensionView(Dimension dimension) throws OLAPException {
        return new MondrianDimensionView(dimension);
    }

    public EdgeView createEdgeView() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public Collection getSchemas() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public Constant createConstant() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public MemberObjectFactories getMemberObjectFactories() throws OLAPException {
        return memberObjectFactories;
    }

    public QueryTransactionManager getQueryTransactionManager() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void abort() throws OLAPException {
    }
}

// End MondrianJolapConnection.java
