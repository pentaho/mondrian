/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;
import javax.olap.metadata.CubeDimensionAssociation;
import javax.olap.metadata.Dimension;
import javax.olap.metadata.HierarchyLevelAssociation;
import javax.olap.metadata.LevelBasedHierarchy;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link LevelBasedHierarchy JOLAP Hierarchy} based upon a
 * {@link mondrian.olap.Hierarchy Mondrian Hierarchy}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianJolapHierarchy extends ClassifierSupport implements LevelBasedHierarchy {
    private mondrian.olap.Hierarchy hierarchy;
    private OrderedRelationshipList hierarchyLevelAssociation = new OrderedRelationshipList(Meta.hierarchyLevelAssociation);
    private Dimension dimension;
    private RelationshipList cubeDimensionAssociation = new RelationshipList(Meta.cubeDimensionAssociation);
    private Dimension defaultedDimension;

    static class Meta {
        static Relationship hierarchyLevelAssociation = new Relationship(MondrianJolapHierarchy.class, "hierarchyLevelAssociation", HierarchyLevelAssociation.class);
        static Relationship cubeDimensionAssociation = new Relationship(MondrianJolapHierarchy.class, "cubeDimensionAssociation", CubeDimensionAssociation.class);
    }

    public MondrianJolapHierarchy(mondrian.olap.Hierarchy hierarchy) {
        super();
        this.hierarchy = hierarchy;
    }

    public List getHierarchyLevelAssociation() throws OLAPException {
        return hierarchyLevelAssociation;
    }

    public Dimension getDimension() throws OLAPException {
        return dimension;
    }

    public void setDimension(Dimension value) throws OLAPException {
        throw new UnsupportedOperationException("cannot transfer");
    }

    public Collection getCubeDimensionAssociation() throws OLAPException {
        return cubeDimensionAssociation;
    }

    public void setDefaultedDimension(Dimension input) throws OLAPException {
        this.defaultedDimension = input;
    }

    public Dimension getDefaultedDimension() throws OLAPException {
        return defaultedDimension;
    }
}

// End MondrianJolapHierarchy.java
