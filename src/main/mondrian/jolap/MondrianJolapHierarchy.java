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

import javax.olap.metadata.LevelBasedHierarchy;
import javax.olap.metadata.HierarchyLevelAssociation;
import javax.olap.metadata.Dimension;
import javax.olap.metadata.CubeDimensionAssociation;
import javax.olap.OLAPException;
import java.util.Collection;
import java.util.List;

/**
 * A <code>MondrianJolapHierarchy</code> is ...
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

	public void setHierarchyLevelAssociation(Collection input) throws OLAPException {
		hierarchyLevelAssociation.set(input);
	}

	public List getHierarchyLevelAssociation() throws OLAPException {
		return hierarchyLevelAssociation.get();
	}

	public void removeHierarchyLevelAssociation(HierarchyLevelAssociation input) throws OLAPException {
		hierarchyLevelAssociation.remove(input);
	}

	public void moveHierarchyLevelAssociationBefore(HierarchyLevelAssociation before, HierarchyLevelAssociation input) throws OLAPException {
		hierarchyLevelAssociation.moveBefore(before, input);
	}

	public void moveHierarchyLevelAssociationAfter(HierarchyLevelAssociation before, HierarchyLevelAssociation input) throws OLAPException {
		hierarchyLevelAssociation.moveAfter(before, input);
	}

	public Dimension getDimension() throws OLAPException {
		return dimension;
	}

	public void setCubeDimensionAssociation(Collection input) throws OLAPException {
		cubeDimensionAssociation.set(input);
	}

	public Collection getCubeDimensionAssociation() throws OLAPException {
		return cubeDimensionAssociation.get();
	}

	public void addCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException {
		cubeDimensionAssociation.add(input);
	}

	public void removeCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException {
		cubeDimensionAssociation.remove(input);
	}

	public void setDefaultedDimension(Dimension input) throws OLAPException {
		this.defaultedDimension = input;
	}

	public Dimension getDefaultedDimension() throws OLAPException {
		return defaultedDimension;
	}
}

// End MondrianJolapHierarchy.java