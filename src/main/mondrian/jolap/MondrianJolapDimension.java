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

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Property;

import javax.olap.OLAPException;
import javax.olap.metadata.CubeDimensionAssociation;
import javax.olap.metadata.MemberSelection;
import javax.olap.metadata.Schema;
import java.util.Collection;

/**
 * A <code>MondrianJolapDimension</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianJolapDimension extends ClassifierSupport implements javax.olap.metadata.Dimension {
	private Dimension dimension;
	private RelationshipList hierarchy = new RelationshipList(Meta.hierarchy);
	private RelationshipList memberSelection = new RelationshipList(Meta.memberSelection);
	private RelationshipList cubeDimensionAssociation = new RelationshipList(Meta.cubeDimensionAssociation);
	private javax.olap.metadata.Hierarchy displayDefault;
	private Schema schema;

	static class Meta {
		static final Relationship hierarchy = new Relationship(MondrianJolapDimension.class, "hierarchy", MondrianJolapHierarchy.class, "dimension");
		static final Relationship memberSelection = new Relationship(MondrianJolapDimension.class, "memberSelection", MemberSelection.class, "dimension");
		static final Relationship cubeDimensionAssociation = new Relationship(MondrianJolapDimension.class, "cubeDimensionAssociation", CubeDimensionAssociation.class);
	}
	MondrianJolapDimension(Schema schema, Dimension dimension) {
		this.dimension = dimension;
		final Hierarchy[] hierarchies = dimension.getHierarchies();
		for (int i = 0; i < hierarchies.length; i++) {
			Hierarchy hierarchy = dimension.getHierarchies()[i];
			this.hierarchy.add(new MondrianJolapHierarchy(hierarchy));
			final Level[] levels = hierarchy.getLevels();
			for (int j = 0; j < levels.length; j++) {
				final Level level = levels[j];
				memberSelection.add(new MondrianJolapLevel(level, this));
				feature.add(new AttributeSupport() {
					public String getName() {
						return level.getName();
					}
				});
				final Property[] properties = level.getProperties();
				for (int k = 0; k < properties.length; k++) {
					final Property property = properties[k];
					feature.add(new AttributeSupport() {
						public String getName() {
							return property.getName();
						}
					});
				}
			}
		}
	}

	public void setIsTime(Boolean input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Boolean getIsTime() {
		return dimension.getDimensionType() == Dimension.TIME ?
				Boolean.TRUE : Boolean.FALSE;
	}

	public void setIsMeasure(Boolean input) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Boolean getIsMeasure() {
		return dimension.isMeasures() ? Boolean.TRUE : Boolean.FALSE;
	}

	public String getName() {
		return dimension.getName();
	}

	public Collection getHierarchy() {
		return hierarchy.get();
	}

	public void setHierarchy(Collection input) throws OLAPException {
		hierarchy.set(input);
	}

	public void removeHierarchy(javax.olap.metadata.Hierarchy input) throws OLAPException {
		hierarchy.remove(input);
	}

	public Collection getMemberSelection() {
		return memberSelection.get();
	}

	public void setMemberSelection(Collection input) throws OLAPException {
		memberSelection.set(input);
	}

	public void removeMemberSelection(MemberSelection input) throws OLAPException {
		memberSelection.remove(input);
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

	public void setDisplayDefault(javax.olap.metadata.Hierarchy input) throws OLAPException {
		this.displayDefault = input;
	}

	public javax.olap.metadata.Hierarchy getDisplayDefault() throws OLAPException {
		return displayDefault;
	}

	public Schema getSchema() throws OLAPException {
		return schema;
	}
}

// End MondrianJolapDimension.java