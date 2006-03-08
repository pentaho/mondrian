/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import mondrian.olap.*;

import javax.olap.OLAPException;
import javax.olap.metadata.CubeDimensionAssociation;
import javax.olap.metadata.MemberSelection;
import javax.olap.metadata.Schema;
import java.util.Collection;

/**
 * Implementation of {@link Dimension JOLAP Dimension} based upon a
 * {@link mondrian.olap.Dimension Mondrian Dimension}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
class MondrianJolapDimension extends ClassifierSupport implements javax.olap.metadata.Dimension {
    Dimension dimension;
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

    public void setTime(boolean input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public boolean isTime() {
        return dimension.getDimensionType() == DimensionType.TimeDimension;
    }

    public void setMeasure(boolean input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public boolean isMeasure() {
        return dimension.isMeasures();
    }

    public String getName() {
        return dimension.getName();
    }

    public Collection getHierarchy() {
        return hierarchy;
    }

    public Collection getMemberSelection() {
        return memberSelection;
    }

    public Collection getCubeDimensionAssociation() throws OLAPException {
        return cubeDimensionAssociation;
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

    public void setSchema(Schema value) throws OLAPException {
        schema = value;
    }
}

// End MondrianJolapDimension.java
