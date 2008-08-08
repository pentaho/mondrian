/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.resource.MondrianResource;

/**
 * Abstract implementation for a {@link Dimension}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class DimensionBase
    extends OlapElementBase
    implements Dimension {

    protected final String name;
    protected final String uniqueName;
    protected final String description;
    protected final boolean highCardinality;
    protected Hierarchy[] hierarchies;
    protected DimensionType dimensionType;

    protected DimensionBase(
            String name,
            DimensionType dimensionType,
            final boolean highCardinality)
    {
        this.name = name;
        this.uniqueName = Util.makeFqName(name);
        this.description = null;
        this.dimensionType = dimensionType;
        this.highCardinality = highCardinality;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Hierarchy[] getHierarchies() {
        return hierarchies;
    }

    public Hierarchy getHierarchy() {
        return hierarchies[0];
    }

    public Dimension getDimension() {
        return this;
    }

    public DimensionType getDimensionType() {
        return dimensionType;
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxDimensionName.str(getUniqueName());
    }

    public boolean isMeasures() {
        return getUniqueName().equals(MEASURES_UNIQUE_NAME);
    }

    public boolean usesDimension(Dimension dimension) {
        return dimension == this;
    }

    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment s)
    {
        return lookupChild(schemaReader, s, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        OlapElement oe = lookupHierarchy(s);
        // If the user is looking for [Marital Status].[Marital Status] we
        // should not return oe "Marital Status", because he is
        // looking for level - we can check that by checking of hierarchy and
        // dimension name is the same.
        if ((oe == null) || oe.getName().equalsIgnoreCase(getName())) {
            OlapElement oeLevel =
                getHierarchy().lookupChild(schemaReader, s, matchType);
            if (oeLevel != null) {
                oe = oeLevel; // level match overrides hierarchy match
            }
        }

        if (getLogger().isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(64);
            buf.append("DimensionBase.lookupChild: ");
            buf.append("name=");
            buf.append(getName());
            buf.append(", childname=");
            buf.append(s);
            if (oe == null) {
                buf.append(" returning null");
            } else {
                buf.append(" returning elementname=" + oe.getName());
            }
            getLogger().debug(buf.toString());
        }

        return oe;
    }

    public boolean isHighCardinality() {
        return this.highCardinality;
    }

    private Hierarchy lookupHierarchy(Id.Segment s) {
        for (Hierarchy hierarchy : hierarchies) {
            if (hierarchy.getName().equalsIgnoreCase(s.name)) {
                return hierarchy;
            }
        }
        return null;
    }
}


// End DimensionBase.java
