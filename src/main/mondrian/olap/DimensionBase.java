/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.resource.MondrianResource;

import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.NamedList;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract implementation for a {@link Dimension}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public abstract class DimensionBase
    extends OlapElementBase
    implements Dimension
{
    protected final String name;
    protected final String uniqueName;
    protected final String description;
    protected final NamedList<Hierarchy> hierarchyList =
        new NamedListImpl<Hierarchy>();
    protected final DimensionType dimensionType;

    /**
     * Creates a DimensionBase.
     *
     * @param name Name
     * @param dimensionType Type
     */
    protected DimensionBase(
        String name,
        boolean visible,
        String caption,
        String description,
        DimensionType dimensionType)
    {
        this.name = name;
        this.caption = caption;
        this.visible = visible;
        this.uniqueName = Util.makeFqName(name);
        this.description = description;
        this.dimensionType = dimensionType;
        assert dimensionType != null;
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

    /**
     * {@inheritDoc}
     *
     * <p>In this case, the expression is a dimension, so the hierarchy is the
     * dimension's default hierarchy (its first).
     */
    public Hierarchy getHierarchy() {
        return hierarchyList.get(0);
    }

    public Dimension getDimension() {
        return this;
    }

    public DimensionType getDimensionType() {
        return dimensionType;
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxDimensionName.str(
            getUniqueName());
    }

    public boolean isMeasures() {
        return getUniqueName().equals(MEASURES_UNIQUE_NAME);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        OlapElement oe = lookupHierarchy(s);

        // Original mondrian behavior:
        // If the user is looking for [Marital Status].[Marital Status] we
        // should not return oe "Marital Status", because he is
        // looking for level - we can check that by checking of hierarchy and
        // dimension name is the same.
        //
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            if (oe == null || oe.getName().equalsIgnoreCase(getName())) {
                OlapElement oeLevel =
                    getHierarchy().lookupChild(schemaReader, s, matchType);
                if (oeLevel != null) {
                    return oeLevel; // level match overrides hierarchy match
                }
            }
            return oe;
        } else {
            // New (SSAS-compatible) behavior. If there is no matching
            // hierarchy, find the first level with the given name.
            if (oe != null) {
                return oe;
            }
            final List<Hierarchy> hierarchyList =
                schemaReader.getDimensionHierarchies(this);
            for (Hierarchy hierarchy : hierarchyList) {
                oe = hierarchy.lookupChild(schemaReader, s, matchType);
                if (oe != null) {
                    return oe;
                }
            }
            return null;
        }
    }

    public boolean isHighCardinality() {
        return false;
    }

    private Hierarchy lookupHierarchy(Id.Segment s) {
        for (Hierarchy hierarchy : hierarchyList) {
            if (Util.equalName(hierarchy.getName(), s.name)) {
                return hierarchy;
            }
        }
        return null;
    }
}

// End DimensionBase.java
