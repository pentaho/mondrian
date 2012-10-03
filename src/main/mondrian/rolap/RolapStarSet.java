/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.Collections;
import java.util.List;

/**
 * Collection of stars that form the context for an action on a dimension.
 *
 * <p>For example, the query
 *
 * <blockquote><pre>
 * SELECT {[Measures].[Store Sales], [Measures].[Inventory]} ON 0,
 *   NON EMPTY {[Product].[Dairy].Children} ON 1
 * FROM [Warehouse and Sales]}</pre></blockquote>
 *
 * we are finding children that are non-empty in both sales_fact_1997 (the fact
 * table which underlies the [Store Sales] measure) and inventory_fact_1997
 * (for the [Inventory] measure).
 *
 * @author jhyde
 * @since 9 October, 2008
 */
public class RolapStarSet {
    final RolapCube cube;
    private final RolapStar star;
    private final RolapMeasureGroup measureGroup;
    private final RolapMeasureGroup aggMeasureGroup;

    public RolapStarSet(
        RolapStar star,
        RolapMeasureGroup measureGroup,
        RolapMeasureGroup aggMeasureGroup)
    {
        // star may be null - see NonEmptyTest.testMondrianBug138
        this.star = star;
        this.cube = null; // cube seems to be ALWAYS null
        this.measureGroup = measureGroup;
        this.aggMeasureGroup = aggMeasureGroup;
        if (aggMeasureGroup != null) {
            Util.discard(0);
        }
    }

    /**
     * Returns a list of stars to join against.
     *
     * @return List of stars; may be empty, never null
     */
    List<RolapStar> getStars() {
        if (star != null) {
            return Collections.singletonList(star);
        } else if (cube == null) {
            return Collections.emptyList();
        } else {
            return cube.getStars();
        }
    }

    /**
     * Returns this star set's unique star, or null if no star.
     *
     * @return Unique star, or null
     *
     * @throws RuntimeException if more than one star
     */
    public RolapStar getStar() {
        if (star != null) {
            return star;
        }
        if (cube == null) {
            return null;
        }
        final List<RolapStar> stars = cube.getStars();
        switch (stars.size()) {
        case 0:
            return null;
        case 1:
            return stars.get(0);
        default:
            throw Util.newInternal("Expected 0 or 1 stars, got " + stars);
        }
    }

    /**
     * Returns this star set's unique star, never null.
     *
     * @return Unique star, never null
     *
     * @throws RuntimeException if no star or more than one star
     */
    public RolapStar getSoleStar() {
        if (star != null) {
            return star;
        }
        if (cube == null) {
            throw Util.newInternal("Expected 1 star, got 0");
        }
        final List<RolapStar> starList = cube.getStars();
        if (starList.size() != 1) {
            throw Util.newInternal("Expected 1 star, got " + starList.size());
        }
        return starList.get(0);
    }

    public RolapMeasureGroup getMeasureGroup() {
        return measureGroup;
    }

    public AggStar getAggStar() {
        // FIXME; will throw if not null; eliminate method
        return (AggStar) (Object) aggMeasureGroup;
    }

    public RolapMeasureGroup getAggMeasureGroup() {
        return aggMeasureGroup;
    }
}

// End RolapStarSet.java
