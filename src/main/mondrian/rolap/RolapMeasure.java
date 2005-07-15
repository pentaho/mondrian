/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.Property;

/**
 * A <code>RolapMeasure</code> is a member of the "Measures" dimension.
 *
 * <p>The only derived class currently is {@link RolapStoredMeasure}.
 * ({@link RolapCalculatedMember calculated members} are not always measures,
 * so they do not derive from this class.)
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 **/
abstract class RolapMeasure extends RolapMember {
    /**
     * Holds the {@link mondrian.rolap.RolapStar.Measure} from which this
     * member is computed. Untyped, because another implementation might store
     * it somewhere else.
     */
    private Object starMeasure;

    RolapMeasure(Member parentMember,
                 RolapLevel level,
                 String name,
                String formatString) {
        super(parentMember, level, name);
        if (formatString == null) {
            formatString = "";
        }
        setProperty(
                Property.FORMAT_EXP.name,
                Literal.createString(formatString));
    }
    Object getStarMeasure() {
        return starMeasure;
    }
    void setStarMeasure(Object starMeasure) {
        this.starMeasure = starMeasure;
    }
}


// End RolapMeasure.java
