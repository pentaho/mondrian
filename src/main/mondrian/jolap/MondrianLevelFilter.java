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

import mondrian.olap.Exp;

import javax.olap.OLAPException;
import javax.olap.metadata.Level;
import javax.olap.query.dimensionfilters.LevelFilter;

/**
 * Implementation of {@link LevelFilter}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianLevelFilter extends MondrianDimensionFilter
        implements LevelFilter {
    private Level level;

    public MondrianLevelFilter(MondrianDimensionStepManager manager) {
        super(manager);
    }

    Exp convert(Exp exp) throws OLAPException {
        Exp newExp = _convert();
        return combine(exp, newExp);
    }

    private Exp _convert() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    // object model methods

    public void setLevel(Level input) throws OLAPException {
        this.level = input;
    }

    public Level getLevel() throws OLAPException {
        return level;
    }
}

// End MondrianLevelFilter.java
