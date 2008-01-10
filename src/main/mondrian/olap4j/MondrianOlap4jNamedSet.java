/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.metadata.NamedSet;
import org.olap4j.metadata.Cube;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.impl.Named;

import java.util.Locale;

/**
 * Implementation of {@link org.olap4j.metadata.NamedSet}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 12, 2007
 */
public class MondrianOlap4jNamedSet implements NamedSet, Named {
    private final MondrianOlap4jCube olap4jCube;
    private mondrian.olap.NamedSet namedSet;

    MondrianOlap4jNamedSet(
        MondrianOlap4jCube olap4jCube,
        mondrian.olap.NamedSet namedSet)
    {
        this.olap4jCube = olap4jCube;
        this.namedSet = namedSet;
    }

    public Cube getCube() {
        return olap4jCube;
    }

    public ParseTreeNode getExpression() {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
                .olap4jConnection;
        return olap4jConnection.toOlap4j(namedSet.getExp());
    }

    public String getName() {
        return namedSet.getName();
    }

    public String getUniqueName() {
        return namedSet.getUniqueName();
    }

    public String getCaption(Locale locale) {
        // todo: i18n
        return namedSet.getCaption();
    }

    public String getDescription(Locale locale) {
        // todo: i18n
        return namedSet.getDescription();
    }
}

// End MondrianOlap4jNamedSet.java
