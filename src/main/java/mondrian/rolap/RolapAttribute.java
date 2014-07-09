/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.*;
import mondrian.spi.MemberFormatter;

import org.apache.log4j.Logger;

import org.olap4j.metadata.Level;

import java.util.List;

/**
 * Attribute.
 *
 * <p>Attributes belong to {@link RolapHierarchy hierarchies} and are composed
 * to make {@link RolapLevel levels}.
 *
 * @author jhyde
 */
public interface RolapAttribute extends OlapElement {
    final Logger LOGGER = Logger.getLogger(RolapAttribute.class);

    RolapDimension getDimension();

    List<RolapProperty> getExplicitProperties();

    List<RolapProperty> getProperties();

    // TODO: obsolete
    Property.Datatype getType();

    // TODO: obsolete this method; use datatype of columns in getKeyList()
    Dialect.Datatype getDatatype();

    int getApproxRowCount();

    // following methods not for public use

    /**
     * The column (or columns) that yields the attribute's key. The columns may
     * be calculated.
     */
    List<RolapSchema.PhysColumn> getKeyList();

    /**
     * Ths column that gives the name of members of this level. If null,
     * members are named using the key expression.
     */
    RolapSchema.PhysColumn getNameExp();

    /**
     * The column or expression which yields the caption of the attribute.
     */
    RolapSchema.PhysColumn getCaptionExp();

    /**
     * The list of columns that are used to sort the attribute.
     *
     * <p>These columns do not necessarily include the key of the parent level,
     * when the attribute is used in a non-trivial hierarchy. When used to
     * generate an ORDER BY clause, Mondrian will sort by the parent's key
     * first.
     */
    List<RolapSchema.PhysColumn> getOrderByList();

    Level.Type getLevelType();

    MemberFormatter getMemberFormatter();

    Larder getLarder();
}

// End RolapAttribute.java
