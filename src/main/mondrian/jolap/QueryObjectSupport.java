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

import javax.olap.OLAPException;
import javax.olap.query.enumerations.SelectedObjectType;
import javax.olap.query.enumerations.SelectedObjectTypeEnum;
import javax.olap.query.querycoremodel.QueryObject;
import javax.olap.query.querycoremodel.SelectedObject;
import javax.olap.query.querytransaction.QueryTransaction;

/**
 * Abstract implementation of {@link QueryObject}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class QueryObjectSupport extends RefObjectSupport implements QueryObject {
    private boolean supportsTransactions;

    QueryObjectSupport(boolean supportsTransactions) {
        this.supportsTransactions = supportsTransactions;
    }

    /**
     * Factory method for creating {@link SelectedObject} objects:<ul>
     * <li>{@link SelectedObjectTypeEnum#ATTRIBUTE_REFERENCE} yields
     *     {@link MondrianAttributeReference}</li>
     * </ul>
     *
     * @param dimensionView Dimension view that the object belongs to
     * @param type Type of object
     * @return Object of the requested type
     * @throws UnsupportedOperationException if type is not known
     */
    static SelectedObject createSelectedObject(
            MondrianDimensionView dimensionView, SelectedObjectType type) {
        if (type == SelectedObjectTypeEnum.ATTRIBUTE_REFERENCE) {
            return new MondrianAttributeReference(dimensionView);
        } else {
            throw new UnsupportedOperationException("Unknown type " + type);
        }
    }


    public void setActiveIn(QueryTransaction input) throws OLAPException {
        if (!supportsTransactions) {
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException(); // todo: implement
    }

    public QueryTransaction getActiveIn() throws OLAPException {
        if (!supportsTransactions) {
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException(); // todo: implement
    }

    public String getName() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setName(String input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public String getId() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setId(String input) throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End QueryObjectSupport.java
