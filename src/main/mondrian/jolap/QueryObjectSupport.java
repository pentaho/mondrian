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

import javax.olap.query.querycoremodel.QueryObject;
import javax.olap.query.querytransaction.QueryTransaction;
import javax.olap.OLAPException;

/**
 * A <code>QueryObjectSupport</code> is ...
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