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

import mondrian.olap.Util;

import javax.olap.cursor.CubeCursor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A <code>RelationshipList</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class RelationshipList extends ArrayList {
	protected Relationship relationship;

	RelationshipList(Relationship relationship) {
		this.relationship = relationship;
	}

	public void set(Collection collection) {
		clear();
		addAll(collection);
		for (Iterator iterator = collection.iterator(); iterator.hasNext();) {
			final Object o = iterator.next();
			Util.assertTrue(relationship.toClass.isInstance(o));
		}
	}

	public List get() {
		return this;
	}

	public Object addNew(Object o) {
		add(o);
		return o;
	}
}

// End RelationshipList.java