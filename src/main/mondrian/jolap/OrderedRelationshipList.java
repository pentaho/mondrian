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

/**
 * A <code>OrderedRelationshipList</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class OrderedRelationshipList extends RelationshipList {

	public OrderedRelationshipList(Relationship relationship) {
		super(relationship);
	}

	public Object addBefore(Object before, Object o) {
		Util.assertTrue(relationship.toClass.isInstance(o));
		int i = indexOf(before);
		add(i, o);
		return o;
	}

	public Object addAfter(Object after, Object o) {
		Util.assertTrue(relationship.toClass.isInstance(o));
		int i = indexOf(after);
		add(i + 1, o);
		return o;
	}

	public void moveBefore(Object before, Object o) {
		int i = indexOf(before);
		Util.assertTrue(remove(o));
		add(i, o);
	}

	public void moveAfter(Object after, Object o) {
		int i = indexOf(after);
		Util.assertTrue(remove(o));
		add(i + 1, o);
	}
}

// End OrderedRelationshipList.java