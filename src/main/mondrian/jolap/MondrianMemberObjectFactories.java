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

import javax.olap.metadata.*;
import javax.olap.query.CurrentMember;

/**
 * A <code>MondrianMemberObjectFactories</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
public class MondrianMemberObjectFactories implements MemberObjectFactories  {
	public Member createMember(Dimension owner) {
		return new MondrianJolapMember(owner);
	}

	public CurrentMember createCurrentMember(Dimension owner) {
		throw new UnsupportedOperationException();
	}

	public MemberList createMemberList(Dimension owner) {
		throw new UnsupportedOperationException();
	}

	public MemberValue createMemberValue(Member owner) {
		throw new UnsupportedOperationException();
	}

	public MemberSet createMemberSet(Dimension owner) {
		throw new UnsupportedOperationException();
	}
}

// End MondrianMemberObjectFactories.java