/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import org.omg.java.cwm.objectmodel.core.Attribute;

import javax.olap.OLAPException;
import javax.olap.metadata.*;

/**
 * Implementation of {@link MemberObjectFactories}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianMemberObjectFactories implements MemberObjectFactories  {
	public Member createMember(Dimension owner) {
		return new MondrianJolapMember(owner);
	}

	public CurrentMember createCurrentMember(Dimension owner) {
		throw new UnsupportedOperationException();
	}

	public MemberList createMemberList(Dimension owner) {
		throw new UnsupportedOperationException();
	}

    public MemberValue createMemberValue(Member owner, Attribute attribute) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianMemberObjectFactories.java
