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

import javax.olap.query.dimensionfilters.DataBasedMemberFilterInput;
import javax.olap.query.dimensionfilters.DataBasedMemberFilter;
import javax.olap.query.querycoremodel.QualifiedMemberReference;
import javax.olap.query.querycoremodel.Ordinate;
import javax.olap.query.sorting.DataBasedSort;
import javax.olap.query.calculatedmembers.OrdinateOperator;
import javax.olap.OLAPException;
import javax.olap.metadata.Member;
import java.util.Collection;

/**
 * A <code>MondrianQualifiedMemberReference</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianQualifiedMemberReference extends MondrianDataBasedMemberFilterInput
		implements QualifiedMemberReference {
	private RelationshipList member = new RelationshipList(Meta.member);

	static class Meta {
		static Relationship member = new Relationship(MondrianQualifiedMemberReference.class, "member", Member.class);
	}
	public MondrianQualifiedMemberReference() {
	}

	public void setMember(Collection input) throws OLAPException {
		member.set(input);
	}

	public Collection getMember() throws OLAPException {
		return member.get();
	}

	public void addMember(Member input) throws OLAPException {
		member.add(input);
	}

	public void removeMember(Member input) throws OLAPException {
		member.remove(input);
	}

	public DataBasedSort getDataBasedSort() throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End MondrianQualifiedMemberReference.java