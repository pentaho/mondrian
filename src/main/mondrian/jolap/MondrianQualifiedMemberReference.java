/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;
import javax.olap.metadata.Member;
import javax.olap.query.querycoremodel.Ordinate;
import javax.olap.query.querycoremodel.QualifiedMemberReference;
import java.util.Collection;

/**
 * Implementation of {@link QualifiedMemberReference}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
class MondrianQualifiedMemberReference extends MondrianDataBasedMemberFilterInput
        implements QualifiedMemberReference {
    private RelationshipList member = new RelationshipList(Meta.member);

    static class Meta {
        static Relationship member = new Relationship(MondrianQualifiedMemberReference.class, "member", Member.class);
    }

    public MondrianQualifiedMemberReference() {
    }

    public Collection getMember() throws OLAPException {
        return member;
    }

    public void setOrdinate(Ordinate value) throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianQualifiedMemberReference.java
