/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 26, 2003
*/
package mondrian.rolap;



/**
 * A {@link MemberReader} which recasts members from an underlying member
 * reader into a given hierarchy.
 *
 * <p>It is used when several cube hierarchies have the same shared hierarchy.
 * All of the hierarchies will share the same underlying reader, but the
 * members which come out of this reader belong to the desired hierarchy.
 *
 * @author jhyde
 * @since Jan 13, 2005
 * @version $Id$
 */
class BrandingMemberReader extends DelegatingMemberReader {
    BrandingMemberReader(MemberReader memberReader) {
        super(memberReader);
    }
}

// End BrandingMemberReader.java
