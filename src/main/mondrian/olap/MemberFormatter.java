/*
//$Id$
//This software is subject to the terms of the Common Public License
//Agreement, available at the following URL:
//http://www.opensource.org/licenses/cpl.html.
//Copyright (C) 2004-2004 TONBELLER AG
//All Rights Reserved.
//You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * this interface provides a user exit to redefine 
 *  the member caption beeing displayed.
 */
public interface MemberFormatter {
	String formatMember(Member m);
} // MemberFormatter
