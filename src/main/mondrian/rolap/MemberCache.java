/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;

/**
 * A <code>MemberCache</code> can retrieve members based upon their parent and
 * name.
 *
 * @author jhyde
 * @since 22 December, 2001
 * @version $Id$
 **/
interface MemberCache {
	/** Creates a key with which to {@link #get} or {@link #put} the {@link
	 * RolapMember} with a given parent and key. **/
	Object makeKey(RolapMember parent, Object key);
	/** Retrieves the {@link RolapMember} with a given key (created by {@link
	 * #makeKey}). **/
	RolapMember getMember(Object key);
	/** Replaces the {@link RolapMember} with a given key (created by {@link
	 * #makeKey}). Returns the previous member with that key, or null. **/
	Object putMember(Object key, RolapMember value);
}


// End MemberCache.java
