/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.sql;

/**
 * Restricts the members that are fetched by SqlMemberSource.
 *
 * @see mondrian.rolap.SqlMemberSource
 *
 * @author av
 * @since Nov 2, 2005
 */
public interface SqlConstraint {

   /**
    * Returns a key that becomes part of the key for caching the
    * result of the SQL query. So SqlConstraint instances that
    * produce the same SQL resultset must return equal keys
    * in terms of equal() and hashCode().
    * @return valid key or null to prevent the result from being cached
    */
    Object getCacheKey();
}

// End SqlConstraint.java
