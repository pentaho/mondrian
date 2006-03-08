/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.sql;

/**
 * restricts the members that are fetched by SqlMemberSource.
 * <p>
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
