/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
