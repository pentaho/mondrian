/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

/**
 * describes the public methods of {@link mondrian.rolap.SqlTupleReader}.
 * 
 * @author av
 * @since Nov 21, 2005
 */

public interface TupleReader {
    /**
     * provides the environment to create new members for a
     * hierarchy from SQL result.
     *
     * @author av
     * @since Nov 11, 2005
     */
    public interface MemberBuilder {
        /**
         * returns the All member of the hierarchy or null if the hierarchy does not have one
         */
        RolapMember getAllMember();

        /**
         * returns the <code>MemberCache</code> to look up members before creating them.
         */
        MemberCache getMemberCache();

        /**
         * creates a new member (together with its properties)
         * @see SqlMemberSource#makeMember(RolapMember, RolapLevel, Object, Object, boolean, ResultSet, Object, int)
         */
        RolapMember makeMember(RolapMember parentMember, RolapLevel childLevel,
                Object value, Object captionValue, boolean parentChild,
                ResultSet resultSet, Object key, int column) throws SQLException;
    }

    /**
     * adds a hierarchy to retrieve memers from
     */
    void addLevelMembers(RolapLevel level, MemberBuilder memberBuilder);

    /**
     * performs the read
     * @return a list of RolapMember, if a single level was read, a list of tuples (RolapMember[] instances) else.
     */
    List readTuples(Connection jdbcConnection);

    /**
     * Performs the read.
     *
     * @return a list of RolapMember, if a single level was read, a list of tuples (RolapMember[] instances) else.
     */
    List readTuples(DataSource dataSource);

    /**
     * Returns an object that uniquely identifies the Result that this
     * {@link TupleReader} would return. Clients may use this as a key for
     * caching the result.
     */
    Object getCacheKey();

}

// End TupleReader.java
