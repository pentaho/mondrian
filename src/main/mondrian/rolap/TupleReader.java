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
 * Describes the public methods of {@link mondrian.rolap.SqlTupleReader}.
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public interface TupleReader {
    /**
     * Factory to create new members for a
     * hierarchy from SQL result.
     *
     * @author av
     * @since Nov 11, 2005
     */
    public interface MemberBuilder {

        /**
         * Returns the <code>MemberCache</code> to look up members before
         * creating them.
         */
        MemberCache getMemberCache();

        /**
         * Creates a new member (together with its properties).
         * @see SqlMemberSource#makeMember(RolapMember, RolapLevel, Object, Object, boolean, ResultSet, Object, int)
         */
        RolapMember makeMember(RolapMember parentMember, RolapLevel childLevel,
                Object value, Object captionValue, boolean parentChild,
                ResultSet resultSet, Object key, int column) throws SQLException;
    }

    /**
     * Adds a hierarchy to retrieve members from.
     *
     * @param level level that the members correspond to
     * @param memberBuilder used to build new members for this level
     * @param srcMembers if set, array of enumerated members that make up
     * this level
     */
    void addLevelMembers(
        RolapLevel level, MemberBuilder memberBuilder,
        RolapMember[] srcMembers);

    /**
     * Performs the read.
     *
     * @return a list of RolapMember[]
     */
    List<RolapMember[]> readTuples(
        DataSource dataSource,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult);

    /**
     * Performs the read.
     *
     * @param dataSource source for reading tuples
     * @param partialResult partially cached result that should be used
     * instead of executing sql query
     * @param newPartialResult if non-null, return the result of the read;
     * note that this is a subset of the full return list

     * @return a list of RolapMember
     */
    List<RolapMember> readMembers(
        DataSource dataSource,
        List<List<RolapMember>> partialResult,
        List<List<RolapMember>> newPartialResult);

    /**
     * Returns an object that uniquely identifies the Result that this
     * {@link TupleReader} would return. Clients may use this as a key for
     * caching the result.
     */
    Object getCacheKey();

}

// End TupleReader.java
