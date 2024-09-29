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

package mondrian.olap4j;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import mondrian.olap.MondrianServer;
import mondrian.olap.Util;
import mondrian.olap.Util.PropertyList;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapConnectionProperties;

public class XmlaExtraTest extends TestCase {
    /**
     * This test makes sure that the value of
     * {@link RolapConnectionProperties#JdbcPassword} isn't leaked through
     * the XmlaExtra interface.
     */
     public void testGetDataSourceDoesntLeakPassword() throws Exception {
        final List<Map<String, Object>> expectedList =
            new ArrayList<Map<String,Object>>();
        final Map<String, Object> expectedMap =
            new HashMap<String, Object>();
        expectedMap.put(
            "DataSourceInfo",
            "Provider=Mondrian;Jdbc=foo;JdbcPassword=bar;JdbcUser=bacon");
        expectedList.add(expectedMap);

        final MondrianServer server = mock(MondrianServer.class);
        final RolapConnection rConn = mock(RolapConnection.class);
        final MondrianOlap4jConnection conn =
            mock(MondrianOlap4jConnection.class);
        final MondrianOlap4jExtra extra =
            mock(MondrianOlap4jExtra.class);

        doReturn(expectedList).when(server).getDatabases(rConn);
        doReturn(server).when(rConn).getServer();
        doReturn(rConn).when(conn).getMondrianConnection();
        doCallRealMethod().when(extra).getDataSources(conn);

        for (Map<String, Object> ds : extra.getDataSources(conn)) {
            final PropertyList props =
                Util.parseConnectString(
                    String.valueOf(ds.get("DataSourceInfo")));
            assertNull(
                props.get(RolapConnectionProperties.Jdbc.name()));
            assertNull(
                props.get(RolapConnectionProperties.JdbcUser.name()));
            assertNull(
                props.get(RolapConnectionProperties.JdbcPassword.name()));
        }
     }
}
//End XmlaExtraTest.java
