/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007 Bart Pappyn
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.List;
import java.util.ArrayList;

import mondrian.olap.Connection;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.rolap.cache.CachePool;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.test.FoodMartTestCase;
import mondrian.spi.impl.DataSourceChangeListenerImpl;
import mondrian.spi.impl.DataSourceChangeListenerImpl2;
import org.apache.log4j.Logger;

/**
 * Tests for testing the DataSourceChangeListener plugin.
 *
 * @author Bart Pappyn
 * @since Jan 05, 2007
 * @version $Id$
 */
public class DataSourceChangeListenerTest extends FoodMartTestCase {
    private static Logger logger = Logger.getLogger(DataSourceChangeListenerTest.class);
    SqlConstraintFactory scf = SqlConstraintFactory.instance();

          
    public DataSourceChangeListenerTest() {
        super();
    }

    public DataSourceChangeListenerTest(String name) {
        super(name);
    }

    /**
     * Test whether the data source plugin is able to tell mondrian
     * to read the hierarchy again.
     */
    public void testDataSourceChangeListenerPlugin() {
        
        CachePool.instance().flush();
        
        /* Use hard caching for testing. When using soft references, we can not test caching
        * because things may be garbage collected during the tests. */        
        SmartMemberReader smr = getSmartMemberReader("Store");
        smr.mapLevelToMembers.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapLevel, Object>,
                List<RolapMember>>());
        smr.mapMemberToChildren.setCache(
            new HardSmartCache<
                SmartMemberListCache.Key2<RolapMember, Object>,
                List<RolapMember>>());
        smr.mapKeyToMember = new HardSmartCache<Object, RolapMember>();
        
        // Create a dummy DataSource which will throw a 'bomb' if it is asked
        // to execute a particular SQL statement, but will otherwise behave
        // exactly the same as the current DataSource.
        SqlLogger sqlLogger = new SqlLogger();
        RolapUtil.threadHooks.set(sqlLogger);
                
        try {
            String s1, s2, s3, s4, s5;
            
            // Flush the cache, to ensure that the query gets executed.            
            RolapResult r1 = (RolapResult) executeQuery(
            "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r1);
            s1 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();            
            // s1 should not be empty
            assertNotSame("[]", s1);
            
            // Run query again, to make sure only cache is used
            RolapResult r2 = (RolapResult) executeQuery(
            "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r2);
            s2 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();                            
            assertEquals("[]", s2);
            
            // Attach dummy change listener that tells mondrian the datasource is never changed            
            smr.changeListener = new DataSourceChangeListenerImpl();
            
            // Run query again, to make sure only cache is used
            RolapResult r3 = (RolapResult) executeQuery(
            "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r3);
            s3 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();                            
            assertEquals("[]",s3);
            
            // Manually clear the cache to make compare sql result later on
            smr.mapKeyToMember.clear();
            smr.mapLevelToMembers.clear();
            smr.mapMemberToChildren.clear();
            // Run query again, to make sure only cache is used
            RolapResult r4 = (RolapResult) executeQuery(
            "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r4);
            s4 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();                            
            assertNotSame("[]",s4);                                                           
            
            // Attach dummy change listener that tells mondrian the datasource is always changed            
            smr.changeListener = new DataSourceChangeListenerImpl2();
            
            // Run query again, to make sure only cache is used
            RolapResult r5 = (RolapResult) executeQuery(
            "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r5);
            s5 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();                            
            assertEquals(s4,s5);                                               
        } finally {
            RolapUtil.threadHooks.set(null);
        }                
}
    
    private static class SqlLogger implements RolapUtil.ExecuteQueryHook {
        private final List<String> sqlQueries;

        public SqlLogger() {
            this.sqlQueries = new ArrayList<String>();                       
        }
        
        public void clear() {
            sqlQueries.clear();
        }
            
        public List<String> getSqlQueries() {
            return sqlQueries;
        }

        public void onExecuteQuery(String sql) {
            sqlQueries.add(sql);
        }
    }
    
    Result executeQuery(String mdx, Connection connection) {
        Query query = connection.parseQuery(mdx);
        return connection.execute(query);
    }

    SmartMemberReader getSmartMemberReader(String hierName) {
        Connection con = super.getConnection(false);
        return getSmartMemberReader(con, hierName);
    }

    SmartMemberReader getSmartMemberReader(Connection con, String hierName) {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader = (RolapSchemaReader) cube.getSchemaReader();
        RolapHierarchy hierarchy = (RolapHierarchy) cube.lookupHierarchy(hierName, false);
        assertNotNull(hierarchy);
        return (SmartMemberReader) hierarchy.getMemberReader(schemaReader.getRole());
    }
}

// End NonEmptyTest.java
