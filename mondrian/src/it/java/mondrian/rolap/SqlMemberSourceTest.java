/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import mondrian.olap.Dimension;
import mondrian.olap.Level;
import mondrian.olap.MondrianDef;
import mondrian.test.FoodMartTestCase;
import mondrian.util.ByteString;

public class SqlMemberSourceTest extends FoodMartTestCase {
  private static final String STORE_TABLE = "store";
  private static final String MY_SQL_PRODUCT_VERSION = "3.23.58";
  private static final String MY_SQL_PRODUCT_NAME = "MySQL";
  private static final ByteString MD5_BYTE_STRING = new ByteString( "TEST SCHEMA".getBytes() );
  private SqlMemberSource sqlMemberSource;
  private static RolapHierarchy storeHierarchyMock;
  private RolapLevel allLevelMock;
  private RolapLevel countryLevelMock;
  private RolapLevel stateLevelMock;
  private RolapLevel cityLevelMock;
  private DataSource dataSourceMock;

  @Override
  public void setUp() throws Exception {
    // create Store hierarchy mock
    storeHierarchyMock = getStoreHierarchyMock();
    dataSourceMock = getDataSourceMock();
    // create mocked levels
    allLevelMock = getAllLevelMock();
    countryLevelMock = getCountryLevelMock();
    stateLevelMock = getStateLevelMock();
    cityLevelMock = getCityLevelMock();
    Level[] storeLevels = new RolapLevel[] { allLevelMock, countryLevelMock, stateLevelMock, cityLevelMock };
    // add mocked levels to Store hierarchy
    addLevels( storeHierarchyMock, storeLevels );
  }

  public void testMakeLevelMemberCountSql() throws Exception {
    boolean[] mustCount = new boolean[1];
    sqlMemberSource = new SqlMemberSource( storeHierarchyMock );

    String result = runMakeLeveMemberCountSql( cityLevelMock, dataSourceMock, mustCount );
    assertNotNull( result );
    assertEquals( "select count(DISTINCT `store`.`store_city`, `store`.`store_state`, `store`.`store_country`) as `c0`", result );
  }

  private String runMakeLeveMemberCountSql( RolapLevel level, DataSource dataSource, boolean[] mustCount ) throws Exception {
    Method method = sqlMemberSource.getClass().getDeclaredMethod( "makeLevelMemberCountSql", RolapLevel.class, DataSource.class, boolean[].class );
    method.setAccessible( true );
    String result = (String) method.invoke( sqlMemberSource, level, dataSource, mustCount );
    return result;
  }

  // Below there are mocked levels in Store hierarchy of Store dimension according to the default test foodmart schema
  private static RolapLevel getAllLevelMock() {
    RolapLevel rlLevelMock = mock( RolapLevel.class );
    return rlLevelMock;
  }

  private static RolapLevel getCountryLevelMock() throws Exception {
    mondrian.olap.MondrianDef.Level level = new MondrianDef.Level( wrapStrSources( getStoreCountry() ) );
    RolapLevel rlLevel = new RolapLevel( storeHierarchyMock, 0, level );
    RolapLevel rlLevelSpy = spy( rlLevel );
    return rlLevelSpy;
  }

  private static RolapLevel getStateLevelMock() throws Exception {
    mondrian.olap.MondrianDef.Level level = new MondrianDef.Level( wrapStrSources( getStoreState() ) );
    RolapLevel rlLevel = new RolapLevel( storeHierarchyMock, 0, level );
    RolapLevel rlLevelSpy = spy( rlLevel );
    return rlLevelSpy;
  }

  // Below there are mocked levels in Store hierarchy of Store dimension according to the default test foodmart schema
  private static RolapLevel getCityLevelMock() throws Exception {
    mondrian.olap.MondrianDef.Level level = new MondrianDef.Level( wrapStrSources( getStoreCity() ) );
    RolapLevel rlLevel = new RolapLevel( storeHierarchyMock, 0, level );
    RolapLevel rlLevelSpy = spy( rlLevel );
    return rlLevelSpy;
  }

  // Mock for the Store hierarchy
  private RolapHierarchy getStoreHierarchyMock() {
    MondrianDef.Relation relationMock = mock( MondrianDef.Relation.class );
    when( relationMock.getAlias() ).thenReturn( STORE_TABLE );
    Dimension dimMock = mock( Dimension.class );
    when( dimMock.getDimensionType() ).thenReturn( null );

    SchemaKey sKeyMock = mock( SchemaKey.class );
    RolapConnection rlConnectionMock = mock( RolapConnection.class );
    RolapSchema rlSchema = new RolapSchema( sKeyMock, MD5_BYTE_STRING, rlConnectionMock );
    RolapHierarchy rlHierarchyStoreMock = mock( RolapHierarchy.class );

    when( rlConnectionMock.getDataSource() ).thenReturn( dataSourceMock );

    when( rlHierarchyStoreMock.getRolapSchema() ).thenReturn( rlSchema );
    when( rlHierarchyStoreMock.getUniqueName() ).thenReturn( "[Store]" );
    when( rlHierarchyStoreMock.getUniqueTable() ).thenReturn( relationMock );
    when( rlHierarchyStoreMock.getDimension() ).thenReturn( dimMock );
    when( rlHierarchyStoreMock.tableExists( STORE_TABLE ) ).thenReturn( true );
    return rlHierarchyStoreMock;
  }

  private static void addLevels( RolapHierarchy hr, Level[] levels ) {
    // mocking depth
    for ( int i = 0; i < levels.length; i++ ) {
      when( levels[i].getDepth() ).thenReturn( i );
    }
    when( storeHierarchyMock.getLevels() ).thenReturn( levels );
  }

  private static Connection getMySQLconnectionMock() throws Exception {
    Connection mySQLconnectionMock = mock( Connection.class );

    DatabaseMetaData dbMetaDataMock = mock( DatabaseMetaData.class );
    Statement statementMock = mock( Statement.class );
    ResultSet getVersionResultSetMock = mock( ResultSet.class );
    // mocking mySQL connection
    when( mySQLconnectionMock.getMetaData() ).thenReturn( dbMetaDataMock );
    when( mySQLconnectionMock.createStatement() ).thenReturn( statementMock );
    // Mocking defining version of Impala db
    when( statementMock.executeQuery( "select version()" ) ).thenReturn( getVersionResultSetMock );
    // Mock result set as it contains MySQL version before 4
    when( getVersionResultSetMock.next() ).thenReturn( true ).thenReturn( false );
    when( getVersionResultSetMock.getString( 1 ) ).thenReturn( MY_SQL_PRODUCT_VERSION );
    // mock mySQL meta data
    when( dbMetaDataMock.getDatabaseProductName() ).thenReturn( MY_SQL_PRODUCT_NAME );
    when( dbMetaDataMock.getDatabaseProductVersion() ).thenReturn( MY_SQL_PRODUCT_VERSION );
    return mySQLconnectionMock;
  }

  private static DataSource getDataSourceMock() throws Exception {
    DataSource dSourceMock = mock( DataSource.class );
    Connection mySQLconnectionMock = getMySQLconnectionMock();
    when( dSourceMock.getConnection() ).thenReturn( mySQLconnectionMock );
    return dSourceMock;
  }

  private static DOMWrapper wrapStrSources( String resStr ) throws XOMException {
    final Parser xmlParser = XOMUtil.createDefaultParser();
    final DOMWrapper def = xmlParser.parse( resStr );
    return def;
  }

  private static String getStoreCountry() {
    return "<Level name=\"Store Country\" table=\"store\" column=\"store_country\" uniqueMembers=\"true\"/>";
  }

  private static String getStoreState() {
    return "<Level name=\"Store State\" table=\"store\" column=\"store_state\" uniqueMembers=\"false\"/>";
  }

  private static String getStoreCity() {
    return "<Level name=\"Store City\" table=\"store\" column=\"store_city\" uniqueMembers=\"false\"/>";
  }

}
