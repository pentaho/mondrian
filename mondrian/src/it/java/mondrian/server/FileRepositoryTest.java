/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2002-2024 Hitachi Vantara and others
 * All Rights Reserved.
 */

package mondrian.server;

import junit.framework.TestCase;
import mondrian.olap.MondrianServer;
import mondrian.server.FileRepository.CatalogInfo;
import org.olap4j.OlapConnection;

import java.util.Properties;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FileRepositoryTest extends TestCase {
  private String createContentStub( String datasourceName, String datasourceInfo, String catalogName ) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DataSources>\n"
      + "<DataSource>\n"
      + "<DataSourceName>" + datasourceName + "</DataSourceName>\n"
      + "<DataSourceDescription>Pentaho BI Platform Datasources</DataSourceDescription>\n"
      + "<URL>/pentaho/Xmla</URL>\n"
      + "<DataSourceInfo>" + datasourceInfo + "</DataSourceInfo>\n"
      + "<ProviderName>PentahoXMLA</ProviderName>\n"
      + "<ProviderType>MDP</ProviderType>\n"
      + "<AuthenticationMode>Unauthenticated</AuthenticationMode>\n"
      + "<Catalogs>\n"
      + "<Catalog name=\"" + catalogName + "\">\n"
      + "<DataSourceInfo>Jdbc=example.com;EnableXmla=false;Provider=mondrian;Datasource=\"SampleData\";"
      + "overwrite=\"true\"</DataSourceInfo>\n"
      + "<Definition>mondrian:/SampleData</Definition>\n"
      + "</Catalog>\n"
      + "</Catalogs>\n"
      + "</DataSource>\n"
      + "</DataSources>\n";
  }

  public void testGetServerInfo_ifGetDataSourceInfoIsCalled() {
    final String datasourceName = "DATASOURCE_NAME";
    final String datasourceInfo = "DATASOURCE_INFO";
    final String catalogName = "SampleData";
    final String contentStub = createContentStub( datasourceName, datasourceInfo, catalogName );

    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );

    assertTrue( databaseInfo.getProperties().containsValue( datasourceInfo ) );
  }

  public void testDiscoverDatasourceLegacyNameMatch() throws Exception {
    final String datasourceName = "Pentaho Mondrian";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SampleData";
    final String contentStub = createContentStub( datasourceName, datasourceInfo, catalogName );

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNotNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );
    final String dsInfo = "Provider=mondrian";
    final CatalogInfo cInfo = databaseInfo.catalogMap.get( catalogName );
    final OlapConnection oc = mock( OlapConnection.class );

    // Make sure to short circuit the connection creation internal call...
    doReturn( oc ).when( fileRepositoryMock ).getConnection( cInfo, mockServer, null, mockProps );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    // OK make the call
    OlapConnection ocPrime =
      fileRepositoryMock.getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made
    verify( fileRepositoryMock, times( 1 ) ).getConnection( cInfo, mockServer, null, mockProps );

    // Now do the same w/ the DS Info name instead. Legacy mode.

    // We'll try to call into this method with the DSInfo instead of the name of the database
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, dsInfo, catalogName, null, mockProps );

    // Make the call
    ocPrime = fileRepositoryMock.getConnection( mockServer, dsInfo, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made. 2 calls by now.
    verify( fileRepositoryMock, times( 2 ) ).getConnection( cInfo, mockServer, null, mockProps );
  }

  public void testDatabaseNameMatchCatalogName() throws Exception {
    final String realDatasourceName = "Pentaho Mondrian";
    final String datasourceName = "SteelWheels";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SteelWheels";
    final String contentStub = createContentStub( realDatasourceName, datasourceInfo, catalogName );

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );
    final String dsInfo = "Provider=mondrian";
    final CatalogInfo cInfo = serverInfo.getDatasourceMap().get( realDatasourceName ).catalogMap.get( catalogName );
    final OlapConnection oc = mock( OlapConnection.class );

    // Make sure to short circuit the connection creation internal call...
    doReturn( oc ).when( fileRepositoryMock ).getConnection( cInfo, mockServer, null, mockProps );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    // OK make the call
    OlapConnection ocPrime =
      fileRepositoryMock.getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made
    verify( fileRepositoryMock, times( 1 ) ).getConnection( cInfo, mockServer, null, mockProps );

    // Now do the same w/ the DS Info name instead. Legacy mode.

    // We'll try to call into this method with the DSInfo instead of the name of the database.
    doCallRealMethod().when( fileRepositoryMock ).getConnection( mockServer, dsInfo, catalogName, null, mockProps );

    // Make the call.
    ocPrime = fileRepositoryMock.getConnection( mockServer, dsInfo, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made. 2 calls by now.
    verify( fileRepositoryMock, times( 2 ) ).getConnection( cInfo, mockServer, null, mockProps );
  }

  public void testGetConnectionInvalidDatabases() throws Exception {
    final String contentStub = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DataSources/>\n";

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();
    doCallRealMethod().when( fileRepositoryMock ).getConnection( mockServer, null, null, null, mockProps );

    try {
      fileRepositoryMock.getConnection( mockServer, null, null, null, mockProps );
      fail();
    } catch ( Exception e ) {
      assertEquals( "No databases configured on this server", e.getMessage() );
    }
  }

  public void testDatabaseNameMatchCatalogNameAndDatabaseNameIsInvalid() throws Exception {
    final String datasourceName = "Pentaho Mondrian";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SteelWheels";
    final String contentStub = createContentStub( datasourceName, datasourceInfo, catalogName );

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );
    final String dsInfo = "Provider=mondrian";
    final CatalogInfo cInfo = serverInfo.getDatasourceMap().get( datasourceName ).catalogMap.get( catalogName );
    final OlapConnection oc = mock( OlapConnection.class );

    // Make sure to short circuit the connection creation internal call...
    doReturn( oc ).when( fileRepositoryMock ).getConnection( cInfo, mockServer, null, mockProps );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, null, catalogName, null, mockProps );

    // OK make the call
    OlapConnection ocPrime =
      fileRepositoryMock.getConnection( mockServer, null, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made
    verify( fileRepositoryMock, times( 1 ) ).getConnection( cInfo, mockServer, null, mockProps );

    // Now do the same w/ the DS Info name instead. Legacy mode.

    // We'll try to call into this method with the DSInfo instead of the name of the database.
    doCallRealMethod().when( fileRepositoryMock ).getConnection( mockServer, dsInfo, catalogName, null, mockProps );

    // Make the call.
    ocPrime = fileRepositoryMock.getConnection( mockServer, dsInfo, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made. 2 calls by now.
    verify( fileRepositoryMock, times( 2 ) ).getConnection( cInfo, mockServer, null, mockProps );
  }

  public void testInvalidCatalogs() throws Exception {
    final String datasourceName = "Pentaho Mondrian";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SampleData";
    final String contentStub = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DataSources>\n"
      + "<DataSource>\n"
      + "<DataSourceName>" + datasourceName + "</DataSourceName>\n"
      + "<DataSourceDescription>Pentaho BI Platform Datasources</DataSourceDescription>\n"
      + "<URL>/pentaho/Xmla</URL>\n"
      + "<DataSourceInfo>" + datasourceInfo + "</DataSourceInfo>\n"
      + "<ProviderName>PentahoXMLA</ProviderName>\n"
      + "<ProviderType>MDP</ProviderType>\n"
      + "<AuthenticationMode>Unauthenticated</AuthenticationMode>\n"
      + "<Catalogs/>\n"
      + "</DataSource>\n"
      + "</DataSources>\n";

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNotNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    try {
      fileRepositoryMock.getConnection( mockServer, datasourceName, catalogName, null, mockProps );
      fail();
    } catch ( Exception e ) {
      assertEquals( "Mondrian Error:Internal error: Unknown catalog '" + catalogName + "'", e.getMessage() );
    }
  }

  @SuppressWarnings( "ExtractMethodRecommender" )
  public void testInvalidCatalogsAndNullCatalogName() throws Exception {
    final String datasourceName = "Pentaho Mondrian";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String contentStub = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DataSources>\n"
      + "<DataSource>\n"
      + "<DataSourceName>" + datasourceName + "</DataSourceName>\n"
      + "<DataSourceDescription>Pentaho BI Platform Datasources</DataSourceDescription>\n"
      + "<URL>/pentaho/Xmla</URL>\n"
      + "<DataSourceInfo>" + datasourceInfo + "</DataSourceInfo>\n"
      + "<ProviderName>PentahoXMLA</ProviderName>\n"
      + "<ProviderType>MDP</ProviderType>\n"
      + "<AuthenticationMode>Unauthenticated</AuthenticationMode>\n"
      + "<Catalogs/>\n"
      + "</DataSource>\n"
      + "</DataSources>\n";

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNotNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, null, null, mockProps );

    try {
      fileRepositoryMock.getConnection( mockServer, datasourceName, null, null, mockProps );
      fail();
    } catch ( Exception e ) {
      assertEquals( "No catalogs in the database named Pentaho Mondrian", e.getMessage() );
    }
  }

  public void testUnknownDatabase() throws Exception {
    final String realDatasourceName = "Pentaho Mondrian";
    final String datasourceName = "SteelWheels";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SteelWheels";
    final String contentStub = createContentStub( realDatasourceName, datasourceInfo, catalogName );

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );
    final CatalogInfo cInfo = serverInfo.getDatasourceMap().get( realDatasourceName ).catalogMap.get( catalogName );
    final OlapConnection oc = mock( OlapConnection.class );

    // Make sure to short circuit the connection creation internal call...
    doReturn( oc ).when( fileRepositoryMock ).getConnection( cInfo, mockServer, null, mockProps );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, null, null, mockProps );

    try {
      fileRepositoryMock.getConnection( mockServer, datasourceName, null, null, mockProps );
      fail();
    } catch ( Exception e ) {
      assertEquals( "Mondrian Error:Internal error: Unknown database '" + datasourceName + "'", e.getMessage() );
    }
  }

  public void testDiscoverDatasourceLegacyNameMatchAndCatalogNameIsInvalid() throws Exception {
    final String datasourceName = "Pentaho Mondrian";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SampleData";
    final String contentStub = createContentStub( datasourceName, datasourceInfo, catalogName );

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNotNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );
    final String dsInfo = "Provider=mondrian";
    final CatalogInfo cInfo = databaseInfo.catalogMap.get( catalogName );
    final OlapConnection oc = mock( OlapConnection.class );

    // Make sure to short circuit the connection creation internal call...
    doReturn( oc ).when( fileRepositoryMock ).getConnection( cInfo, mockServer, null, mockProps );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    // OK make the call
    OlapConnection ocPrime =
      fileRepositoryMock.getConnection( mockServer, datasourceName, catalogName, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made
    verify( fileRepositoryMock, times( 1 ) ).getConnection( cInfo, mockServer, null, mockProps );

    // Now do the same w/ the DS Info name instead. Legacy mode.

    // We'll try to call into this method with the DSInfo instead of the name of the database
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, dsInfo, null, null, mockProps );

    // Make the call
    ocPrime = fileRepositoryMock.getConnection( mockServer, dsInfo, null, null, mockProps );

    // Check we got a proper output
    assertSame( ocPrime, oc );

    // Make sure the protected call was made. 2 calls by now.
    verify( fileRepositoryMock, times( 2 ) ).getConnection( cInfo, mockServer, null, mockProps );
  }

  public void testDiscoverDatasourceLegacyNameMatchAndCatalogNameIsInvalidAndThrowsException() throws Exception {
    final String datasourceName = "Pentaho Mondrian";
    final String datasourceInfo = "Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee";
    final String catalogName = "SampleData";
    final String contentStub = createContentStub( datasourceName, datasourceInfo, catalogName );

    // Mocks
    FileRepository fileRepositoryMock = mock( FileRepository.class );
    RepositoryContentFinder repositoryContentFinderMock = mock( RepositoryContentFinder.class );

    // Return the content we want from the RCF
    doReturn( repositoryContentFinderMock ).when( fileRepositoryMock ).getRepositoryContentFinder();
    doReturn( contentStub ).when( repositoryContentFinderMock ).getContent();

    // Calling getServerInfo's actual implementation
    doCallRealMethod().when( fileRepositoryMock ).getServerInfo();

    // Give it a try
    final FileRepository.ServerInfo serverInfo = fileRepositoryMock.getServerInfo();

    // Some sanity checks
    FileRepository.DatabaseInfo databaseInfo = serverInfo.getDatasourceMap().get( datasourceName );
    assertNotNull( "Database not found by name", databaseInfo );

    // More mocks
    final Properties mockProps = mock( Properties.class );
    final MondrianServer mockServer = mock( MondrianServer.class );
    final CatalogInfo cInfo = databaseInfo.catalogMap.get( catalogName );

    // Make sure to short circuit the connection creation internal call...
    doThrow( RuntimeException.class ).when( fileRepositoryMock ).getConnection( cInfo, mockServer, null, mockProps );

    // We'll try to call into this method with the DS name
    doCallRealMethod().when( fileRepositoryMock )
      .getConnection( mockServer, datasourceName, null, null, mockProps );

    try {
      fileRepositoryMock.getConnection( mockServer, datasourceName, null, null, mockProps );
      fail();
    } catch ( Exception e ) {
      assertEquals( "Mondrian Error:Internal error: No suitable connection found", e.getMessage() );
    }
  }
}
