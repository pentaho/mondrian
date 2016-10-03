/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.server.FileRepository.CatalogInfo;

import junit.framework.TestCase;

import org.olap4j.OlapConnection;

import java.util.Properties;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FileRepositoryTest extends TestCase {

     public void testGetServerInfo_ifGetDataSourceInfoIsCalled() {
        final String datasourceNameStub = "DATASOURCE_NAME";
        final String datasourceInfoStub = "DATASOURCE_INFO";
        final String contentStub = "<?xml version=\"1.0\""
        + " encoding=\"UTF-8\"?>\n"
                + "<DataSources>\n"
                + "<DataSource>\n"
                + "<DataSourceName>"
                + datasourceNameStub
                + "</DataSourceName>\n"
                + "<DataSourceDescription>Pentaho BI Platform Datasources</DataSourceDescription>\n"
                + "<URL>/pentaho/Xmla</URL>\n"
                + "<DataSourceInfo>"
                + datasourceInfoStub
                + "</DataSourceInfo>\n"
                + "<ProviderName>PentahoXMLA</ProviderName>\n"
                + "<ProviderType>MDP</ProviderType>\n"
                + "<AuthenticationMode>Unauthenticated</AuthenticationMode>\n"
                + "<Catalogs>\n"
                + "<Catalog name=\"SampleData\">\n"
                + "<DataSourceInfo>DataSource=SampleData;EnableXmla=false;Provider=mondrian;Datasource=\"SampleData\";overwrite=\"true\"</DataSourceInfo>\n"
                + "<Definition>mondrian:/SampleData</Definition>\n"
                + "</Catalog>\n"
                + "</Catalogs>\n"
                + "</DataSource>\n"
                + "</DataSources>\n";

        FileRepository fileRepositoryMock = mock(FileRepository.class);
        RepositoryContentFinder repositoryContentFinderMock =
            mock(RepositoryContentFinder.class);

        doReturn(repositoryContentFinderMock).when(fileRepositoryMock)
                .getRepositoryContentFinder();
        doReturn(contentStub).when(repositoryContentFinderMock).getContent();

        doCallRealMethod().when(fileRepositoryMock).getServerInfo();

        FileRepository.ServerInfo serverInfo =
            fileRepositoryMock.getServerInfo();

        FileRepository.DatabaseInfo databaseInfo =
            serverInfo.getDatasourceMap().get(datasourceNameStub);

        assertTrue(
            databaseInfo.getProperties().containsValue(datasourceInfoStub));
     }

     public void testDiscoverDatasourceLegacyNameMatch() throws Exception {
         final String contentStub =
             "<?xml version=\"1.0\""
             + " encoding=\"UTF-8\"?>\n"
             + "<DataSources>\n"
             + "<DataSource>\n"
             + "<DataSourceName>Pentaho Mondrian</DataSourceName>\n"
             + "<DataSourceDescription>Pentaho BI Platform Datasources</DataSourceDescription>\n"
             + "<URL>/pentaho/Xmla</URL>\n"
             + "<DataSourceInfo>Provider=mondrian;Jdbc=SomethingAwfulWhichPeopleCantSee</DataSourceInfo>\n"
             + "<ProviderName>PentahoXMLA</ProviderName>\n"
             + "<ProviderType>MDP</ProviderType>\n"
             + "<AuthenticationMode>Unauthenticated</AuthenticationMode>\n"
             + "<Catalogs>\n"
             + "<Catalog name=\"SampleData\">\n"
             + "<DataSourceInfo>Jdbc=example.com;EnableXmla=false;Provider=mondrian;Datasource=\"SampleData\";overwrite=\"true\"</DataSourceInfo>\n"
             + "<Definition>mondrian:/SampleData</Definition>\n"
             + "</Catalog>\n"
             + "</Catalogs>\n"
             + "</DataSource>\n"
             + "</DataSources>\n";

         // Mocks.
         FileRepository fileRepositoryMock = mock(FileRepository.class);
         RepositoryContentFinder repositoryContentFinderMock =
             mock(RepositoryContentFinder.class);

         // Return the content we want from the RCF.
         doReturn(repositoryContentFinderMock).when(fileRepositoryMock)
                 .getRepositoryContentFinder();
         doReturn(contentStub).when(repositoryContentFinderMock).getContent();

         // Calling getServerInfo's actual implementation
         doCallRealMethod().when(fileRepositoryMock).getServerInfo();

         // Give it a try.
         final FileRepository.ServerInfo serverInfo =
             fileRepositoryMock.getServerInfo();

         // Some sanity checks.
         FileRepository.DatabaseInfo databaseInfo =
             serverInfo.getDatasourceMap().get("Pentaho Mondrian");
         assertNotNull("Database not found by name", databaseInfo);

         // Moar mocks.
         final Properties mockProps = mock(Properties.class);
         final MondrianServer mockServer = mock(MondrianServer.class);
         final String databaseName =
             "Pentaho Mondrian";
         final String dsInfo =
             "Provider=mondrian";
         final String catalogName = "SampleData";
         final String roleName = null;
         final CatalogInfo cInfo = databaseInfo.catalogMap.get("SampleData");
         final OlapConnection oc = mock(OlapConnection.class);

         // Make sure to short circuit the connection creation internal call...
         doReturn(oc).when(fileRepositoryMock)
             .getConnection(cInfo, mockServer, roleName, mockProps);

         // We'll try to call into this method with the DS name.
         doCallRealMethod().when(fileRepositoryMock)
             .getConnection(
                 mockServer, databaseName, catalogName, roleName, mockProps);

         // OK make the call.
         OlapConnection ocPrime = fileRepositoryMock.getConnection(
             mockServer, databaseName, catalogName, roleName, mockProps);

         // Check we got a proper output.
         assertTrue(ocPrime == oc);
         // Make sure the protected call was made
         verify(fileRepositoryMock, times(1))
             .getConnection(cInfo, mockServer, roleName, mockProps);

         // *** Now do the same w/ the DS Info name instead. Legacy mode.

         // We'll try to call into this method with the DSInfo instead of the
         // name of the database.
         doCallRealMethod().when(fileRepositoryMock)
             .getConnection(
                 mockServer, dsInfo, catalogName, roleName, mockProps);

         // Make the call.
         ocPrime = fileRepositoryMock.getConnection(
             mockServer, dsInfo, catalogName, roleName, mockProps);

         // Check we got a proper output.
         assertTrue(ocPrime == oc);
         // Make sure the protected call was made. 2 calls by now.
         verify(fileRepositoryMock, times(2))
             .getConnection(cInfo, mockServer, roleName, mockProps);
      }
}
// End FileRepositoryTest.java