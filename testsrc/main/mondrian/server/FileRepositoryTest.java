/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.server;

import junit.framework.TestCase;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

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
}
// End FileRepositoryTest.java