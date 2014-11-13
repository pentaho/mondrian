package mondrian.server;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import junit.framework.TestCase;

public class FileRepositoryTest extends TestCase {

	public void testGetServerInfo_ifGetDataSourceInfoIsCalled() {

		final String datasourceNameStub = "DATASOURCE_NAME";
		final String datasourceInfoStub = "DATASOURCE_INFO";
		final String contentStub = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
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
		RepositoryContentFinder repositoryContentFinderMock = mock(RepositoryContentFinder.class);

		doReturn(repositoryContentFinderMock).when(fileRepositoryMock)
				.getRepositoryContentFinder();
		doReturn(contentStub).when(repositoryContentFinderMock).getContent();

		doCallRealMethod().when(fileRepositoryMock).getServerInfo();

		FileRepository.ServerInfo serverInfo = fileRepositoryMock
				.getServerInfo();

		// in reality DATASOURCE_NAME is Provider=Mondrian for example
		FileRepository.DatabaseInfo databaseInfo = (FileRepository.DatabaseInfo) serverInfo
				.getDatasourceMap().get(datasourceNameStub);

		assertTrue(databaseInfo.getProperties().containsValue(
				datasourceInfoStub));
	}

}
