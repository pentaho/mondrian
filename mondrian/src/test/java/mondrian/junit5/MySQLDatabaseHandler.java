package mondrian.junit5;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class MySQLDatabaseHandler implements DatabaseHandler {

    private static MySQLContainer<?> mySQLContainer;
    static Consumer<OutputFrame> x = t -> System.out.println(t.getUtf8String());
    private String user;
    private String pass;
    private String dbname;

    @Override
    public void close() throws IOException {
	if (mySQLContainer != null) {
	    mySQLContainer.stop();
	    mySQLContainer.close();
	}
    }

    @Override
    public boolean startDatabase(Map<String, Object> props) {
	dbname = props.getOrDefault("dbname", UUID.randomUUID().toString().replace("-","")).toString();
	user = props.getOrDefault("username", UUID.randomUUID().toString().replace("-","")).toString();
	pass = props.getOrDefault("password", UUID.randomUUID().toString().replace("-","")).toString();


	mySQLContainer = new MySQLContainer<>("mysql:5.7.34").withDatabaseName(dbname).withUsername(user)
		.withPassword(pass).withEnv("MYSQL_ROOT_HOST", "%").withLogConsumer(x)
		.withSharedMemorySize(2L * FileUtils.ONE_GB);

	mySQLContainer.start();
	return true;
//		Connection con = DriverManager.getConnection(url, "user", "pass");

    }

    @Override
    public String getJdbcUrl() {
	return mySQLContainer.getJdbcUrl()+"?user="+user+"&password="+pass+"&rewriteBatchedStatements=true";

    }





}
