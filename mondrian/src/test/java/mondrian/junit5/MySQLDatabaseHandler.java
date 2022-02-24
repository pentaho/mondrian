package mondrian.junit5;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.OutputFrame;

public class MySQLDatabaseHandler implements DatabaseHandler {

    private static MySQLContainer<?> mySQLContainer;
    static Consumer<OutputFrame> x = t -> System.out.println(t.getUtf8String());

    @Override
    public void close() throws IOException {
	if (mySQLContainer != null) {
	    mySQLContainer.stop();
	    mySQLContainer.close();
	}
    }

    @Override
    public String setUpDatabase(Map<String, Object> props) {
	String user=props.getOrDefault("username", UUID.randomUUID().toString().replace("-","")).toString();
	String pass=props.getOrDefault("password", UUID.randomUUID().toString().replace("-","")).toString();


	mySQLContainer = new MySQLContainer<>("mysql:5.7.34").withDatabaseName(user).withUsername(pass)
		.withPassword("pass").withEnv("MYSQL_ROOT_HOST", "%").withLogConsumer(x);

	mySQLContainer.start();

	return mySQLContainer.getJdbcUrl()+"?user="+user+"&password="+pass;
//		Connection con = DriverManager.getConnection(url, "user", "pass");

    }





}
