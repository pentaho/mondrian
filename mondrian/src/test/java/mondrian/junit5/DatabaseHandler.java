package mondrian.junit5;

import java.io.Closeable;
import java.util.Map;

public interface DatabaseHandler extends Closeable {
    

    boolean startDatabase(Map<String,Object> props);
    String getJdbcUrl();

}
