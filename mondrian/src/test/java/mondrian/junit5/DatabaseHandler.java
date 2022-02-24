package mondrian.junit5;

import java.io.Closeable;
import java.util.Map;

public interface DatabaseHandler extends Closeable {
    
    /**
     * 
     * @param props - properties
     * @return jdbc connection String
     */
    String setUpDatabase(Map<String,Object> props);

}
