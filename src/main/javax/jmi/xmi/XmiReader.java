package javax.jmi.xmi;

import javax.jmi.reflect.RefPackage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public interface XmiReader {
    public Collection read(InputStream stream, String URI, RefPackage extent) 
        throws IOException, MalformedXMIException;
    public Collection read(String URI, RefPackage extent) 
        throws IOException, MalformedXMIException;
}