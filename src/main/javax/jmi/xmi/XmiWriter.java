package javax.jmi.xmi;

import java.io.OutputStream;
import java.io.IOException;
import java.util.Collection;
import javax.jmi.reflect.RefPackage;

public interface XmiWriter {
    public void write(OutputStream stream, RefPackage extent, String xmiVersion)
        throws IOException;
    public void write(OutputStream stream, Collection objects, String xmiVersion)
        throws IOException;
}

