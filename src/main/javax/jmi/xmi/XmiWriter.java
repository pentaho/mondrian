package javax.jmi.xmi;

import javax.jmi.reflect.RefPackage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public interface XmiWriter {
    public void write(OutputStream stream, RefPackage extent, String xmiVersion)
        throws IOException;
    public void write(OutputStream stream, Collection objects, String xmiVersion)
        throws IOException;
}

