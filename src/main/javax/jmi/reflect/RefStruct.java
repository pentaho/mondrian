package javax.jmi.reflect;

import java.util.List;
import java.io.Serializable;

public interface RefStruct extends Serializable {
    public List refFieldNames();
    public Object refGetValue(String fieldName);
    public List refTypeName();
    public boolean equals(Object other);
    public int hashCode();
}
