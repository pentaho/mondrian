package javax.jmi.reflect;

import java.io.Serializable;
import java.util.List;

public interface RefStruct extends Serializable {
    public List refFieldNames();
    public Object refGetValue(String fieldName);
    public List refTypeName();
    public boolean equals(Object other);
    public int hashCode();
}
