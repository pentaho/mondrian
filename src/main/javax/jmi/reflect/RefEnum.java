package javax.jmi.reflect;

import java.io.Serializable;
import java.util.List;

public interface RefEnum extends Serializable {
    public String toString();
    public List refTypeName();
    public boolean equals(Object other);
    public int hashCode();
}
