package org. omg. cwm. foundation. datatypes;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Enumeration extends DataType {


// class scalar attributes
public Boolean getIsOrdered();
public void setIsOrdered( Boolean input);
// class references
public void setLiteral( Collection input);
public Collection getLiteral();
public void removeLiteral( EnumerationLiteral input);
// class operations
}


