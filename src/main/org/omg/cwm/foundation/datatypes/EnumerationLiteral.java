package org. omg. cwm. foundation. datatypes;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface EnumerationLiteral extends ModelElement {


// class scalar attributes
public Expression getValue();
public void setValue( Expression input);
// class references
public void setEnumeration( Enumeration input);
public Enumeration getEnumeration();
// class operations
}


