package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public abstract interface Namespace extends ModelElement {
// class scalar attributes
// class references
public void setOwnedElement( Collection input);


public Collection getOwnedElement();
public void removeOwnedElement( ModelElement input);
// class operations
}


