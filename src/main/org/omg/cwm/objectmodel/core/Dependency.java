package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public interface Dependency extends ModelElement {
// class scalar attributes
public String getKind();
public void setKind( String input);
// class references
public void setClient( Collection input);
public Collection getClient();
public void addClient( ModelElement input);
public void removeClient( ModelElement input);
public void setSupplier( Collection input);
public Collection getSupplier();
public void addSupplier( ModelElement input);
public void removeSupplier( ModelElement input);
// class operations
}
