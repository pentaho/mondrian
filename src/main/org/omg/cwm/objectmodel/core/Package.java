package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public interface Package extends Namespace {
// class scalar attributes
// class references
public void setImportedElement( Collection input);


public Collection getImportedElement();
public void addImportedElement( ModelElement input);
public void removeImportedElement( ModelElement input);
// class operations
}


