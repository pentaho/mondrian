package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public abstract interface ModelElement extends Element {
// class scalar attributes
public String getName();
public void setName( String input);
public String getVisibility();
public void setVisibility( String input);


// class references
public void setClientDependency( Collection input);
public Collection getClientDependency();
public void addClientDependency( Dependency input);
public void removeClientDependency( Dependency input);
public void setConstraint( Collection input);
public Collection getConstraint();
public void addConstraint( Constraint input);
public void removeConstraint( Constraint input);
public void setImporter( Collection input);
public Collection getImporter();
public void addImporter( org. omg. cwm. objectmodel. core. Package input);
public void removeImporter( org. omg. cwm. objectmodel. core. Package input);
public void setNamespace( Namespace input);
public Namespace getNamespace();
// class operations
}

