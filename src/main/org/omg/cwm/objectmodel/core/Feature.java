package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public abstract interface Feature extends ModelElement {
// class scalar attributes
public String getOwnerScope();
public void setOwnerScope( String input);
// class references
public void setOwner( Classifier input);
public Classifier getOwner();
// class operations
}


