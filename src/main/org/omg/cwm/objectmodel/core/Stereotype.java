package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public interface Stereotype extends ModelElement {
// class scalar attributes
public String getBaseClass();
public void setBaseClass( String input);
// class references
public void setExtendedElement( Collection input);
public Collection getExtendedElement();
public void addExtendedElement( ModelElement input);
public void removeExtendedElement( ModelElement input);
public void setRequiredTag( Collection input);
public Collection getRequiredTag();
public void removeRequiredTag( TaggedValue input);
public void setStereotypeConstraint( Collection input);
public Collection getStereotypeConstraint();
public void removeStereotypeConstraint( Constraint input);
// class operations
}


