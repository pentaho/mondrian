package org. omg. cwm. objectmodel. core;
import java. util. List; import java. util. Collection;


public interface Constraint extends ModelElement {
// class scalar attributes
public BooleanExpression getBody();
public void setBody( BooleanExpression input);
// class references
public void setConstrainedElement( Collection input);
public List getConstrainedElement();
public void addConstrainedElement( ModelElement input);
public void removeConstrainedElement( ModelElement input);
public void addConstrainedElementBefore( ModelElement before, ModelElement input);


public void addConstrainedElementAfter( ModelElement before, ModelElement input);
public void moveConstrainedElementBefore( ModelElement before, ModelElement input);
public void moveConstrainedElementAfter( ModelElement before, ModelElement input);
// class operations
}
