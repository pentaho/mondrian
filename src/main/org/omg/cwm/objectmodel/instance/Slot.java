package org. omg. cwm. objectmodel. instance;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Slot extends ModelElement {
// class scalar attributes
// class references
public void setObject( org. omg. cwm. objectmodel. instance. Object input);


public org. omg. cwm. objectmodel. instance. Object getObject();
public void setValue( Instance input);
public Instance getValue();
public void setFeature( StructuralFeature input);
public StructuralFeature getFeature();
// class operations
}
