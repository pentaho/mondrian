package org. omg. cwm. objectmodel. relationships;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Generalization extends ModelElement {


// class scalar attributes
// class references
public void setChild( Classifier input);


public Classifier getChild();
public void setParent( Classifier input);
public Classifier getParent();
// class operations
}


