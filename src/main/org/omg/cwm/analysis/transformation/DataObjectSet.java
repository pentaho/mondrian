package org. omg. cwm. analysis. transformation;
import java. util. List;
import java. util. Collection; import org. omg. cwm. objectmodel. core.*;
import org. omg. cwm. foundation. expressions.*; import org. omg. cwm. foundation. softwaredeployment.*;


public interface DataObjectSet extends ModelElement {
// class scalar attributes
// class references
public void setElement( Collection input);


public Collection getElement();
public void addElement( ModelElement input);
public void removeElement( ModelElement input);
public void setSourceTransformation( Collection input);
public Collection getSourceTransformation();
public void addSourceTransformation( Transformation input);
public void removeSourceTransformation( Transformation input);
public void setTargetTransformation( Collection input);
public Collection getTargetTransformation();
public void addTargetTransformation( Transformation input);
public void removeTargetTransformation( Transformation input);
// class operations
}


