package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface TransformationTask extends Component {


// class scalar attributes
// class references
public void setTransformation( Collection input);


public Collection getTransformation();
public void addTransformation( Transformation input);
public void removeTransformation( Transformation input);
public void setInverseTask( Collection input);
public Collection getInverseTask();
public void addInverseTask( TransformationTask input);
public void removeInverseTask( TransformationTask input);
public void setOriginalTask( Collection input);
public Collection getOriginalTask();
public void addOriginalTask( TransformationTask input);
public void removeOriginalTask( TransformationTask input);
// class operations
}


