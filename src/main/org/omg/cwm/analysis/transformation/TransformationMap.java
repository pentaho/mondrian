package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface TransformationMap extends Transformation
{
// class scalar attributes
// class references
public void setClassifierMap( Collection input);


public Collection getClassifierMap();
public void removeClassifierMap( ModelElement input);
// class operations
}

