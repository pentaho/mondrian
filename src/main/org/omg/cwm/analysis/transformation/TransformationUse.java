package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface TransformationUse extends Dependency {


// class scalar attributes
public String getType();
public void setType( String input);
// class references
public void setTransformation( Collection input);
public Collection getTransformation();
public void addTransformation( ModelElement input);
public void removeTransformation( ModelElement input);
public void setOperation( Collection input);
public Collection getOperation();
public void addOperation( ModelElement input);
public void removeOperation( ModelElement input);
// class operations
}
