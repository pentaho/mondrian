package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface StepPrecedence extends Dependency {
// class scalar attributes
// class references
public void setPrecedingStep( Collection input);


public Collection getPrecedingStep();
public void addPrecedingStep( ModelElement input);
public void removePrecedingStep( ModelElement input);
public void setSucceedingStep( Collection input);
public Collection getSucceedingStep();
public void addSucceedingStep( ModelElement input);
public void removeSucceedingStep( ModelElement input);
// class operations
}


