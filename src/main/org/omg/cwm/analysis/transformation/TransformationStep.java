package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface TransformationStep extends ModelElement {


// class scalar attributes
// class references
public void setTask( TransformationTask input);


public TransformationTask getTask();
public void setActivity( Namespace input);
public Namespace getActivity();
public void setPrecedence( Collection input);
public Collection getPrecedence();
public void addPrecedence( Constraint input);
public void removePrecedence( Constraint input);
public void setPrecedingStep( Collection input);
public Collection getPrecedingStep();
public void addPrecedingStep( Dependency input);
public void removePrecedingStep( Dependency input);
public void setSucceedingStep( Collection input);
public Collection getSucceedingStep();
public void addSucceedingStep( Dependency input);
public void removeSucceedingStep( Dependency input);
// class operations
}

