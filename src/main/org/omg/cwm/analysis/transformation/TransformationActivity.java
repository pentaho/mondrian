package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface TransformationActivity extends Subsystem {


// class scalar attributes
public String getCreationDate();
public void setCreationDate( String input);
// class references
public void setStep( Collection input);
public Collection getStep();
public void removeStep( ModelElement input);
// class operations
}


