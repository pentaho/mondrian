package org. omg. cwm. foundation. softwaredeployment;
import java. util. List; import java. util. Collection;
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*;
import org. omg. cwm. foundation. businessinformation.*;
public interface DataManager extends DeployedComponent {


// class scalar attributes
public Boolean getIsCaseSensitive();
public void setIsCaseSensitive( Boolean input);
// class references
public void setDataPackage( Collection input);
public Collection getDataPackage();
public void addDataPackage( org. omg. cwm. objectmodel. core. Package input);


public void removeDataPackage( org. omg. cwm. objectmodel. core. Package input);
// class operations
}


