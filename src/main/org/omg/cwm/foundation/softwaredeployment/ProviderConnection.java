package org. omg. cwm. foundation. softwaredeployment;
import java. util. List; import java. util. Collection;
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*;
import org. omg. cwm. foundation. businessinformation.*;
public interface ProviderConnection extends ModelElement {


// class scalar attributes
public Boolean getIsReadOnly();
public void setIsReadOnly( Boolean input);
// class references
public void setDataProvider( DataProvider input);
public DataProvider getDataProvider();
public void setDataManager( DataManager input);
public DataManager getDataManager();
// class operations
}


