package org. omg. cwm. foundation. softwaredeployment;
import java. util. List; import java. util. Collection;
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*;
import org. omg. cwm. foundation. businessinformation.*;
public interface DeployedComponent extends org. omg. cwm. objectmodel. core. Package
{
// class scalar attributes
public String getPathname();


public void setPathname( String input);
// class references
public void setComponent( Component input);
public Component getComponent();
public void setMachine( Machine input);
public Machine getMachine();
// class operations
}


