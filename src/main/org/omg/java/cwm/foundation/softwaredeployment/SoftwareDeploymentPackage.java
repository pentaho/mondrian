/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface SoftwareDeploymentPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.foundation.businessinformation.BusinessInformationPackage getBusinessInformation();

  public org.omg.java.cwm.foundation.typemapping.TypeMappingPackage getTypeMapping();

  public org.omg.java.cwm.foundation.softwaredeployment.SiteClass getSite();

  public org.omg.java.cwm.foundation.softwaredeployment.MachineClass getMachine();

  public org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystemClass getSoftwareSystem();

  public org.omg.java.cwm.foundation.softwaredeployment.DeployedComponentClass getDeployedComponent();

  public org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystemClass getDeployedSoftwareSystem();

  public org.omg.java.cwm.foundation.softwaredeployment.DataManagerClass getDataManager();

  public org.omg.java.cwm.foundation.softwaredeployment.DataProviderClass getDataProvider();

  public org.omg.java.cwm.foundation.softwaredeployment.ProviderConnectionClass getProviderConnection();

  public org.omg.java.cwm.foundation.softwaredeployment.ComponentClass getComponent();

  public org.omg.java.cwm.foundation.softwaredeployment.PackageUsageClass getPackageUsage();

  public org.omg.java.cwm.foundation.softwaredeployment.SystemTypespace getSystemTypespace();

  public org.omg.java.cwm.foundation.softwaredeployment.ComponentDeployments getComponentDeployments();

  public org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystemComponents getDeployedSoftwareSystemComponents();

  public org.omg.java.cwm.foundation.softwaredeployment.DataManagerDataPackage getDataManagerDataPackage();

  public org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystemDeployments getSoftwareSystemDeployments();

  public org.omg.java.cwm.foundation.softwaredeployment.DataManagerConnections getDataManagerConnections();

  public org.omg.java.cwm.foundation.softwaredeployment.DataProviderConnections getDataProviderConnections();

  public org.omg.java.cwm.foundation.softwaredeployment.SiteMachines getSiteMachines();

  public org.omg.java.cwm.foundation.softwaredeployment.ComponentsOnMachine getComponentsOnMachine();

  public org.omg.java.cwm.foundation.softwaredeployment.RelatedSites getRelatedSites();

}
