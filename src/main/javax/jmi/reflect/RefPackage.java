package javax.jmi.reflect;

import java.util.Collection;
import java.util.List;

public interface RefPackage extends RefBaseObject {
    public RefClass refClass(RefObject type);
    public RefClass refClass(String typeName);
    public RefPackage refPackage(RefObject nestedPackage);
    public RefPackage refPackage(String nestedPackageName);
    public RefAssociation refAssociation(RefObject association);
    public RefAssociation refAssociation(String associationName);
    public Collection refAllPackages();
    public Collection refAllClasses();
    public Collection refAllAssociations();
    public RefStruct refCreateStruct(RefObject struct, List params);
    public RefStruct refCreateStruct(String structName, List params);
    public RefEnum refGetEnum(RefObject enum, String name);
    public RefEnum refGetEnum(String enumName, String name);
    public void refDelete();
}
