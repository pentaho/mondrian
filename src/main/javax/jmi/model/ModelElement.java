package javax.jmi.model;

import javax.jmi.reflect.*;

public interface ModelElement extends RefObject {
    public String CONTAINERDEP = "container";
    public String CONTENTSDEP = "contents";
    public String SIGNATUREDEP = "signature";
    public String CONSTRAINTDEP = "constraint";
    public String CONSTRAINEDELEMENTSDEP = "constrained elements";
    public String SPECIALIZATIONDEP = "specialization";
    public String IMPORTDEP = "import";
    public String TYPEDEFINITIONDEP = "type definition";
    public String REFERENCEDENDSDEP = "referenced ends";
    public String TAGGEDELEMENTSDEP = "tagged elements";
    public String INDIRECTDEP = "indirect";
    public String ALLDEP = "all";
    public java.util.Collection findRequiredElements(java.util.Collection kinds, boolean recursive);
    public boolean isRequiredBecause(ModelElement otherElement, String reason[]);
    public boolean isFrozen();
    public boolean isVisible(ModelElement otherElement);
    public String getName();
    public void setName(String newValue);
    public java.util.List getQualifiedName();
    public String getAnnotation();
    public void setAnnotation(String newValue);
    public java.util.Collection getRequiredElements();
    public Namespace getContainer();
    public void setContainer(Namespace newValue);
    public java.util.Collection getConstraints();
}
