package javax.jmi.model;



public interface Reference extends StructuralFeature {
    public AssociationEnd getExposedEnd();
    public void setExposedEnd(AssociationEnd newValue);
    public AssociationEnd getReferencedEnd();
    public void setReferencedEnd(AssociationEnd newValue);
}
