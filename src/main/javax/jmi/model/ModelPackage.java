package javax.jmi.model;

import javax.jmi.reflect.*;

public interface ModelPackage extends RefPackage {
    public ModelElementClass getModelElement();
    public NamespaceClass getNamespace();
    public GeneralizableElementClass getGeneralizableElement();
    public TypedElementClass getTypedElement();
    public ClassifierClass getClassifier();
    public MofClassClass getMofClass();
    public MultiplicityType createMultiplicityType(int lower, int upper, boolean isOrdered, boolean isUnique);
    public DataTypeClass getDataType();
    public PrimitiveTypeClass getPrimitiveType();
    public EnumerationTypeClass getEnumerationType();
    public CollectionTypeClass getCollectionType();
    public StructureTypeClass getStructureType();
    public StructureFieldClass getStructureField();
    public AliasTypeClass getAliasType();
    public FeatureClass getFeature();
    public StructuralFeatureClass getStructuralFeature();
    public AttributeClass getAttribute();
    public ReferenceClass getReference();
    public BehavioralFeatureClass getBehavioralFeature();
    public OperationClass getOperation();
    public MofExceptionClass getMofException();
    public AssociationClass getAssociation();
    public AssociationEndClass getAssociationEnd();
    public MofPackageClass getMofPackage();
    public ImportClass getImport();
    public ParameterClass getParameter();
    public ConstraintClass getConstraint();
    public ConstantClass getConstant();
    public TagClass getTag();
    public AttachesTo getAttachesTo();
    public DependsOn getDependsOn();
    public Contains getContains();
    public Generalizes getGeneralizes();
    public Aliases getAliases();
    public Constrains getConstrains();
    public CanRaise getCanRaise();
    public Exposes getExposes();
    public RefersTo getRefersTo();
    public IsOfType getIsOfType();
}
