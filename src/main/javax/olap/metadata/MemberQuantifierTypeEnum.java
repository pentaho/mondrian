package javax.olap.metadata;
import java.util.*;
public class MemberQuantifierTypeEnum implements MemberQuantifierType
{
	public static final MemberQuantifierTypeEnum ANY = new MemberQuantifierTypeEnum("ANY");
	public static final MemberQuantifierTypeEnum EACH = new MemberQuantifierTypeEnum("EACH");
	public static final MemberQuantifierTypeEnum EVERY = new MemberQuantifierTypeEnum("EVERY");
	private static final List typeName;
	private final String literalName;
	static
	{
		java.util.ArrayList temp = new java.util.ArrayList();
		temp.add("ANY");
		temp.add("EACH");
		temp.add("EVERY");
		typeName = java.util.Collections.unmodifiableList(temp);
	}
	private MemberQuantifierTypeEnum(String literalName)
	{
		this.literalName = literalName;
	}
	public String toString()
	{
		return(literalName);
	}
	public List refTypeName()
	{
		return(typeName);
	}
	public int hashCode()
	{
		return(literalName.hashCode());
	}
	public boolean equals(Object o)
	{
		if(o instanceof MemberQuantifierTypeEnum) return (o == this);
		else if(o instanceof MemberQuantifierType) return
				(o.toString().equals(literalName));
		else return(false);
	}
}
