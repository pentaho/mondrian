package javax. olap. query. enumerations;
import java. util.*;
public class RankingTypeEnum implements RankingType {
public static final RankingTypeEnum TOP = new RankingTypeEnum(" TOP"); public static final RankingTypeEnum BOTTOM = new
RankingTypeEnum(" BOTTOM"); public static final RankingTypeEnum TOPBOTTOM = new
RankingTypeEnum(" TOPBOTTOM"); private static final List typeName;
private final String literalName; static
{ java. util. ArrayList temp = new java. util. ArrayList();
temp. add(" TOP"); temp. add(" BOTTOM");
temp. add(" TOPBOTTOM"); typeName = java. util. Collections. unmodifiableList( temp);
}
private RankingTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof RankingTypeEnum) return (o == this); else if( o instanceof RankingType) return
(o. toString(). equals( literalName)); else return( false);
}
}