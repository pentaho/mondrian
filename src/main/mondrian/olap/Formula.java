/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 2000
*/

package mondrian.olap;
import java.io.*;

/**
 * Set or member specification.
 **/
public class Formula extends QueryPart {

   /** name of set or member **/
   String[] names;
   /** defining expression **/
   ExpBase exp;
   MemberProperty[] memberProperties; // properties/solve order of member
   boolean isMember;       // whether member or set

   Member mdxMember;
   Set mdxSet;

   /** Construct formula specifying a set. */
   Formula(String[] names, Exp exp) {
      this(false, names, (ExpBase) exp, new MemberProperty[0]);
   }

   /** Construct a formula specifying a member. */
   Formula(String[] names, Exp exp, MemberProperty[] memberProperties) {
      this(true, names, (ExpBase) exp, memberProperties);
   }

   private Formula(
      boolean isMember, String[] names, ExpBase exp,
      MemberProperty[] memberProperties)
   {
      this.isMember = isMember;
      this.names = names;
      this.exp = exp;
      this.memberProperties = memberProperties;
   }

   public Object clone()
   {
      return new Formula(
         isMember, names, (ExpBase) exp.clone(),
         MemberProperty.cloneArray(memberProperties));
   }

   static Formula[] cloneArray(Formula[] x)
   {
      Formula[] x2 = new Formula[x.length];
      for (int i = 0; i < x.length; i++)
         x2[i] = (Formula) x[i].clone();
      return x2;
   }

   public QueryPart resolve(Query q)
   {
      exp = (ExpBase) exp.resolve(q);
      String id = Util.quoteMdxIdentifier(names);
      if (isMember) {
         if (!(!exp.isSet() ||
              (exp instanceof FunCall && ((FunCall) exp).isCallToTuple()))) {
            throw Util.getRes().newMdxMemberExpIsSet(id);
         }
      } else {
         if (!exp.isSet()) {
            throw Util.getRes().newMdxSetExpNotSet(id);
         }
      }
      return this;
   }

   public QueryPart createElement(Query q)
   {
      // first resolve the name, bit by bit
      if (isMember) {
         OlapElement mdxElement = q.getCube();
         for (int i = 0; i < names.length; i++) {
            OlapElement parent = mdxElement;
            mdxElement = q.lookupChild(parent, names[i], false);
            if (mdxElement == null) {
               // this part of the name was not found... define it
               Level level;
               Member parentMember = null;
               if (parent instanceof Member) {
                  parentMember = (Member) parent;
                  level = parentMember.getLevel().getChildLevel();
               } else {
                  Hierarchy hierarchy = parent.getHierarchy();
                  if (hierarchy == null) {
                     throw Util.getRes().newMdxCalculatedHierarchyError(
                        Util.quoteMdxIdentifier(names));
                  }
                  level = hierarchy.getLevels()[0];
               }
               Member mdxMember = level.getHierarchy().createMember(
                  parentMember, level, names[i], this);
               mdxElement = mdxMember;
            }
         }
         this.mdxMember = (Member) mdxElement;
      } else {
         // don't need to tell query... it's already in query.formula
         Util.assertTrue(
            names.length == 1, "set names must not be compound");
         mdxSet = new SetBase(names[0], exp);
      }
      return this;
   }

   public Object[] getChildren()
   {
      return new Object[] {exp};
   }

   public void replaceChild(int ordinal, QueryPart with)
   {
      Util.assertTrue(ordinal == 0);
      exp = (ExpBase) with;
   }

   public void unparse(PrintWriter pw, ElementCallback callback)
   {
      if (isMember) {
         pw.print("member ");
         mdxMember.unparse(pw, callback);
      } else {
         pw.print("set ");
         pw.print(Util.quoteMdxIdentifier(names));
      }
      pw.print(" as '");
      exp.unparse(pw, callback);
      pw.print("'");
      if (memberProperties != null) {
         for (int i = 0; i < memberProperties.length; i++) {
            pw.print(", ");
            memberProperties[i].unparse(pw, callback);
         }
      }
   }

   /** returns type of formulae ("member" or "set") as a string*/
   public String getTypeAsString()
   {
      if (isMember)
         return "member";
      return "set";
   }

   public boolean isMember() {return isMember;}

   /** returns name of formula*/
   public String getName()
   {
      if (isMember)
         return mdxMember.getName();
      return mdxSet.getName();
   }

   public String[] getNames()
   {
      return names;
   }

   /** returns caption of formula*/
   public String getCaption()
   {
      if (isMember)
         return mdxMember.getCaption();
      return mdxSet.getName();
   }

   /**
    * Changes the last part of the name to <code>newName</code>. For example,
    * <code>[Abc].[Def].[Ghi]</code> becomes <code>[Abc].[Def].[Xyz]</code>;
    * and the member or set is renamed from <code>Ghi</code> to
    * <code>Xyz</code>.
    **/
   void rename(String newName)
   {
      String oldName = getElement().getName();
      Util.assertTrue(
         this.names[this.names.length - 1].equalsIgnoreCase(oldName));
      this.names[this.names.length - 1] = newName;
      if (isMember) {
         mdxMember.setName(newName);
      } else {
         mdxSet.setName(newName);
      }
   }

   /** returns uniqueName of formula*/
   public String getUniqueName()
   {
      if(isMember)
         return mdxMember.getUniqueName();
      return mdxSet.getUniqueName();
   }

   /** returns name of associated hierachy if there is one*/
   public String getHierarchyName()
   {
      if (!isMember)
         return "";
      return mdxMember.getHierarchy().getUniqueName();
   }

   /** returns depth of a member. If it's a set, returns -1 */
   public int getDepth()
   {
      if (!isMember)
         return -1;
      return mdxMember.getDepth();
   }

   /** prints member properties as xml*/
   public void printMemberPropertiesAsXml(PrintWriter pw)
   {
      if (!isMember)
         return;
      if (memberProperties != null) {
         for (int i = 0; i < memberProperties.length; i++)
            memberProperties[i].printAsXml(pw);
      }
   }

   //** returns formula's expresion as string*/
   public String expToString()
   {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      exp.unparse(pw, new ElementCallback());
      return sw.toString();
   }

   OlapElement getElement()
   {
      if (isMember) {
         return mdxMember;
      } else {
         return mdxSet;
      }
   }

    /*returns parent's unique name (member's name) or "", if parent does not
      exist*/
   public String getParentUname()
   {
      if (isMember && mdxMember != null)
         return mdxMember.getParentUniqueName();
      return "";
   }

   /** Returns whether this formula represents hidden member (unique name
    * contains {@link Query#hidden} string). */
   public boolean isHidden()
   {
      return getElement().getUniqueName().indexOf(Query.hidden) >= 0;
   }

   /** returns expression of this formula */
   public String getExpString()
   {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      exp.unparse(pw, new ElementCallback());
      return sw.toString();
   }

   public Exp getExpression()
   {
      return exp;
   }

   public Exp getMemberProperty(String name)
   {
      return MemberProperty.get(memberProperties, name);
   }

   /**
    * get the mdx member
    */
   public Member getMdxMember() {
     return mdxMember;
   }

}


// End Formula.java
