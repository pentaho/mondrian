/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 2000
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * A <code>Formula</code> is a clause in an MDX query which defines a Set or a
 * Member.
 **/
public class Formula extends QueryPart {

   /** name of set or member **/
   String[] names;
   /** defining expression **/
   ExpBase exp;
   MemberProperty[] memberProperties; // properties/solve order of member
	/**
	 * <code>true</code> is this is a member, <code>false</code> if it is a
	 * set.
	 */
	boolean isMember;

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

	/**
	 * Resolves identifiers into objects.
	 * @param resolver The query which contains this formula.
	 */
	void resolve(Exp.Resolver resolver) {
		exp = (ExpBase) resolver.resolveChild(exp);
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
		for (int i = 0; i < memberProperties.length; i++) {
			resolver.resolveChild(memberProperties[i]);
		}
		// Get the format expression from the property list, or derive it from
		// the formula.
		Exp formatExp = getFormatExp();
		if (formatExp != null) {
			mdxMember.setProperty(Property.PROPERTY_FORMAT_EXP, formatExp);
		}
	}

	/**
	 * Creates the {@link Member} or {@link Set} object which this formula
	 * defines.
	 */
	void createElement(Query q) {
		// first resolve the name, bit by bit
		if (isMember) {
			OlapElement mdxElement = q.getCube();
			final SchemaReader schemaReader = q.getSchemaReader();
			for (int i = 0; i < names.length; i++) {
				OlapElement parent = mdxElement;
				mdxElement = schemaReader.getElementChild(parent, names[i]);
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
	}


  public Object[] getChildren() {
    Object[] children = new Object[1 + memberProperties.length];
    children[0] = exp;
    System.arraycopy(memberProperties, 0,
                     children, 1, memberProperties.length);
    return children;
  }


   public void replaceChild(int ordinal, QueryPart with)
   {
      Util.assertTrue(ordinal == 0);
      exp = (ExpBase) with;
   }

   public void unparse(PrintWriter pw)
   {
      if (isMember) {
         pw.print("member ");
         mdxMember.unparse(pw);
      } else {
         pw.print("set ");
         pw.print(Util.quoteMdxIdentifier(names));
      }
      pw.print(" as '");
      exp.unparse(pw);
      pw.print("'");
      if (memberProperties != null) {
         for (int i = 0; i < memberProperties.length; i++) {
            pw.print(", ");
            memberProperties[i].unparse(pw);
         }
      }
   }

	public boolean isMember() {
		return isMember;
	}

   /** Returns this formula's name. */
   public String getName() {
	   if (isMember) {
		   return mdxMember.getName();
	   } else {
		   return mdxSet.getName();
	   }
   }

   /** Returns this formula's caption. */
   public String getCaption() {
	   if (isMember) {
		   return mdxMember.getCaption();
	   } else {
		   return mdxSet.getName();
	   }
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

   /** Returns the unique name of the member or set. */
   String getUniqueName() {
	   if (isMember) {
		   return mdxMember.getUniqueName();
	   } else {
		   return mdxSet.getUniqueName();
	   }
   }

   OlapElement getElement()
   {
      if (isMember) {
         return mdxMember;
      } else {
         return mdxSet;
      }
   }

   /** Returns whether this formula represents hidden member (unique name
    * contains {@link Query#hidden} string). */
   public boolean isHidden()
   {
      return getElement().getUniqueName().indexOf(Query.hidden) >= 0;
   }

	public Exp getExpression() {
		return exp;
	}

	Exp getMemberProperty(String name) {
		return MemberProperty.get(memberProperties, name);
	}

   /**
    * Returns the Member. (Not valid if this formula defines a set.)
	*
	* @pre isMember()
	* @post return != null
    */
   public Member getMdxMember() {
     return mdxMember;
   }

	/**
	 * Deduces a formatting expression for this calculated member. First it
	 * looks for properties called "format", "format_string", etc. Then it looks
	 * inside the expression, and returns the formatting expression for the
	 * first member it finds.
	 */
	private Exp getFormatExp() {
		for (int i = 0; i < Property.FORMAT_PROPERTIES.length; i++) {
			Exp formatExp = getMemberProperty(Property.FORMAT_PROPERTIES[i]);
			if (formatExp != null) {
				return formatExp;
			}
		}
		Walker walker = new Walker(exp);
		while (walker.hasMoreElements()) {
			final Object o = walker.nextElement();
			if (o instanceof Member) {
				Exp formatExp = (Exp) ((Member) o).getPropertyValue(
						Property.PROPERTY_FORMAT_EXP);
				if (formatExp != null) {
					return formatExp;
				}
			}
		}
		return null;
	}
}

// End Formula.java
