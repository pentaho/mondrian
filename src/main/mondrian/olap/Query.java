/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * MDX query.
 **/
public class Query extends QueryPart implements NameResolver {
	// enum axisType
	public static final int noAxis = -2;
	public static final int slicerAxis = -1;
	public static final int xAxis = 0;
	public static final int yAxis = 1;
	public static final EnumeratedValues axisEnum = new EnumeratedValues(
		new String[] {"none", "slicer", "x", "y"},
		new int[] {-2, -1, 0, 1});

	// enum sortDirection
  public static final int ascDirection = 0; // ascending inside hierarchy
  public static final int descDirection = 1; // descending inside hierarchy
  public static final int bascDirection = 2; // ascending disregarding hierarchy
  public static final int bdescDirection = 3; // descending disregarding hierarchy
  public static final int noneDirection = -1;
  public static final EnumeratedValues directionEnum = new EnumeratedValues(
		new String[] {"none", "ascending", "descending", 
                           "nonhierarchized ascending", "nonhierarchized descending"},
    new int[] {-1, 0, 1, 2, 3} );

	//hidden string
	public static final String hidden = "hidden_";

	public Formula formulas[];
	public QueryAxis axes[];
	public Cube mdxCube;
	public Exp slicer;
	public QueryPart cellProps[];
	private Parameter parameters[]; // stores definitions of parameters
	private Connection connection;

	public Query()
		{}

	/** Constructs a Query. */
	public Query(
		Connection connection, Formula[] formulas, QueryAxis[] axes,
		String cube, Exp slicer, QueryPart[] cellProps)
	{
		this(
				connection,
				connection.getSchema().lookupCube(cube, true),
				formulas, axes, slicer, cellProps, new Parameter[0]);
	}

	/** Construct a Query; called from clone(). */
	public Query(
			Connection connection, Cube mdxCube,
			Formula[] formulas, QueryAxis[] axes, Exp slicer,
			QueryPart[] cellProps, Parameter[] parameters) {
		this.connection = connection;
		this.mdxCube = mdxCube;
		this.formulas = formulas;
		this.axes = axes;
		normalizeAxes();
		setSlicer(slicer);
		this.cellProps = cellProps;
		this.parameters = parameters;
		resolve(this); // resolve self and children
		resolveParameters();  //calculate parameter's usage in query
	}

	public Object clone() throws CloneNotSupportedException
	{
		return new Query(
				connection,  mdxCube,
				Formula.cloneArray(formulas), QueryAxis.cloneArray(axes),
				slicer == null ? null : (Exp) slicer.clone(), null,
				Parameter.cloneArray(parameters));
	}

	public Query safeClone()
	{
		try {
			return (Query) clone();
		} catch (CloneNotSupportedException e) {
			throw Util.getRes().newInternal("Query.clone() failed", e);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public static final String[] axisNames = {
		"COLUMNS", "ROWS", "PAGES", "CHAPTERS", "SECTIONS"
	};

	private void normalizeAxes()
	{
		for (int i = 0; i < axes.length; i++) {
			String correctName = axisNames[i];
			if (!axes[i].axisName.equalsIgnoreCase(correctName)) {
				boolean found = false;
				for (int j = i + 1; j < axes.length; j++) {
					if (axes[j].axisName.equalsIgnoreCase(correctName)) {
						// swap axes
						QueryAxis temp = axes[i];
						axes[i] = axes[j];
						axes[j] = temp;
						found = true;
						break;
					}
				}
			}
		}
	}

	public QueryPart resolve(Query q)
	{
		if (formulas != null) {
			//resolving of formulas should be done in two parts
			//because formulas might depend on each other, so all calculated
			//mdx elements have to be defined during resolve
			for (int i = 0; i < formulas.length; i++)
				formulas[i] = (Formula) formulas[i].createElement(q);
			for (int i = 0; i < formulas.length; i++)
				formulas[i] = (Formula) formulas[i].resolve(q);
		}

		if (axes != null)
			for (int i = 0; i < axes.length; i++)
				axes[i] = (QueryAxis) axes[i].resolve(q);

		if (slicer != null) {
			setSlicer((Exp) slicer.resolve(q));
		}

		// Now that out Parameters have been created (from FunCall's to
		// Parameter() and ParamRef()), resolve them.
		for (int i = 0; i < parameters.length; i++)
			parameters[i] = (Parameter) parameters[i].resolve(q);

		return this;
	}

	public void unparse(PrintWriter pw, ElementCallback callback)
	{
		callback.disableHiddenNameLookup(true);
		if (formulas != null) {
			for (int i = 0; i < formulas.length; i++) {
				if (i == 0) {
					pw.print("with ");
				} else {
					pw.print("  ");
				}
				formulas[i].unparse(pw, callback);
				pw.println();
			}
		}
		callback.disableHiddenNameLookup(false);
		pw.print("select ");
		if (axes != null) {
			for (int i = 0; i < axes.length; i++) {
				axes[i].axisOrdinal = i;
				axes[i].unparse(pw, callback);
				if (i < axes.length - 1) {
					pw.println(", ");
					pw.print("  ");
				} else {
					pw.println();
				}
			}
		}
		if (mdxCube != null) {
			String cubeName = null;
			cubeName = callback.registerItself(mdxCube);
			if (cubeName == null) {
				cubeName = mdxCube.getName();
			}
			pw.println("from [" + cubeName + "]");
		}
		if (slicer != null) {
			pw.print("where ");
			slicer.unparse(pw, callback);
			pw.println();
		}
	}

	/** This class tells {@link #unparse} to expand parameters, because the
	 * query is intended for Plato and substitute hidden members with existing
	 * ones */
	class PlatoCallBack extends ElementCallback {
        /** Maps between existing and hidden members **/
		HashMap hiddenNames = new HashMap();
		boolean disableLookup = false;

		public PlatoCallBack() {
		}

		/**creates PlatoCallBack object and initializes hiddenMembers mapping,
		 * using names of formulas. Later, this.hiddenMembers will be used to
		 * find hidden names for existing ones. We use this substitution to
		 * allow formatting of existing members. If do not need to use this
		 * feature call other constructor*/
		public PlatoCallBack(Formula formulas[])
		{
			if (formulas == null) {
				return;
            }
			for (int i = 0; i < formulas.length; i++) {
				if (!formulas[i].isHidden())
					continue;
				String hiddenName = formulas[i].getUniqueName();
				int offset = hiddenName.indexOf(Query.hidden);
				String name = hiddenName.substring(0, offset) +
					hiddenName.substring(offset + Query.hidden.length());
				hiddenNames.put(name, hiddenName);
			}
		}

		public boolean isPlatoMdx()
		{return true;}

		/** returns hiddenName for given uName if it exists. This feature is
		 * used for formatting existing measures*/
		public String findHiddenName(String uName)
		{
			if (disableLookup) {
                return null;
            }
            return (String) hiddenNames.get(uName);
		}

        /** disables or enables hidden name lookup*/
		public void disableHiddenNameLookup(boolean disableLookup) {
            this.disableLookup = disableLookup;
        }
	}

	public String toPlatoMdx()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		ElementCallback callback = new PlatoCallBack(formulas);
		unparse(pw, callback);
		return sw.toString();
	}

	public String toWebUIMdx()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		ElementCallback callback = new ElementCallback();
		unparse(pw, callback);
		resetParametersPrintProperty();
		return sw.toString();
	}

	/**
	 * Returns the axis which the result axis is based on, taking into account
	 * any axis re-ordering.
	 *
	 * <p>Suppose that they've written
	 * <pre>select {} on rows, {} on pages from Sales</pre>
	 *
	 * <p>Then we will execute
	 * <pre>select {} on columns, {} on rows from Sales</pre>
	 *
	 * getLogicalAxis(0) = 1, meaning that axis 0 of the Plato cellset matches
	 * the rows (1) axis of their query; likewise, getLogicalAxis(1) = 2.
	 *
	 * @param ordinal of axis in cellset
	 * @return axis label in original query (0 = columns, 1 = rows, etc.)
	 */
	public int getLogicalAxis(int iPhysicalAxis)
	{
		if (iPhysicalAxis == slicerAxis || iPhysicalAxis == axes.length) {
			return slicerAxis; // slicer is never permuted
		}
		String axisName = axes[iPhysicalAxis].axisName;
		for (int i = 0; i < axisNames.length; i++) {
			if (axisName.equalsIgnoreCase(axisNames[i])) {
				return i;
			}
		}
		return noAxis;
	}

	/** The inverse of {@link #getLogicalAxis}. */
	public int getPhysicalAxis(int iLogicalAxis)
	{
		if (iLogicalAxis < 0) {
			return iLogicalAxis;
		}
		String axisName = axisNames[iLogicalAxis];
		for (int i = 0; i < axes.length; i++) {
			if (axes[i].axisName.equalsIgnoreCase(axisName)) {
				return i;
			}
		}
		return noAxis;
	}

	/** Convert an axis name, such as "x" or "ROWS" into an axis code. */
	public static int getAxisCode(String axisName)
	{
		if (axisName.equalsIgnoreCase("slicer")) {
			return slicerAxis;
		} else if (axisName.equals("none")) {
			return noAxis;
		} else if (axisName.equals("x")) {
			axisName = "COLUMNS";
		} else if (axisName.equals("y")) {
			axisName = "ROWS";
		}
		for (int i = 0; i < axisNames.length; i++) {
			if (axisNames[i].equalsIgnoreCase(axisName)) {
				return i;
			}
		}
		return noAxis;
	}

	/** Inverse of {@link #getAxisCode} */
	public static String getAxisName(int iAxis)
	{
		switch (iAxis) {
		case noAxis:
			return "NONE";
		case slicerAxis:
			return "SLICER";
		default:
			return axisNames[iAxis];
		}
	}

	/** constructs hidden unique name based on given uName. It is used for
	 * formatting existing measures */
	public static String getHiddenMemberUniqueName(String uName)
	{
		int i = uName.lastIndexOf("].[");
		return uName.substring(0, i + 3) + Query.hidden + uName.substring(i+3);
	}

	/** checks for hidden string in name and strips it out. It looks only for
	 * first occurence */
	public static String stripHiddenName(String name)
	{
		if (name.indexOf(Query.hidden) != -1)
			return name.substring(0, name.indexOf(Query.hidden)) +
				name.substring(name.indexOf(Query.hidden) +
							   Query.hidden.length());
		return name;
	}

	public static String getHiddenMemberFormulaDefinition(String uName)
	{return uName;}

	/** @return query string as it was send from webUI or workBench
	 */
	public String toString()
	{ return toWebUIMdx();}

	public Object[] getChildren()
	{
		// Chidren are axes, slicer, and formulas (in that order, to be
		// consistent with replaceChild).
		ArrayList list = new ArrayList();
		for (int i = 0; i < axes.length; i++) {
			list.add(axes[i]);
		}
		if (slicer != null) {
			list.add(slicer);
		}
		for (int i = 0; i < formulas.length; i++) {
			list.add(formulas[i]);
		}
		return list.toArray();
	}

	public void replaceChild(int i, QueryPart with)
	{
		int i0 = i;
		if( i < axes.length ){
			if (with == null) {
				// We need to remove the axis.  Copy the array, omitting
				// element i.
				QueryAxis[] oldAxes = axes;
				axes = new QueryAxis[oldAxes.length - 1];
				for (int j = 0; j < axes.length; j++) {
					axes[j] = oldAxes[j < i ? j : j + 1];
				}
			} else {
				axes[i] = (QueryAxis) with;
			}
			return;
		}

		i -= axes.length;
		if (i == 0) {
			setSlicer((Exp) with); // replace slicer
			return;
		}

		i -= 1;
		if (i < formulas.length) {
			if (with == null) {
				// We need to remove the formula.  Copy the array, omitting
				// element i.
				Formula[] oldFormulas = formulas;
				formulas = new Formula[oldFormulas.length - 1];
				for (int j = 0; j < formulas.length; j++) {
					formulas[j] = oldFormulas[j < i ? j : j + 1];
				}
			} else {
				formulas[i] = (Formula) with;
			}
			return;
		}
		throw Util.getRes().newInternal(
			"Query child ordinal " + i0 + " out of range (there are " +
			axes.length + " axes, " + formulas.length + " formula)");
	}

	/** Normalize slicer into a tuple of members; for example, '[Time]' becomes
	 * '([Time].DefaultMember)'.  todo: Make slicer an Axis, not an Exp, and
	 * put this code inside Axis.  */
	private void setSlicer(Exp exp)
	{
		slicer = exp;
		if (slicer instanceof Level ||
			slicer instanceof Hierarchy ||
			slicer instanceof Dimension) {
			slicer = new FunCall(
				"DefaultMember", new Exp[] {slicer}, FunDef.TypeProperty);
		}
		if (slicer == null) {
			;
		} else if (slicer instanceof FunCall &&
				   ((FunCall) slicer).isCallToTuple()) {
			;
		} else {
			slicer = new FunCall(
				"()", new Exp[] {slicer}, FunDef.TypeParentheses);
		}
	}

	private boolean usesDimension(Dimension dimension)
	{
		for (int iAxis = 0; iAxis < axes.length; iAxis++) {
			if (axes[iAxis].set.usesDimension(dimension)) {
				return true;
			}
		}
		if (slicer != null && slicer.usesDimension(dimension)) {
			return true;
		}
		return false;
	}

	/** Returns an enumeration, each item of which is an Ob containing a
	 * dimension which does not appear in any Axis or in the slicer. */
	public Iterator unusedDimensions() {
		Dimension[] mdxDimensions = mdxCube.getDimensions();
        return Arrays.asList(mdxDimensions).iterator();
	}

	public void addLevelToAxis(int iAxis, Level level)
	{
		Util.assertTrue(iAxis < axes.length, "axis ordinal out of range");
		QueryAxis axis = axes[iAxis];
		axis.addLevel(level);
	}

	/**
	 * Walk over the tree looking for an expression of a particular hierarchy
	 * If there is one, return the walker pointing at it (from which we can get
	 * its parent); otherwise, return null.
	 **/
	private Walker findHierarchy(Hierarchy hierarchy)
	{
		Walker walker = new Walker(this);
		while (walker.hasMoreElements()) {
			Object o = walker.nextElement();
			if (o instanceof Formula) {
				walker.prune();
				continue; // ignore expressions in formula
			} else if (o instanceof Exp) {
				Exp e = (Exp) o;
				// expression must represent a set or be mdx element
				if (!e.isSet() && !e.isElement())
					continue;

				// if object's parent is a function (except a tuple/parentheses
				// or CrossJoin), algorithm shall look only at the first child
				Object parent =  walker.getParent();
				if (parent instanceof FunCall) {
					FunCall funCall = (FunCall) parent;
					if (!funCall.isCallToTuple() &&
						!funCall.isCallToCrossJoin() &&
						funCall.args[0] != o) {
						walker.prune();
						continue;
					}
				}
				Hierarchy obExpHierarchy = e.getHierarchy();
				if (obExpHierarchy == null)
					// set must have a dimension (e.g. disallow CrossJoin)
					continue;

				if (obExpHierarchy.equals(hierarchy))
					return walker; // success!
				else if( e instanceof FunCall && ((FunCall) e).isCallToFilter()){
					// tell walker not to look at any more children of Filter
					walker.prune();
				}
			}
		}
		return null; // no expression of that dimension found
	}

	/**
	 * Returns the hierarchies in an expression
	 * @see #findHierarchy
	 */
	private Hierarchy[] collectHierarchies(QueryPart queryPart)
	{
		Walker walker = new Walker(queryPart);
		HashSet set = new HashSet();
		while (walker.hasMoreElements()) {
			Object o = walker.nextElement();
			if (o instanceof Exp) {
				Exp e = (Exp) o;
				if (!e.isSet() && !e.isMember())
					continue; // expression must represent a set or be a member

				// if object's parent is a function (except tuple/parentheses
				// or CrossJoin), algorithm shall look only at the first child
				Object parent =  walker.getParent();
				if (parent instanceof FunCall) {
					FunCall funCall = (FunCall) parent;
					if (!funCall.isCallToTuple() &&
						!funCall.isCallToCrossJoin() &&
						funCall.args[0] != o) {
						walker.prune();
						continue;
					}
				}

				Hierarchy obExpHierarchy = e.getHierarchy();
				if (obExpHierarchy == null)
					// set must have a dimension (e.g. disallow CrossJoin)
					continue;

				set.add(obExpHierarchy);
			}
		}

		return (Hierarchy[]) set.toArray(new Hierarchy[0]);
	}

	/** Place expression 'exp' at position 'iPositionOnAxis' on axis 'axis'. */
	private void putInAxisPosition(Exp exp, int axis, int iPositionOnAxis)
	{
		switch (axis) {
		case slicerAxis:
			// slicer shall contain at most one tuple
			if (slicer == null) {
				setSlicer(exp);
			} else {
				slicer.addAtPosition(exp, iPositionOnAxis);
			}
			break;

		default:
			Util.assertTrue(axis >= 0);
			if (axis >= axes.length) {
				Util.assertTrue(axis == axes.length);
				QueryAxis[] oldAxes = axes;
				axes = new QueryAxis[oldAxes.length + 1];
				for (int i = 0; i < oldAxes.length; i++) {
					axes[i] = oldAxes[i];
				}
				axes[oldAxes.length] = new QueryAxis(
					false, null, this.getAxisName(axis),
					QueryAxis.subtotalsUndefined);
			}

			Exp axisExp = axes[axis].set;
			if (axisExp == null || axisExp.isEmptySet()) {
				// Axis is empty, so just put expression there.
				axes[axis].set = exp;
			} else {
				if (iPositionOnAxis == 0) {
					// 'exp' has to go first:
					//   axisExp
					// becomes
					//   CrossJoin(exp, axisExp)
					FunCall funCrossJoin = new FunCall(
						"CrossJoin", new Exp[] {exp, axisExp});
					axes[axis].set = funCrossJoin;
				} else if (iPositionOnAxis < 0) {
					// 'exp' has to go last:
					//   axisExp
					// becomes
					//   CrossJoin(axisExp, exp)
					FunCall funCrossJoin = new FunCall(
						"CrossJoin", new Exp[] {axisExp, exp});
					axes[axis].set = funCrossJoin;
				} else {
					int i = axes[axis].set.addAtPosition(exp, iPositionOnAxis);
					if (i != -1) {
						// The expression was not added, because the position
						// equalled or exceded the number of hierarchies. Add
						// it on the end.
						FunCall funCrossJoin = new FunCall(
							"CrossJoin", new Exp[] {axisExp, exp});
						axes[axis].set = funCrossJoin;
					}
				}
			}
			break;
		}
	}

	/**
	 * Toggle the drill state of each member of "dimension".
	 */
	public void drillDown(Level level)
	{
		Walker walker = findHierarchy(level.getHierarchy());
		Util.assertTrue(
			walker != null,
			"could not find expression of dimension " +
			level.getDimension());

		Exp e = (Exp) walker.currentElement();
		FunCall funDrillDownLevel = new FunCall(
			"DrillDownLevel", new Exp[] {e});

		QueryPart parent = (QueryPart) walker.getParent();
		parent.replaceChild(walker.getOrdinal(), funDrillDownLevel);
	}

	/**
	 * Restrict the axis which contains "level" to only return members
	 * between "startMember" and "endMember", inclusive.
	 */
	public void crop(
		Level level, Member startMember, Member endMember)
	{
		// Form the cropping expression.  If we have a range, include all
		// descendents of the ends of the range, because ':' only includes
		// members at the same level.
		Hierarchy hierarchy = level.getHierarchy();
		Exp expCrop =
			startMember.equals(endMember)
			?
			// e.g. {[Beverages]}
			new FunCall("{}", new Exp[] {startMember}, FunDef.TypeBraces)
			:
			// e.g.
			// Generate([Beverages]:[Breakfast Foods],
			//          Descendants([Products].CurrentMember,
			//                      [Products].[(All)],
			//                      SELF_BEFORE_AFTER))
			new FunCall(
				"Generate",
				new Exp[] {
					new FunCall(
						":",
						new Exp[] {startMember, endMember},
						FunDef.TypeInfix),
					new FunCall(
						"Descendants",
						new Exp[] {
							new FunCall(
								"CurrentMember",
								new Exp[] {hierarchy},
								FunDef.TypeProperty),
							hierarchy.lookupLevel("(All)"),
							Literal.createSymbol("SELF_BEFORE_AFTER")
						})
				});
		crop(level, expCrop);
	}

	/**
	 *
	 * The technique is to find the expression which generates the set of
	 * members for that dimension, then intersect it with the cropping set.
	 *
	 * For example,
	 *
	 * select
	 * 	  {[Measures].[Unit Sales], [Measures].[Sales Count]} on columns,
	 * 	  CROSSJOIN(
	 * 		  [Product].[Product Department].MEMBERS,
	 * 		  [Gender].[Gender].MEMBERS) on rows
	 * from Sales
	 *
	 * when cropped with {[Beverages], [Breakfast Foods]} becomes
	 *
	 * select
	 * 	  {[Measures].[Unit Sales], [Measures].[Sales Count]} on columns,
	 * 	  CROSSJOIN(
	 * 		  INTERSECT(
	 * 			  [Product].[Product Department].MEMBERS,
	 * 			  {[Beverages], [Breakfast Foods]}),
	 * 		  [Gender].[Gender].MEMBERS) on rows
	 * from Sales
	 */
	private void crop(Level level, Exp expCrop)
	{
		boolean found = false;
		Walker walker = new Walker(this);
		while (walker.hasMoreElements()) {
			Object o = walker.nextElement();
			if (o instanceof Exp) {
				Exp e = (Exp) o;
				if (!e.isSet())
					continue;	// expression must represent a set

				Dimension dim = e.getDimension();
				if (dim == null)
					// set must have a dimension (e.g. disallow Crossjoin)
					continue;

				if (!dim.equals(level.getDimension()))
					continue; // set must be of right dimension

				FunCall funIntersect = new FunCall(
					"Intersect", new Exp[] {e, expCrop});

				QueryPart parent = (QueryPart) walker.getParent();
				parent.replaceChild(walker.getOrdinal(), funIntersect);
				found = true;
				break;
			}
		}
		Util.assertTrue(
			found,
			"could not find expression of dimension " +
			level.getDimension());
	}

	/** SetParameter.  Assign 'value' to parameter 'name'*/
	public void setParameter( String sParameter, String value )
	{
		Parameter param = lookupParam( sParameter );
		if (param == null)
		{
			throw Util.getRes().newMdxParamNotFound(sParameter);
		}
		param.setValue(value, this);
	}

	/**
	 * Moves <code>hierarchy</code> from <code>fromAxis</code>, to
	 * <code>toAxis</code> at <code>position</code> (-1 means last position).
	 * The hierarchy is added if <code>fromAxis</code> is {@link #noAxis}, and
	 * removed if <code>toAxis</code> is {@link #noAxis}.
	 *
	 * <p>If the target axis is the slicer, selects the [All] member;
	 * otherwise, if the hierarchy is already on an axis, keep the same
	 * drill-state; otherwise, select the first level (children), if expand =
	 * true, else put default member.</p>
	 **/
	public void moveHierarchy(
		Hierarchy hierarchy, int fromAxis, int toAxis, int iPositionOnAxis,
		boolean bExpand)
	{
		Exp e;

		// Find the hierarchy in its current position.
		Walker walker = findHierarchy(hierarchy.getHierarchy());
		if (fromAxis == noAxis) {
			if (walker != null) {
				throw Util.getRes().newMdxHierarchyUsed(hierarchy.getUniqueName());
			}
			e = null;
		} else {
			if (walker == null) {
				throw Util.getRes().newMdxHierarchyNotUsed(hierarchy.getUniqueName());
			}

			// Remove from current position.
			e = (Exp) walker.currentElement();
			QueryPart parent = (QueryPart) walker.getParent();
			Util.assertTrue(parent != null, "hierarchy must have parent");
			if (parent instanceof QueryAxis) {
				// Axis only contains this hierarchy; remove it.
				Util.assertTrue(walker.getAncestor(2) == this);
				int iAxis = walker.getAncestorOrdinal(1);
				replaceChild(iAxis, null);
				if (toAxis > iAxis) {
					--toAxis;
				}
			} else if (parent instanceof Query && fromAxis == slicerAxis) {
				// Hierachy sits on the slicer and it's the only hierachy on
				// the slicer (otherwise the parent would be _Tuple with at
				// least 2 children) and it is being removed - Simply delete
				// the slicer
				slicer = null;
			} else if (parent instanceof FunCall &&
					   ((FunCall) parent).isCallToCrossJoin()) {
				// Function must be CrossJoin.  If 'e' is our expression, then
				//   f(..., CrossJoin(e, other), ...)
				// becomes
				//   f(..., other, ...).
				int iOrdinal = walker.getOrdinal();
				int iOtherOrdinal = 1 - iOrdinal;
				Exp otherExp = ((FunCall) parent).args[iOtherOrdinal];
				QueryPart grandparent = (QueryPart) walker.getAncestor(2);
				int iParentOrdinal = walker.getAncestorOrdinal(1);
				grandparent.replaceChild(iParentOrdinal, (QueryPart) otherExp);
			} else if (parent instanceof FunCall &&
					   ((FunCall)parent).isCallToTuple() &&
					   fromAxis == slicerAxis ){
				int iOrdinal = walker.getOrdinal();
				((FunCall)slicer).removeChild( iOrdinal );
			} else if (parent instanceof Parameter) {
				// The hierarchy is a child of parameter, so we need to remove
				// the parameter itself.
				QueryPart grandparent = (QueryPart) walker.getAncestor(2);
				int iParentOrdinal = walker.getAncestorOrdinal(1);
				if( grandparent instanceof FunCall &&
					   ((FunCall)grandparent).isCallToTuple() &&
					   fromAxis == slicerAxis ){
					((FunCall)slicer).removeChild( iParentOrdinal );
					if( ((FunCall)slicer).args.length == 0 ){
						// the slicer is empty now
						slicer = null;
					}
				} else if (grandparent instanceof FunCall &&
					   ((FunCall) grandparent).isCallToCrossJoin()) {
					// Function must be CrossJoin.  If 'e' is our expression,
					// then
					//   f(..., CrossJoin(e, other), ...)
					// becomes
					//   f(..., other, ...).
					int iOtherOrdinal = 1 - iParentOrdinal;
					Exp otherExp = ((FunCall) grandparent).args[iOtherOrdinal];
					QueryPart grandGrandparent = (QueryPart)
						walker.getAncestor(3);
					int iGrandParentOrdinal = walker.getAncestorOrdinal(2);
					grandGrandparent.replaceChild(
						iGrandParentOrdinal, (QueryPart) otherExp);
				}
			} else {
				throw Util.getRes().newInternal(
					"hierarchy starts under " + parent.toString());
			}
		}

		// Move to slicer?
		switch (toAxis) {
		case slicerAxis:
			// we do not care of expression is already a Member, because it's a
			// very rare case; we have to make a new expression containing
			// default
			e = new FunCall("DefaultMember", new Exp[] {hierarchy}, FunDef.TypeProperty);
			putInAxisPosition(e, toAxis, iPositionOnAxis);
			break;
		case xAxis:
		case yAxis:
			// If this hierarchy is new, create an expression to display the
			// children of the default member (which is, we hope, the root
			// member).
			if (e == null) {
				if (bExpand)
					e = new FunCall("Children", new Exp[] {hierarchy}, FunDef.TypeProperty);
				else {
					e = new FunCall("DefaultMember", new Exp[] {hierarchy},
									FunDef.TypeProperty);
					e = new FunCall("{}", new Exp[] {e}, FunDef.TypeBraces);
				}
			} else if (fromAxis == slicerAxis) {
				// Expressions on slicers are stored as DefaultMember.  We need
				// to convert it to $Brace expression first (curly braces
				// needed).
				e = new FunCall("{}", new Exp[] {e}, FunDef.TypeBraces);
			}

			// Move to regular axis.
			putInAxisPosition(e, toAxis, iPositionOnAxis);
			break;

		case noAxis:
			// Discard hierarchy.  Nothing to do.
			break;

		default:
			throw Util.getRes().newInternal("bad axis code: " + toAxis);
		}
	}

	/**
	 * Filters the set of elements which are returned from a hierarchy.  If
	 * hierarchy is in the slicer, the set must contain exactly one element.
	 * (Hierarchy must be on the axis specified.)
	 *
     * 'members' are the members to be displayed.  They may be from different
     * levels - for example, {[USA], [USA].[California]} - and their order is
     * important.
	 **/
	public void filterHierarchy(
		Hierarchy hierarchy, int /*axisType*/ axis, Member[] members)
	{
		// Check that there can be only one filter per hierarchy applied on
		// slicer.
		if (axis == slicerAxis && members.length > 1) {
			throw Util.getRes().newInternal(
				"there can be only one filter per hierarchy on slicer");
		}
		// Check that members are all in the right hierarchy.
		for (int iMember = 0; iMember < members.length; iMember++) {
			if (!members[iMember].getHierarchy().equals(hierarchy)) {
				throw Util.getRes().newInternal(
					"member " + members[iMember] +
					" is not in hierarchy " + hierarchy);
			}
		}

		Walker walker = findHierarchy(hierarchy.getHierarchy());
		if (walker == null) {
			// Hierarchy is not currently used.  Put it at the last position on
			// the desired axis, then filter it.
			moveHierarchy(hierarchy, noAxis, axis, -1, true);
			walker = findHierarchy(hierarchy.getHierarchy());
			Util.assertTrue(walker != null, "hierarchy wasn't added");
		}

		// The expression we find may be either:
		// a) a member, for example '[Gender].[M]' in
		//      ([Gender].[M], [Marital Status].[S])
		//    or in
		//      CrossJoin([Marital Status].Members, {[Gender].[M]}); or
		// b) a set, for example '[Gender].Members' in
		//      CrossJoin([Store].Members, [Gender].Members).
		// We replace a set with a set, and a member with a member.
		QueryPart parent = (QueryPart) walker.getParent();
		int iOrdinal = walker.getOrdinal();
		Exp e = (Exp) walker.currentElement();
		if (e.isMember()) {
			Util.assertTrue(
				members.length == 1,
				"filterHierarchy cannot replace member with set");
			parent.replaceChild(iOrdinal, (QueryPart) members[0]);
		} else if (e.isSet()) {
			// Build a set out of the members supplied using the "{}" operator.
			// If there are no members, revert to the default member (for bug
			// 13728).
			Exp[] exps = members;
			if (members.length == 0) {
				exps = new Exp[] {new FunCall(
					"DefaultMember", new Exp[] {hierarchy}, FunDef.TypeProperty)};
			}
			// Neither slicer nor the tuple function (which is likely to occur
			// in a slicer) can have a set as a child, so reduce a singleton
			// set to a member in these cases.
			Exp exp =
				exps.length == 1 &&
				(parent instanceof Query || // because e is slicer
				 parent instanceof FunCall &&
				 ((FunCall) parent).isCallToTuple())
				? exps[0]
				: new FunCall("{}", exps, FunDef.TypeBraces);
			parent.replaceChild(iOrdinal, (QueryPart) exp);
		} else {
			throw Util.newInternal(
				"findHierarchy returned a " +
				Exp.catEnum.getName(e.getType()));
		}
	}

	/** ToggleDrillState. */
	public void toggleDrillState(Member member)
	{
		Walker walker = findHierarchy(member.getHierarchy());
		if (walker == null) throw Util.getRes().newInternal(
				"member's dimension is not used: " + member.toString());

		// If 'e' is our expression, then
		//    f(..., e, ...)
		// becomes
		//    f(..., ToggleDrillState(e, {member}), ...)
		Exp e = (Exp) walker.currentElement();
		FunCall funToggle = new FunCall(
			"ToggleDrillState", new Exp[] {
				e, new FunCall(
					"{}",
					new Exp[] {member},
					FunDef.TypeBraces)});
		QueryPart parent = (QueryPart) walker.getParent();
		int iOrdinal = walker.getOrdinal();
		parent.replaceChild(iOrdinal, funToggle);
	}


	/** Sort.  'axis' is the axis to sort; direction is one of {ascending,
     * descending, none}; specification is the expression to sort on.  For
     * example, the y-axis can be sorted by [Time].[Quarter] (its name), or by
     * {[Measures].[Unit Sales], [Stores].[California]} (Unit Sales in
     * California).  In general, the latter specification identifies a single
     * column (or row, for x-axis sorting) for each hierarchy on the other
     * axis. This function always removes previous sort on iAxis. If direcion
	 * is "none" then axis becomes sorted in natural order (no explicit
	 * sorting)*/
	public void sort(
		int /*axisType*/ axis, int /*sortDirection*/ direction,
		Member[] members)
	{
		Util.assertTrue(axis < axes.length, "Bad axis code");
		// Find and remove any existing sorts on this axis.
		removeSortFromAxis(axis);

		//apply new sort
		String sDirection;
		switch (direction) {
		case ascDirection: sDirection = "ASC"; break;
		case descDirection: sDirection = "DESC"; break;
    case bascDirection: sDirection = "BASC"; break;
    case bdescDirection: sDirection = "BDESC"; break;
		case noneDirection: /*we already removed the sort*/ return;
		default:
				throw Util.getRes().newInternal("bad direction code " + direction);
		}

		Exp e = axes[axis].set;

		if (members.length == 0)
			// No members to sort on means use default sort order.  As
			// we've already removed any sorters, we're done.
			return;
		else {
			FunCall funOrder = new FunCall(
				"Order",
				new Exp[] {
					e,
					members.length == 0 ? null : // handled above
					members.length == 1 ? (Exp) members[0] :
					(Exp) new FunCall(
						"()",
						members,
						FunDef.TypeParentheses),
					Literal.createSymbol(sDirection)});
			axes[axis].set = funOrder;
		}
	}

	/** Finds and removes existing sorts and TopBottomN functions from axis*/
	public void removeSortFromAxis(int /*axisType*/ axis)
	{
		// Find and remove any existing sorts on this axis.
		Util.assertTrue(axis < axes.length, "Bad axis code");
		Walker walker = new Walker((QueryPart) axes[axis].set);
		while (walker.hasMoreElements()) {
			Object o = walker.nextElement();
			if (o instanceof FunCall) {
				FunCall funCall = (FunCall) o;
				if (!funCall.isCallTo("Order") &&
					!isValidTopBottomNName(funCall.getFunName()))
					continue;

				Exp e = funCall.args[0];
				QueryPart parent = (QueryPart) walker.getParent();
				if (parent == null) {
					axes[axis].set = e;
				} else {
					parent.replaceChild(walker.getOrdinal(), (QueryPart) e);
				}
			}
		}
	}


	/** Calls removeSortFromAxis first and then applies TopBottomN function to
	 * the axis */
	public void applyTopBottomN(
		int /*axisType*/ axis, String /*function's name*/ fName,
		Integer /*N*/ n, /*sorting members*/ Member[] members)
	{
		Util.assertTrue(fName != null, "TopBottomN function name" +
						  " can not be null");
		Util.assertTrue(axis < axes.length, "Bad axis code");
		if (members.length == 0) throw Util.getRes().newMdxTopBottomNRequireSortMember();

		if (!isValidTopBottomNName(fName)) throw Util.getRes().newMdxTopBottomInvalidFunctionName(fName);

		// Find and remove any existing sorts on this axis.
		removeSortFromAxis(axis);

		Exp e = axes[axis].set;
		FunCall funOrder = new FunCall(
			fName,
			new Exp[] {
				e,
				Literal.create(n),
				members.length == 1 ? (Exp) members[0] :
				(Exp) new FunCall(
					"()", members, FunDef.TypeParentheses)});
		axes[axis].set = funOrder;
	}

	boolean isValidTopBottomNName(String fName)
	{
		if (fName.equalsIgnoreCase("TopCount") ||
			fName.equalsIgnoreCase("BottomCount") ||
			fName.equalsIgnoreCase("TopPercent") ||
			fName.equalsIgnoreCase("BottomPercent"))
			return true;
		return false;
	}

	/** Returns filtered sQuery based on user's grant privileges. {@link
	 * CubeAccess} contains a list of forbidden hiearchies, and limited
	 * members. oFilterAxesMembers[0] will contain array of mdxMembers, which
	 * would have to be applied after query execution. (it is very hard to
	 * apply limited members on expressions like [Food].children)
	 * */
	public String processFilterQuery(
		CubeAccess cubeAccess, ArrayList oFilterAxesMembers[])
	{
		if (!cubeAccess.hasRestrictions()){
			return this.toPlatoMdx();
		}
		//it is possible that some of parameters are no longer used and
		// they have to be purged from ParametersDefs
		resolveParameters();
		Query query = safeClone();
		oFilterAxesMembers[0] = query.applyPermissions(cubeAccess);
		return query.toPlatoMdx();
	}

	/** Apply {@link CubeAcess} permissions to the query by filtering
	 * hierarchies, putting limits on the slicer, etc.  If some of the
	 * permissions can not be applied (user used expression like currentYear,
	 * which can not be computed prior to running query) the function returns
	 * array of limited members, which could not be applied. It means, that the
	 * we need to parse the results before showing them to the user.
	 */
	private ArrayList applyPermissions(CubeAccess cubeAccess)
	{
		ArrayList filterAxesMemberList = null;
		// first check: if query contains any forbidden hierarchies
		Hierarchy[]  noAccessHierarchies =
			cubeAccess.getNoAccessHierarchies();
		if (noAccessHierarchies != null){
			for( int i = 0; i < noAccessHierarchies.length; i++ ){
				Walker walker = findHierarchy( noAccessHierarchies[i] );
				if (walker != null){
					// noAccess hierarchy is used; reject the query
					throw Util.getRes().newUserDoesNotHaveRightsTo(
						noAccessHierarchies[i].getUniqueName());
				}
			}
		}
		//second check: we need to apply restricted hierarchies
		Member[] limitedMembers = cubeAccess.getLimitedMembers();
		if (limitedMembers != null){
			for (int i = 0; i < limitedMembers.length; i++){
				Hierarchy limitedHierarchy =
					limitedMembers[i].getHierarchy();
				Member[] mdxMember={ limitedMembers[i] };
				Walker walker = findHierarchy(limitedHierarchy);
				if (walker == null) {
					//put limitedMember on the slicer
					filterHierarchy(limitedHierarchy, slicerAxis, mdxMember);
				} else {
					// the hierarchy is used somewhere in query
					// if it is used on the slicer, we should modify the slicer
					// to include it. If it is used on one of the axes, it
					// we will parse returned results
					int axis = getAxisCodeForWalker(walker);
					if (axis == slicerAxis){
						Object foundNode = walker.currentElement();
						Member foundMember = null;
						if (foundNode instanceof Member){
							foundMember = (Member) foundNode;
						} else if (foundNode instanceof FunCall &&
									((FunCall) foundNode).isCallToTuple()) {
							// tuple has only one node, which is our possible
							// target
							FunCall funCall = (FunCall) foundNode;
							if (funCall.args[0] instanceof Member) {
								foundMember = (Member) funCall.args[0];
							}
						}
						if (foundMember != null){
							//we found  member (not member expression)
							applyLimitOnMember(
								foundMember, limitedMembers[i], axis); 
						} else {
							// it looks like member is within an expression on
							// slicer. let's remove it from there and add
							// member
							moveHierarchy(
								limitedHierarchy, slicerAxis, noAxis, 0, true);
							filterHierarchy(
								limitedHierarchy, slicerAxis, mdxMember);
						}
					} else if (axis == xAxis || axis == yAxis) {
						if (walker.currentElement() instanceof Member) {
							// try to apply the limitation before executing
							// query
							applyLimitOnMember(
								(Member) walker.currentElement(),
								limitedMembers[i], axis);
						} else {
							// there might be an expression on the axes.  we
							// will filter the result set, so we need to build
							// filters
							if (filterAxesMemberList == null) {
								filterAxesMemberList = new ArrayList();
							}
							filterAxesMemberList.add(limitedMembers[i]);
						}
					}
				}
			}
		}
		return filterAxesMemberList;
	}

	/**
	 * This function takes the walker, which is presumably Member or
	 * Hierarchy and return the axis code, on which it was found.
	 **/
	private int getAxisCodeForWalker(Walker walker)
	{
		int depth = 0;
		Object parent = walker.getAncestor(depth);
		Util.assertTrue(
			parent != null,
			"failed to find Axis for" + walker.currentElement().toString());

		//walk up the tree
		++depth;
		Object grandParent = walker.getAncestor( depth );
		++depth;
		while (grandParent != null &&
			   !(grandParent instanceof Query)) {
			parent = grandParent;
			grandParent = walker.getAncestor( depth );
			++depth;
		}
		if (parent instanceof Axis) {
			if (((QueryAxis) parent).axisName.equals("columns")) {
				return xAxis;
			}
			return yAxis;
		} else if (parent instanceof FunCall &&
				   ((FunCall) parent).isCallToTuple()) {
			return slicerAxis;
		}
		return noAxis;
	}

	private void applyLimitOnMember(
		Member foundMember, Member limitedMember, int axis)
	{
		if (foundMember.isChildOrEqualTo(limitedMember)) {
			return;
		} else if (limitedMember.isChildOrEqualTo(foundMember)) {
			Member[] mdxMembers = {limitedMember};
			filterHierarchy(foundMember.getHierarchy(), axis, mdxMembers);
		} else {
			// limitedMember and foundMember are not inheriting each other
			// example. [OR].[Seattle] and [CA].[San Jose]
			throw Util.getRes().newUserDoesNotHaveRightsTo(
				foundMember.getUniqueName());
		}
	}

	//
	public Parameter createOrLookupParam(FunCall fParam)
	{
		//this is a definition of parameter
		Util.assertTrue(
			fParam.args[0] instanceof Literal,
			"The name of parameter has to be a quoted string");
		String name = (String) ((Literal)fParam.args[0]).getValue();
		Parameter param = lookupParam(name);
		if( param == null ){
			// Create a new parameter.
			param = new Parameter( fParam );

			// Append it to the array of known parameters.
			Parameter[] oldParameters = parameters;
			parameters = new Parameter[oldParameters.length + 1];
			for (int i = 0; i < oldParameters.length; i++) {
				parameters[i] = oldParameters[i];
			}
			parameters[oldParameters.length] = param;

		} else {
			// the parameter is already defined, update it
			param.update( fParam );
		}
		return param;
	}

	public Parameter lookupParam( String pName )
	{
		for( int i = 0; i < parameters.length; i++ ){
			if( parameters[i].getName().equals(pName )){
				return parameters[i];
			}
		}
		return null;
	}

	//validate each parameter, calculated their usage and  clean unused ones
	void resolveParameters()
	{
		//validate definitions
		ArrayList validParameters = new ArrayList();
		for (int i = 0; i < parameters.length; i++) {
			if (!parameters[i].isToBeDeleted()) {
				parameters[i].validate(this);
				validParameters.add(parameters[i]);
			}
		}
		parameters = (Parameter[]) validParameters.toArray(new Parameter[0]);

		//calculate usage
		for (int i = 0; i < parameters.length; i++) {
			parameters[i].nUses = 0;
		}
		Walker queryElements = new Walker(this);
		while (queryElements.hasMoreElements()) {
			Object queryElement = queryElements.nextElement();
			if (queryElement instanceof Parameter) {
				boolean found = false;
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].equals( queryElement )){
						parameters[i].nUses++;
						found = true;
						break;
					}
				}
				if (!found) throw Util.getRes().newMdxParamNotFound(
						((Parameter) queryElement).name);
			}
		}
	}

	/**
	 * Returns the parameters used in this query.
	 **/
	public Parameter[] getParameters()
	{
		resolveParameters();
		return parameters;
	}

	void resetParametersPrintProperty()
	{
		for( int i = 0; i < parameters.length; i++ )
			parameters[i].resetPrintProperty();
	}

	// implement NameResolver
	public Cube getCube() {
		return mdxCube;
	}
//  	public OlapElement get(OlapElement e) {
//  		return mdxCube.get(e);
//  	}
//  	public Dimension get(Dimension dimension, Cube parent) {
//  		return mdxCube.get(dimension, parent);
//  	}
//  	public Hierarchy get(Hierarchy hierarchy, Dimension parent) {
//  		return mdxCube.get(hierarchy, parent);
//  	}
//  	public Level get(Level level, Hierarchy parent) {
//  		return mdxCube.get(level, parent);
//  	}
//  	public Member get(Member member, Level parent) {
//  		return mdxCube.get(member, parent);
//  	}

	// implement NameResolver
	public OlapElement lookupChild(
		OlapElement parent, String s, boolean failIfNotFound)
	{
		OlapElement mdxElement = null;
		// first look in cube
		mdxElement = mdxCube.lookupChild(parent, s, false);
		if (mdxElement != null)
			return mdxElement;

		// then look in defined members
		Iterator definedMembers = getDefinedMembers().iterator();
		while (definedMembers.hasNext()) {
			Member mdxMember = (Member) definedMembers.next();
			if (mdxMember.getName().equalsIgnoreCase(s)) //member might be
				// referenced without dimension name in the query - bug21327
				return mdxMember;
		}

		// then in defined sets
		for (int i = 0; i < formulas.length; i++) {
			Formula formula = formulas[i];
			if (formula.isMember)
				continue;		// have already done these

			if (formula.names[0].equals(s)) {
				return formula.mdxSet;
			}
		}

		// fail if we didn't find it
		if (mdxElement == null && failIfNotFound) {
			throw Util.getRes().newMdxChildObjectNotFound(
				s, parent.getQualifiedName());
		}
		return mdxElement;
	}

	// implement NameResolver
	public Member lookupMember(String s, boolean failIfNotFound) {
		return Util.lookupMember(this,s,failIfNotFound);
	}

	// implement NameResolver
	public Member lookupMemberCompound(
		String[] names, boolean failIfNotFound)
	{
		return Util.lookupMemberCompound(this, names, failIfNotFound);
	}

	// implement NameResolver
	public Member lookupMemberByUniqueName(String s, boolean failIfNotFound)
	{
		Member member = lookupMemberFromCache(s);
		if (member == null) {
			member = mdxCube.lookupMemberByUniqueName(s, failIfNotFound);
		}
		return member;
	}

	// implement NameResolver
	public Member lookupMemberFromCache(String s) {
		// first look in defined members
		Iterator definedMembers = getDefinedMembers().iterator();
		while (definedMembers.hasNext()) {
			Member mdxMember = (Member) definedMembers.next();
			if (mdxMember.getUniqueName().equals(s)) {
				return mdxMember;
			}
		}
		// then look in cube
		return mdxCube.lookupMemberFromCache(s);
	}

	/** Return an array of the formulas used in this query. */
	public Formula[] getFormulas()
	{
		return formulas;
	}

	/** Remove a formula from the query. If <code>failIfUsedInQuery</code> is
	 * true, checks and throws an error if formula is used somewhere in the
	 * query; otherwise, what??? */
	public void removeFormula(String uniqueName, boolean failIfUsedInQuery)
	{
		Formula formula = findFormula(uniqueName);
		if (failIfUsedInQuery && formula != null) {
			OlapElement mdxElement = formula.getElement();
			//search the query tree to see if this formula expression is used
			//anywhere (on the axes or in another formula)
			Walker walker = new Walker(this);
			while (walker.hasMoreElements()) {
				Object queryElement = walker.nextElement();
				if (!queryElement.equals(mdxElement)) {
					continue;
				}
				// mdxElement is used in the query. lets find on on which axis
				// or formula
				String formulaType = formula.isMember() ?
					Util.getRes().getCalculatedMember() :
					Util.getRes().getCalculatedSet();

				int i = 0;
				Object parent = walker.getAncestor(i);
				Object grandParent = walker.getAncestor(i+1);
				while (parent != null && grandParent != null) {
					if (grandParent instanceof Query) {
						if (parent instanceof Axis) {
							throw Util.getRes().newMdxCalculatedFormulaUsedOnAxis(
								formulaType, uniqueName,
								((QueryAxis) parent).axisName);
						} else if (parent instanceof Formula) {
							String parentFormulaType =
								((Formula) parent).isMember() ?
								Util.getRes().getCalculatedMember() :
								Util.getRes().getCalculatedSet();
							throw Util.getRes().newMdxCalculatedFormulaUsedInFormula(
								formulaType, uniqueName, parentFormulaType,
								((Formula) parent).getUniqueName());
						} else {
							throw Util.getRes().newMdxCalculatedFormulaUsedOnSlicer(
								formulaType, uniqueName);
						}
					}
					++i;
					parent = walker.getAncestor(i);
					grandParent = walker.getAncestor(i+1);
					}
				throw Util.getRes().newMdxCalculatedFormulaUsedInQuery(
					formulaType, uniqueName, this.toWebUIMdx());
			}
		}

		//remove formula from query
		ArrayList formulaList = new ArrayList();
		for (int i = 0; i < formulas.length; i++) {
			if (!formulas[i].getUniqueName().equalsIgnoreCase(uniqueName)) {
				formulaList.add(formulas[i]);
			}
		}

        // it has been found and removed
		this.formulas = (Formula[]) formulaList.toArray(new Formula[0]);
	}

	/** finds calculated member or set in array of formulas */
	public Formula findFormula(String uniqueName)
	{
		for (int i = 0; i < formulas.length; i++) {
			if (formulas[i].getUniqueName().equalsIgnoreCase(uniqueName))
				return formulas[i];
		}
		return null;
	}

	/** finds formula by name and renames it to new name */
	public void renameFormula(String uniqueName, String newName)
	{
		Formula formula = findFormula(uniqueName);
		if (formula == null) throw Util.getRes().newMdxFormulaNotFound(
				"formula", uniqueName, toWebUIMdx());
		formula.rename(newName);
	}

	ArrayList getDefinedMembers()
	{
		ArrayList definedMembers = new ArrayList();
		for (int i = 0; i < formulas.length; i++) {
			if (formulas[i].isMember && formulas[i].getElement() != null) {
				definedMembers.add((Member)formulas[i].getElement());
            }
		}
		return definedMembers;
	}

	/** finds axis by index and sets flag to show empty cells on that axis*/
	public void setAxisShowEmptyCells(int axis, boolean showEmpty)
	{
		if (axis >= axes.length) {
			throw Util.getRes().newMdxAxisShowEmptyCellsNotSupported(
					new Integer(axis));
		}
		axes[axis].nonEmpty = !showEmpty;
	}

	/** finds axis by index and adds/removes subtotals. It finds all
	 * hierarchies used on axis, then for every hierarchy it finds the
	 * expression, where it's used. Using that expression, it executes mdx
	 * query to generate array of mdxMembers. Based on
	 * <code>showSubtotals</code> it modifies array of mdxMembers and
	 * substitutes expression with set, which is created based on array of
	 * mdxMembers */
	public void setAxisShowSubtotals(int axis, boolean showSubtotals)
	{
		if (axis >= axes.length || axis < 0) {
			//based on Prashant request: don't throw error-just return
			return;
		}

		String sCalculatedMembers = null;
		if (formulas != null) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			for (int i = 0; i < formulas.length; i++) {
				if (i == 0) {
					pw.print("with ");
				} else {
					pw.print("  ");
				}
				formulas[i].unparse(pw, new PlatoCallBack());
				pw.println();
			}
			sCalculatedMembers = sw.toString();
		}

		Hierarchy[] mdxHierarchies = collectHierarchies(axes[axis]);
		for (int j = 0; j < mdxHierarchies.length; j++) {
			Walker walker = findHierarchy(mdxHierarchies[j]);
			Exp e = (Exp) walker.currentElement();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.unparse(pw, new PlatoCallBack());
			String sExp = sw.toString();
			String sQuery = "";
			if (sCalculatedMembers != null)
				sQuery = sCalculatedMembers;
			sQuery += "select {" + sExp + "} on columns from [" +
				mdxCube.getUniqueName() + "]";
			Member[] mdxMembers = mdxCube.getMembersForQuery(
				sQuery, getDefinedMembers());
			HashSet set = new HashSet();
			if (showSubtotals) {
				// we need to put all those members plus all their parent
				// members
				for (int i = 0; i < mdxMembers.length; i++){
					if (!set.contains(mdxMembers[i])) {
						Member[] parentMembers =
							mdxMembers[i].getAncestorMembers();
						for (int k = parentMembers.length - 1; k >= 0; k--) {
							if (!set.contains(parentMembers[k]))
								set.add(parentMembers[k]);
						}
						set.add(mdxMembers[i]);
					}
				}
			} else {
				//we need to put only members with biggest depth
				int nMaxDepth = 0;
				for (int i = 0; i < mdxMembers.length; i++){
					if (nMaxDepth < mdxMembers[i].getDepth())
						nMaxDepth = mdxMembers[i].getDepth();
				}
				for (int i = 0; i < mdxMembers.length; i++){
					if (nMaxDepth == mdxMembers[i].getDepth()) {
						set.add(mdxMembers[i]);
					}
				}
			}
			Member[] goodMembers = (Member[]) set.toArray(new Member[0]);
			filterHierarchy(mdxHierarchies[j], axis, goodMembers);
		}
		axes[axis].setShowSubtotals(showSubtotals);
	}

	/** returns <code>Hierarchy[]</code> used on <code>axis</code>. It calls
	 * collectHierarchies() */
	public Hierarchy[] getMdxHierarchiesOnAxis(int axis)
	{
		if (axis >= axes.length) {
			throw Util.getRes().newMdxAxisShowSubtotalsNotSupported(new Integer(axis));
		}
		if (axis == Query.slicerAxis) {
			return collectHierarchies((QueryPart) slicer);
		} else {
			return collectHierarchies(axes[axis]);
		}
	}
	
}

// End Query.java
