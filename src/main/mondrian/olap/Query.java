/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2003 Kana Software, Inc. and others.
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
 * <code>Query</code> is an MDX query.
 *
 * <p>It is created by calling {@link Connection#parseQuery},
 * and executed by calling {@link Connection#execute},
 * to return a {@link Result}.
 *
 * <p><code>Query</code> contains several methods to manipulate the parse tree
 * of the query; {@link #swapAxes} and {@link #drillDown} are examples.
 **/
public class Query extends QueryPart {

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

	/**
	 * Returns the MDX query string. If the query was created by parsing an
	 * MDX string, the string returned by this method may not be identical, but
	 * it will have the same meaning. If the query's parse tree has been
	 * manipulated (for instance, the rows and columns axes have been
	 * interchanged) the returned string represents the current parse tree.
	 */
	public String getQueryString() {
		return toWebUIMdx();
	}

	private void normalizeAxes()
	{
		for (int i = 0; i < axes.length; i++) {
			String correctName = AxisOrdinal.instance.getName(i);
			if (!axes[i].axisName.equalsIgnoreCase(correctName)) {
				for (int j = i + 1; j < axes.length; j++) {
					if (axes[j].axisName.equalsIgnoreCase(correctName)) {
						// swap axes
						QueryAxis temp = axes[i];
						axes[i] = axes[j];
						axes[j] = temp;
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
			for (int i = 0; i < formulas.length; i++) {
				formulas[i].createElement(q);
			}
			for (int i = 0; i < formulas.length; i++) {
				formulas[i].resolve(q);
			}
		}

		if (axes != null)
			for (int i = 0; i < axes.length; i++)
				axes[i] = (QueryAxis) axes[i].resolve(q);

		if (slicer != null) {
			setSlicer(slicer.resolve(q));
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
					pw.println(",");
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
	 * @param iPhysicalAxis ordinal of axis in cellset
	 * @return axis label in original query (0 = columns, 1 = rows, etc.)
	 */
	public int getLogicalAxis(int iPhysicalAxis)
	{
		if (iPhysicalAxis == AxisOrdinal.SLICER || iPhysicalAxis == axes.length) {
			return AxisOrdinal.SLICER; // slicer is never permuted
		}
		String axisName = axes[iPhysicalAxis].axisName;
        final EnumeratedValues.Value value = AxisOrdinal.instance.getValue(axisName);
		if (value != null) {
			return value.getOrdinal();
		}
		return AxisOrdinal.NONE;
	}

	/** The inverse of {@link #getLogicalAxis}. */
	public int getPhysicalAxis(int iLogicalAxis)
	{
		if (iLogicalAxis < 0) {
			return iLogicalAxis;
		}
		String axisName = AxisOrdinal.instance.getName(iLogicalAxis);
		for (int i = 0; i < axes.length; i++) {
			if (axes[i].axisName.equalsIgnoreCase(axisName)) {
				return i;
			}
		}
		return AxisOrdinal.NONE;
	}

	/** Constructs hidden unique name based on given uName. It is used for
	 * formatting existing measures. */
	public static String getHiddenMemberUniqueName(String uName)
	{
		int i = uName.lastIndexOf("].[");
		return uName.substring(0, i + 3) + hidden + uName.substring(i+3);
	}

	/** checks for hidden string in name and strips it out. It looks only for
	 * first occurence */
	public static String stripHiddenName(String name)
	{
		final int i = name.indexOf(hidden);
		if (i >= 0) {
			return name.substring(0, i) + name.substring(i + hidden.length());
		}
		return name;
	}

	public static String getHiddenMemberFormulaDefinition(String uName) {
		return uName;
	}

	/** Returns the MDX query string. */
	public String toString() {
		return toWebUIMdx();
	}

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
		if (i < axes.length) {
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

	/** Returns an enumeration, each item of which is an Ob containing a
	 * dimension which does not appear in any Axis or in the slicer. */
	public Iterator unusedDimensions() {
		Dimension[] mdxDimensions = mdxCube.getDimensions();
        return Arrays.asList(mdxDimensions).iterator();
	}

	/**
	 * Adds a level to an axis expression.
	 *
	 * @pre AxisOrdinal.instance().isValid(axis)
	 * @pre axis &lt; axes.length
	 */
	public void addLevelToAxis(int axis, Level level) {
		Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), "AxisOrdinal.instance.isValid(axis)");
		Util.assertPrecondition(axis < axes.length, "axis < axes.length");
		axes[axis].addLevel(level);
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
		ArrayList hierList = new ArrayList();
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
        if(!hierList.contains(obExpHierarchy))
				  hierList.add(obExpHierarchy);
			}
		}

		return (Hierarchy[]) hierList.toArray(new Hierarchy[0]);
	}

	/** Place expression 'exp' at position 'iPositionOnAxis' on axis 'axis'. */
	private void putInAxisPosition(Exp exp, int axis, int iPositionOnAxis)
	{
		switch (axis) {
		case AxisOrdinal.SLICER:
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
					false, null, AxisOrdinal.instance.getName(axis),
					QueryAxis.SubtotalVisibility.Undefined);
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
							Util.lookupHierarchyLevel(hierarchy, "(All)"),
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
	 * The hierarchy is added if <code>fromAxis</code> is {@link AxisOrdinal#NONE},
	 * and removed if <code>toAxis</code> is {@link AxisOrdinal#NONE}.
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
		if (fromAxis == AxisOrdinal.NONE) {
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
			} else if (parent instanceof Query && fromAxis == AxisOrdinal.SLICER) {
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
					   fromAxis == AxisOrdinal.SLICER ){
				int iOrdinal = walker.getOrdinal();
				((FunCall)slicer).removeChild( iOrdinal );
			} else if (parent instanceof Parameter) {
				// The hierarchy is a child of parameter, so we need to remove
				// the parameter itself.
				QueryPart grandparent = (QueryPart) walker.getAncestor(2);
				int iParentOrdinal = walker.getAncestorOrdinal(1);
				if( grandparent instanceof FunCall &&
					   ((FunCall)grandparent).isCallToTuple() &&
					   fromAxis == AxisOrdinal.SLICER ){
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
		case AxisOrdinal.SLICER:
			// we do not care of expression is already a Member, because it's a
			// very rare case; we have to make a new expression containing
			// default
			e = new FunCall("DefaultMember", new Exp[] {hierarchy}, FunDef.TypeProperty);
			putInAxisPosition(e, toAxis, iPositionOnAxis);
			break;
		case AxisOrdinal.COLUMNS:
		case AxisOrdinal.ROWS:
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
			} else if (fromAxis == AxisOrdinal.SLICER) {
				// Expressions on slicers are stored as DefaultMember.  We need
				// to convert it to $Brace expression first (curly braces
				// needed).
				e = new FunCall("{}", new Exp[] {e}, FunDef.TypeBraces);
			}

			// Move to regular axis.
			putInAxisPosition(e, toAxis, iPositionOnAxis);
			break;

		case AxisOrdinal.NONE:
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
	 *
	 * @pre AxisOrdinal.instance().isValid(axis)
	 * @pre axis &lt; axes.length
	 **/
	public void filterHierarchy(
		Hierarchy hierarchy, int /*axisType*/ axis, Member[] members)
	{
		Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), "AxisOrdinal.instance.isValid(axis)");
		Util.assertPrecondition(axis < axes.length, "axis < axes.length");
		// Check that there can be only one filter per hierarchy applied on
		// slicer.
		if (axis == AxisOrdinal.SLICER && members.length > 1) {
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
			moveHierarchy(hierarchy, AxisOrdinal.NONE, axis, -1, true);
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
			throw Util.newInternal("findHierarchy returned a " +
					Category.instance.getName(e.getType()));
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


	/**
	 * Sort.
	 *
	 * <p>This function always removes previous sort on <code>axis</code>.
	 * If <code>direction</code> is "none" then axis becomes sorted in natural
	 * order (no explicit sorting).
	 *
	 * @param axis is the axis to sort, a member of {@link AxisOrdinal}
	 * @param direction is the direction to sort, a member of {@link SortDirection}
	 * @param members is tuple of members to sort on.  For
     *   example, the y-axis can be sorted by [Time].[Quarter] (its name), or by
     *   {[Measures].[Unit Sales], [Stores].[California]} (Unit Sales in
     *   California).  In general, the latter specification identifies a single
     *   column (or row, for x-axis sorting) for each hierarchy on the other
     *   axis.
	 *
	 * @pre AxisOrdinal.instance().isValid(axis)
	 * @pre axis &lt; axes.length
	 * @pre SortDirection.instance.isValid(direction)
	 */
	public void sort(int axis, int direction, Member[] members) {
		Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), "AxisOrdinal.instance.isValid(axis)");
		Util.assertPrecondition(axis < axes.length, "axis < axes.length");
		Util.assertPrecondition(SortDirection.instance().isValid(direction), "SortDirection.instance().isValid(direction)");

		// Find and remove any existing sorts on this axis.
		removeSortFromAxis(axis);

		//apply new sort
		if (direction == SortDirection.NONE) {
			return; // we already removed the sort
		}
		String sDirection = SortDirection.instance().getName(direction);
		Exp e = axes[axis].set;

		if (members.length == 0) {
			// No members to sort on means use default sort order.  As
			// we've already removed any sorters, we're done.
			return;
		} else {
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

	/**
	 * Finds and removes existing sorts and top/bottom functions from axis.
	 *
	 * @pre AxisOrdinal.instance().isValid(axis)
	 * @pre axis &lt; axes.length
	 */
	public void removeSortFromAxis(int axis) {
		Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), "AxisOrdinal.instance.isValid(axis)");
		Util.assertPrecondition(axis < axes.length, "axis < axes.length");
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


	/**
	 * Calls {@link #removeSortFromAxis} first and then applies top/bottom
	 * function to the axis.
	 *
	 * @param axis Axis ordinal
	 * @param fName Name of function
	 * @param n Number of members top/bottom should return
	 * @param members Members to sort on
	 *
	 * @pre AxisOrdinal.instance().isValid(axis)
	 * @pre axis &lt; axes.length
	 * @pre fName != null
	 * @pre isValidTopBottomNName(fName)
	 * @pre members != null
	 * @pre members.length > 0
	 */
	public void applyTopBottomN(
			int axis, String fName, Integer n, Member[] members) {
		Util.assertPrecondition(fName != null, "fName != null");
		Util.assertPrecondition(AxisOrdinal.instance.isValid(axis), "AxisOrdinal.instance.isValid(axis)");
		Util.assertPrecondition(axis < axes.length, "axis < axes.length");
		Util.assertPrecondition(members != null, "members != null");
		Util.assertPrecondition(members.length > 0, "members.length > 0");
		Util.assertPrecondition(isValidTopBottomNName(fName), "isValidTopBottomNName(fName)");
		if (!isValidTopBottomNName(fName)) {
			throw Util.getRes().newMdxTopBottomInvalidFunctionName(fName);
		}

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

	public static boolean isValidTopBottomNName(String fName) {
		return fName.equalsIgnoreCase("TopCount") ||
			fName.equalsIgnoreCase("BottomCount") ||
			fName.equalsIgnoreCase("TopPercent") ||
			fName.equalsIgnoreCase("BottomPercent");
	}

	/**
	 * Swaps the x- and y- axes.
	 * Does nothing if the number of axes != 2.
	 */
	public void swapAxes() {
		if (axes.length == 2) {
			Exp e0 = axes[0].set;
			boolean nonEmpty0 = axes[0].nonEmpty;
			Exp e1 = axes[1].set;
			boolean nonEmpty1 = axes[1].nonEmpty;
			axes[1].set = e0;
			axes[1].nonEmpty = nonEmpty0;
			axes[0].set = e1;
			axes[0].nonEmpty = nonEmpty1;
			// showSubtotals ???
		}
	}

	/**
	 * Returns filtered sQuery based on user's grant privileges.
	 *
	 * @param cubeAccess Contains a list of forbidden hiearchies, and limited
	 *   members.
	 * @param oFilterAxesMembers An output parameter; its 0th element will
	 *   contain a list of {@link Member}s, which would have to be applied after
	 *   query execution.
	 **/
	public String processFilterQuery(
		CubeAccess cubeAccess, ArrayList oFilterAxesMembers[]) {
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

	/**
	 * Applies {@link CubeAccess} permissions to the query by filtering
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
					filterHierarchy(limitedHierarchy, AxisOrdinal.SLICER, mdxMember);
				} else {
					// the hierarchy is used somewhere in query
					// if it is used on the slicer, we should modify the slicer
					// to include it. If it is used on one of the axes, it
					// we will parse returned results
					int axis = getAxisCodeForWalker(walker);
					if (axis == AxisOrdinal.SLICER){
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
								limitedHierarchy, AxisOrdinal.SLICER, AxisOrdinal.NONE, 0, true);
							filterHierarchy(
								limitedHierarchy, AxisOrdinal.SLICER, mdxMember);
						}
					} else if (axis == AxisOrdinal.COLUMNS || axis == AxisOrdinal.ROWS) {
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
				return AxisOrdinal.COLUMNS;
			}
			return AxisOrdinal.ROWS;
		} else if (parent instanceof FunCall &&
				   ((FunCall) parent).isCallToTuple()) {
			return AxisOrdinal.SLICER;
		}
		return AxisOrdinal.NONE;
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

	/**
	 * Creates or retrieves the parameter corresponding to a "Parameter" or
	 * "ParamRef" function call.
	 */
	Parameter createOrLookupParam(FunCall funCall)
	{
		//this is a definition of parameter
		Util.assertTrue(
			funCall.args[0] instanceof Literal,
			"The name of parameter has to be a quoted string");
		String name = (String) ((Literal)funCall.args[0]).getValue();
		Parameter param = lookupParam(name);
		if (param == null) {
			// Create a new parameter.
			param = new Parameter(funCall);

			// Append it to the array of known parameters.
			Parameter[] oldParameters = parameters;
			parameters = new Parameter[oldParameters.length + 1];
			for (int i = 0; i < oldParameters.length; i++) {
				parameters[i] = oldParameters[i];
			}
			parameters[oldParameters.length] = param;

		} else {
			// the parameter is already defined, update it
			param.update(funCall);
		}
		return param;
	}

	/**
	 * Returns a parameter with a given name, or <code>null</code> if there is
	 * no such parameter.
	 */
	public Parameter lookupParam(String parameterName)
	{
		for (int i = 0; i < parameters.length; i++) {
			if (parameters[i].getName().equals(parameterName)) {
				return parameters[i];
			}
		}
		return null;
	}

	/**
	 * Validates each parameter, calculates their usage, and removes unused
	 * parameters.
	 */
	private void resolveParameters() {
		//validate definitions
		for (int i = 0; i < parameters.length; i++) {
			parameters[i].validate(this);
		}
		int[] usageCount = new int[parameters.length];
		Walker queryElements = new Walker(this);
		while (queryElements.hasMoreElements()) {
			Object queryElement = queryElements.nextElement();
			if (queryElement instanceof Parameter) {
				boolean found = false;
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].equals(queryElement)) {
						usageCount[i]++;
						found = true;
						break;
					}
				}
				if (!found) {
					throw Util.getRes().newMdxParamNotFound(((Parameter) queryElement).name);
				}
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

	public SchemaReader getSchemaReader() {
		final SchemaReader cubeSchemaReader = mdxCube.getSchemaReader(getConnection().getRole());
		return new DelegatingSchemaReader(cubeSchemaReader) {
			public Member getMemberByUniqueName(String[] uniqueNameParts, boolean failIfNotFound) {
				Member member = lookupMemberFromCache(Util.implode(uniqueNameParts));
				if (member == null) {
					// Not a calculated member in the query, so go to the cube.
					member = schemaReader.getMemberByUniqueName(uniqueNameParts, failIfNotFound);
				}
				return member;
			}
			public OlapElement getElementChild(OlapElement parent, String s) {
				// first look in cube
				OlapElement mdxElement = schemaReader.getElementChild(parent, s);
				if (mdxElement != null) {
					return mdxElement;
				}
				// then look in defined members
				Iterator definedMembers = getDefinedMembers().iterator();
				while (definedMembers.hasNext()) {
					Member mdxMember = (Member) definedMembers.next();
					if (mdxMember.getName().equalsIgnoreCase(s)) {
						// allow member to be referenced without dimension name
						return mdxMember;
					}
				}

				// then in defined sets
				for (int i = 0; i < formulas.length; i++) {
					Formula formula = formulas[i];
					if (formula.isMember) {
						continue;		// have already done these
					}
					if (formula.names[0].equals(s)) {
						return formula.mdxSet;
					}
				}

				return mdxElement;
			}
		};
	}

	/**
	 * Looks up a member whose unique name is <code>s</code> from cache.
	 * If the member is not in cache, returns null.
	 **/
	public Member lookupMemberFromCache(String s) {
		// first look in defined members
		Iterator definedMembers = getDefinedMembers().iterator();
		while (definedMembers.hasNext()) {
			Member mdxMember = (Member) definedMembers.next();
			if (mdxMember.getUniqueName().equals(s)) {
				return mdxMember;
			}
		}
		return null;
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
				definedMembers.add(formulas[i].getElement());
            }
		}
		return definedMembers;
	}

	/** finds axis by index and sets flag to show empty cells on that axis*/
	public void setAxisShowEmptyCells(int axis, boolean showEmpty)
	{
		if (axis >= axes.length) {
			throw Util.getRes().newMdxAxisShowSubtotalsNotSupported(
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
					if (nMaxDepth < mdxMembers[i].getLevel().getDepth())
						nMaxDepth = mdxMembers[i].getLevel().getDepth();
				}
				for (int i = 0; i < mdxMembers.length; i++){
					if (nMaxDepth == mdxMembers[i].getLevel().getDepth()) {
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
		if (axis == AxisOrdinal.SLICER) {
			return collectHierarchies((QueryPart) slicer);
		} else {
			return collectHierarchies(axes[axis]);
		}
	}

}

// End Query.java
