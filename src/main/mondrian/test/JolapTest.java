/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.test;

import junit.framework.TestCase;
import mondrian.olap.Util;
import mondrian.jolap.MondrianMemberObjectFactories;
import org.omg.cwm.objectmodel.core.Attribute;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.olap.OLAPException;
import javax.olap.cursor.EdgeCursor;
import javax.olap.cursor.CubeCursor;
import javax.olap.cursor.DimensionCursor;
import javax.olap.metadata.*;
import javax.olap.query.dimensionfilters.*;
import javax.olap.query.enumerations.*;
import javax.olap.query.querycoremodel.*;
import javax.olap.query.CurrentMember;
import javax.olap.resource.Connection;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

/**
 * A <code>JolapTest</code> is ...
 *
 * @author jhyde
 * @since Dec 23, 2002
 * @version $Id$
 **/
public class JolapTest extends TestCase {
	private static final String nl = System.getProperty("line.separator");
	private MemberObjectFactories memberObjectFactories =
			new MondrianMemberObjectFactories();

	public JolapTest(String name) {
		super(name);
	}

	public void testConnect() throws OLAPException {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		doConnect(pw);
		pw.flush();
		assertEquals("Dimension name is Store Size in SQFT" + nl +
				"Dimension name is Store" + nl +
				"Dimension name is Time" + nl +
				"Dimension name is Product" + nl +
				"Dimension name is Store Type" + nl +
				"Dimension name is Warehouse" + nl,
				sw.toString());
	}

	/**
	 * Adapted from spec version 0.85, section 6.3.
	 */
	private void doConnect(PrintWriter out) throws OLAPException {
		Connection cx = getConnection();
		// Get the list of all JOLAP Dimensions for the default Schema
		// and print out their names...
		List dimList = cx.getDimensions();
		Iterator dimIter = dimList.iterator();
		while ( dimIter.hasNext() )
		{
			javax.olap.metadata.Dimension myDim
					= (javax.olap.metadata.Dimension)dimIter.next();
			out.println( "Dimension name is " + myDim.getName() );
		}
		// Now close the connection to the OLAP resource
		cx.close();
	}

	private Connection getConnection() throws OLAPException {
		try {
			Hashtable env = new Hashtable();
//			final String factoryClassName = "com.mycompany.javax.olap.resource.ConnectionFactoryImpl";
			String factoryClassName = mondrian.jolap.MondrianJolapConnectionFactory.class.getName();
			env.put( Context.INITIAL_CONTEXT_FACTORY,
					factoryClassName );
			Context initCtx = new InitialContext( env );
			// Obtain the JOLAP MondrianJolapConnectionFactory from the JNDI tree
			javax.olap.resource.ConnectionFactory cxf
					= (javax.olap.resource.ConnectionFactory)initCtx.lookup(
							"JOLAPServer" );
			// Create a connection spec
			javax.olap.resource.ConnectionSpec cxs
					=(javax.olap.resource.ConnectionSpec)
					cxf.createConnectionSpec();
			cxs.setName( "jolapuser" );
			cxs.setPassword( "guest" );
			// Note: if the specific type of ConnectionSpec is not known,
			// clients can introspect the returned instance to determine
			// which JavaBean-compliant attributes are required.
			// Establish a connection to the OLAP resource
			Connection cx = (Connection)cxf.getConnection( cxs );
			return cx;
		} catch (NamingException e) {
			throw Util.newError(e, "Error while making connection");
		}
	}

	public void testSimpleCubeView() throws OLAPException {
		Connection connection = getConnection();
		CubeView salesCube = connection.createCubeView();
		EdgeView rowsEdge = salesCube.createOrdinateEdge();
		EdgeView colsEdge = salesCube.createOrdinateEdge();
		EdgeView pageEdge = salesCube.createPageEdge();
	}

	public void _testSimpleDimensionView() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView products = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		products.setDimension(prod);
		DimensionStepManager steps = products.createDimensionStepManager();
		AttributeFilter nameFilter = (AttributeFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTEFILTER);
		Attribute nameAttr = (Attribute)prod.getFeature().get(0);
		nameFilter.setAttribute(nameAttr);
		nameFilter.setSetAction(SetActionTypeEnum.INITIAL);
		nameFilter.setOp(OperatorTypeEnum.EQ);
		nameFilter.setRhs( "Fred" );
	}

	public void testSimpleDimensionView() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView products = someConnection.createDimensionView();
		Dimension prod = getDimension(someConnection, "Product");
		products.setDimension(prod);
		DimensionStepManager steps = products.createDimensionStepManager();
		AttributeFilter nameFilter = (AttributeFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTEFILTER);
		Attribute nameAttr = getAttribute(prod, "Product Subcategory");
		nameFilter.setAttribute(nameAttr);
		nameFilter.setSetAction(SetActionTypeEnum.INITIAL);
		nameFilter.setOp(OperatorTypeEnum.EQ);
		nameFilter.setRhs( "Beer" );
	}

	private Attribute getAttribute(Dimension prod, String name) {
		for (Iterator features = prod.getFeature().iterator(); features.hasNext();) {
			Object o = features.next();
			if (o instanceof Attribute) {
				Attribute attribute = (Attribute) o;
				if (attribute.getName().equals(name)) {
					return attribute;
				}
			}
		}
		return null;
	}

	public void _testSimpleEdgeViewWithOneDimensionView() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView products = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		products.setDimension(prod);
		DimensionStepManager steps = products.createDimensionStepManager();
		AttributeFilter nameFilter = (AttributeFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTEFILTER);
		Attribute nameAttr = (Attribute)prod.getFeature().get(0);
		nameFilter.setAttribute(nameAttr);
		nameFilter.setSetAction(SetActionTypeEnum.INITIAL);
		nameFilter.setOp(OperatorTypeEnum.EQ);
		nameFilter.setRhs("Fred");
		CubeView sampleCube = someConnection.createCubeView();
		EdgeView rows = sampleCube.createOrdinateEdge();
		Segment defaultSegment = rows.createSegment();
		defaultSegment.addDimensionStepManager( steps );
	}

	public void testSimpleEdgeViewWithOneDimensionView() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView products = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		products.setDimension(prod);
		DimensionStepManager steps = products.createDimensionStepManager();
		AttributeFilter nameFilter = (AttributeFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTEFILTER);
		Attribute nameAttr = getAttribute(prod, "Product Subcategory");
		nameFilter.setAttribute(nameAttr);
		nameFilter.setSetAction(SetActionTypeEnum.INITIAL);
		nameFilter.setOp(OperatorTypeEnum.EQ);
		nameFilter.setRhs("Beer");
		CubeView sampleCube = someConnection.createCubeView();
		EdgeView rows = sampleCube.createOrdinateEdge();
		Segment defaultSegment = rows.createSegment();
		defaultSegment.addDimensionStepManager( steps );
	}

	/**
	 * "Sample code 8.3: Attribute Filter with Compound Step"
	 */
	public void testMultiStepAttributeFilter() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView multiStep = 	someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		multiStep.setDimension(prod);
		AttributeReference selectCode = (AttributeReference)
				multiStep.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTEREFERENCE);
		Attribute code = (Attribute)prod.getFeature().get(0);
		selectCode.setAttribute(code);
		DimensionStepManager steps = multiStep.createDimensionStepManager();
		AttributeFilter redFilter = (AttributeFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTEFILTER);
		Attribute color = (Attribute) prod.getFeature().get(1);
		redFilter.setAttribute(color);
		redFilter.setSetAction(SetActionTypeEnum.INITIAL);
		redFilter.setOp(OperatorTypeEnum.EQ);
		redFilter.setRhs("Red");
		AttributeFilter blueFilter = (AttributeFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTEFILTER);
		blueFilter.setAttribute(color);
		blueFilter.setSetAction(SetActionTypeEnum.APPEND);
		blueFilter.setOp(OperatorTypeEnum.EQ);
		blueFilter.setRhs("Blue");
	}


	/**
	 * "Sample code 8.4: Level Filter"
	 */
	public void testLevelFilter() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView partMembers = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		partMembers.setDimension(prod);
		AttributeReference selectCode = (AttributeReference)
				partMembers.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTEREFERENCE);
		Attribute code = (Attribute)prod.getFeature().get(0);
		selectCode.setAttribute(code);
		DimensionStepManager steps = partMembers.createDimensionStepManager();
		LevelFilter levelFilter = (LevelFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.LEVELFILTER);
		Collection parts = prod.getMemberSelection();
		Level part = (Level)parts.iterator().next();
		levelFilter.setLevel(part);
		levelFilter.setSetAction(SetActionTypeEnum.INITIAL);
	}

	/**
	 * "Sample code 8.5: Drill Filter"
	 */
	public void testDrillFilter() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView drill = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		drill.setDimension(prod);
		AttributeReference selectCode = (AttributeReference)
				drill.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTEREFERENCE);
		Attribute code = (Attribute)prod.getFeature().get(0);
		selectCode.setAttribute(code);
		DimensionStepManager steps = drill.createDimensionStepManager();
		HierarchyFilter hierarchyFilter =
		(HierarchyFilter)steps.createDimensionStep(DimensionStepTypeEnum.HIERARCHYFILTER);
		Collection hierarchies = prod.getHierarchy();
		LevelBasedHierarchy stdHierarchy = (LevelBasedHierarchy)
				hierarchies.iterator().next();
		hierarchyFilter.setHierarchy(stdHierarchy);
		hierarchyFilter.setSetAction(SetActionTypeEnum.INITIAL);
		hierarchyFilter.setHierarchyFilterType(HierarchyFilterTypeEnum.ALLMEMBERS);
		Drill drillW1000 = (Drill)
				steps.createDimensionStep(DimensionStepTypeEnum.DRILLFILTER);
		Member W1000 = memberObjectFactories.createMember(prod);
		drillW1000.setDrillMember(W1000);
		drillW1000.setSetAction(SetActionTypeEnum.APPEND);
		drillW1000.setDrillType(DrillTypeEnum.CHILDREN);

	}

	/**
	 * "Sample code 8.6: Data-based Exception Filter"
	 */
	public void testDataBasedExceptionFilter() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView ExceptionMembers = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		Dimension geog = (Dimension)someConnection.getDimensions().get(1);
		Dimension meas = (Dimension)someConnection.getDimensions().get(2);
		Dimension time = (Dimension)someConnection.getDimensions().get(3);
		ExceptionMembers.setDimension(prod);
		AttributeReference selectCode = (AttributeReference)
				ExceptionMembers.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTEREFERENCE);
		Attribute code = (Attribute)prod.getFeature().get(0);
		selectCode.setAttribute(code);
		DimensionStepManager steps = ExceptionMembers.createDimensionStepManager();
		ExceptionMemberFilter prodsGT500 = (ExceptionMemberFilter)
				steps.createDimensionStep(DimensionStepTypeEnum.EXCEPTIONMEMBERFILTER);
		prodsGT500.setSetAction(SetActionTypeEnum.INITIAL);
		prodsGT500.setOp(OperatorTypeEnum.EQ);
		prodsGT500.setRhs(new Integer(500));
		QualifiedMemberReference qmr1 = (QualifiedMemberReference)
				prodsGT500.createDataBasedMemberFilterInput(DataBasedMemberFilterInputTypeEnum.QUALIFIEDMEMBERREFERENCE);
		Member Sales = memberObjectFactories.createMember(meas);
		Member DavesStore = memberObjectFactories.createMember(geog);
		Member Jan2001 = memberObjectFactories.createMember(time);
		qmr1.addMember(Sales);
		qmr1.addMember(DavesStore);
		qmr1.addMember(Jan2001);
	}

	/**
	 * "Sample code 8.7: Data-based Ranking Filter"
	 */
	public void testDataBasedRankingFilter() throws OLAPException {
		Connection someConnection = getConnection();
		DimensionView RankingMembers = someConnection.createDimensionView();
		Dimension prod = (Dimension)someConnection.getDimensions().get(0);
		Dimension geog = (Dimension)someConnection.getDimensions().get(1);
		Dimension meas = (Dimension)someConnection.getDimensions().get(2);
		Dimension time = (Dimension)someConnection.getDimensions().get(3);
		RankingMembers.setDimension(prod);
		AttributeReference selectCode = (AttributeReference)
				RankingMembers.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTEREFERENCE);
		Attribute code = (Attribute)prod.getFeature().get(0);
		selectCode.setAttribute(code);
		DimensionStepManager steps =
		RankingMembers.createDimensionStepManager();
		RankingMemberFilter TB5Prods =
		(RankingMemberFilter)steps.createDimensionStep(DimensionStepTypeEnum.RANKINGMEMBERFILTER);
		TB5Prods.setSetAction(SetActionTypeEnum.INITIAL);
		TB5Prods.setType(RankingTypeEnum.TOPBOTTOM);
		TB5Prods.setTop(new Integer(5));
		TB5Prods.setBottom(new Integer(5));
		TB5Prods.setTopPercent(new Boolean(false));
		TB5Prods.setBottomPercent(new Boolean(false));
		QualifiedMemberReference qmr1 = (QualifiedMemberReference)
				TB5Prods.createDataBasedMemberFilterInput(DataBasedMemberFilterInputTypeEnum.QUALIFIEDMEMBERREFERENCE);
		Member Sales = memberObjectFactories.createMember(meas);
		Member DavesStore = memberObjectFactories.createMember(geog);
		Member Jan2001 = memberObjectFactories.createMember(time);
		qmr1.addMember(Sales);
		qmr1.addMember(DavesStore);
		qmr1.addMember(Jan2001);
	}

	/**
	 * Code fragment for section 9.2.3.2
	 */
	public void testFoo() throws OLAPException {
		// Create a DimensionView for each dimension
		Connection connection = getConnection();
		DimensionView channelView = connection.createDimensionView();
		channelView.setDimension(getDimension(connection, "Channel"));
		DimensionView productView = connection.createDimensionView();
		productView.setDimension(getDimension(connection, "Product"));
		DimensionView geographyView = connection.createDimensionView();
		geographyView.setDimension(getDimension(connection, "Geography"));
		DimensionView timeView = connection.createDimensionView();
		timeView.setDimension(getDimension(connection, "Time"));
		MeasureView measView = connection.createMeasureView();
		// jhyde added
		CubeView query = connection.createCubeView();
		// Create a columns edge and add the time and geography views
		//EdgeView columns = connection.createEdgeView();
		EdgeView columns = query.createOrdinateEdge();
		columns.addDimensionView(timeView);
		columns.addDimensionView(geographyView);
		// Create a rows edge and add the product view
		//EdgeView rows = connection.createEdgeView();
		EdgeView rows = query.createOrdinateEdge();
		rows.addDimensionView(productView);
 		// Create a pages edge and add the channel dimension view
		//EdgeCursor pages = connection.createDimensionView();
		EdgeView pages = query.createPageEdge();
		pages.addDimensionView(channelView);
		pages.addDimensionView(measView);
		// Create the query cube view and add edges and the measure
		//CubeView query = connection.createCubeView();
		//query.addOrdinateEdge(rows);
		//query.addOrdinateEdge(columns);
		//query.addPageEdge(pages);
		CubeCursor dataCursor = query.createCursor();
		//EdgeCursor pageCursor=(List)dataCursor.getPageEdge().elementAt(0);
		EdgeCursor pageCursor=(EdgeCursor) dataCursor.getPageEdge().iterator().next();
		//EdgeCursor rowCursor=(List)dataCursor.getOrdinateEdge().elementAt(0);
		EdgeCursor columnCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(0);
		EdgeCursor rowCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(1);
		DimensionCursor measureCursor = (DimensionCursor) pageCursor.getDimensionCursor().get(0);
		DimensionCursor channelCursor = (DimensionCursor) pageCursor.getDimensionCursor().get(1);
		DimensionCursor productCursor = (DimensionCursor) rowCursor.getDimensionCursor().get(0);
		DimensionCursor geographyCursor =(DimensionCursor) columnCursor.getDimensionCursor().get(0);
		DimensionCursor timeCursor = (DimensionCursor) columnCursor.getDimensionCursor().get(1);
		columnCursor.setFetchSize(6);
		rowCursor.setFetchSize(4);
	}

	private Dimension getDimension(Connection connection, String s) throws OLAPException {
		for (Iterator iterator = connection.getDimensions().iterator(); iterator.hasNext();) {
			Dimension dimension = (Dimension) iterator.next();
			if (dimension.getName().equals(s)) {
				return dimension;
			}
		}
		return null;
	}
}

// End JolapTest.java