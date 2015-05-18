package cubecrud;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import mondrian.olap.CacheControl;
import mondrian.olap.CacheControl.CellRegion;
import mondrian.olap.Cube;
import mondrian.rolap.RolapSchema;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 *
 * This class provides a REST API for cube management in the Mondrian  
 * and also has functionality to invalidate the cache in Mondrian after
 * data change in database.
 * 
 */

/** 
 *
 * @author SHazra
 * 
 */


@Path("/")
public class RequestProcessEngine {	
	private static final org.apache.log4j.Logger LOGGER = Logger.getLogger(RequestProcessEngine.class);
	final static String DATASOURCE_PATH = System.getProperty("user.dir") +
			"/webapps/mondrian/WEB-INF/datasources.xml";	
    String result= "";
    static boolean isPresentCubeHand= false;
    static boolean isPresentCatalogHand= false;
    static String catalogFileForAdd = "";
	
	
	@Path("/catalog/{c}")
	@GET
	@Produces("application/xml")
	public String getCatalogs(@PathParam("c") String catalogName) {
		String result = getCube(DATASOURCE_PATH, "", catalogName);
		LOGGER.info(result);
		return result;
	}
	
	@Path("/cube/{c}/{d}")
	@GET
	@Produces("application/xml")
	public String getCubes(@PathParam("c") String catalogName, @PathParam("d") String cubeName) {
		String result = getCube(DATASOURCE_PATH, cubeName, catalogName);
		LOGGER.info(result);
		return result;
	}
	
	@Path("/cube/{c}")
	@GET
	@Produces("application/xml")
	public String getCubes(@PathParam("c") String cubeName) {
		String result = getCube(DATASOURCE_PATH, cubeName, "");
		LOGGER.info(result);
		return result;
	}
	
	@Path("/cubes")
	@GET
	@Produces("application/xml")
	public String getAllCubes() {
		System.out.println("Datasource = " + DATASOURCE_PATH);
		System.out.println("Current user dir is =" + System.getProperty("user.dir"));
		String result = getCube(DATASOURCE_PATH, "", "");		
		LOGGER.info(result);
		LOGGER.info("user dir=" + System.getProperty("user.dir"));
		return result;
	}
	
		
	@Path("/deletecube/{c}/{d}")
	@DELETE
	@Produces("application/xml")
	public String deleteTask(@PathParam("c") String catalogName, @PathParam("d") String cubeName) {
		String result = "";
		try{
			result = deleteCube(cubeName, catalogName) ? "Cube successfully deleted" : "Deletion failed";			
		}catch (Exception e)
		{
			result = e.getMessage();
		}
		
		result = "<output>" + result + "</output>";
		LOGGER.info(result);
		return result;
	}
	
	
	@Path("/putcube/{c}/{d}")
	@PUT
	@Produces("application/xml")
	@Consumes("text/plain")
	public String putTask(@PathParam("c") String catalogName, @PathParam("d") String cubeName, String inputXml) {
				
		LOGGER.info("Input xml---" + inputXml);
		String result = "<output>"+ addCube(inputXml, catalogName, cubeName) +"</output>";
		LOGGER.info("response xml = " + result);
		return result;
	}
	
	@Path("/invalidatecache/catalog/{c}")
	@PUT
	@Produces("application/xml")
	@Consumes("text/plain")
	public String invalidateCacheCatalog(@PathParam("c") String catalogName) throws SQLException {
    	boolean isCatalogFound = false;
		try {
		System.out.println("Inside invalidate cache with catalog name=" + catalogName);
		for (RolapSchema schema : RolapSchema.getRolapSchemas()) {
            if (schema.getName().equals(catalogName)) {
            	isCatalogFound = true;
            	LOGGER.debug("schema is same as catalog and is flushing the schema");
                schema.getInternalConnection().getCacheControl(null)
                    .flushSchemaCache();                 
            }
        }
		if (!isCatalogFound)
		{
			return "<output>Catalog " + catalogName + " was not found </output>";
		}

     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.error("Error in RequestProcessEngine | invalidateCache " + e.getMessage());
			e.printStackTrace();			
			return "<output>Error in clearing cache for catalog " + catalogName + " | " + e.getMessage() 
					+ "</output>";
		}
		return "<output>Cache clearance for Catalog " + catalogName + " is successful</output>";
	}
	
	@Path("/invalidatecache/cube/{c}")
	@PUT
	@Produces("application/xml")
	@Consumes("text/plain")
	public String invalidateCacheCube(@PathParam("c") String cubeName) throws SQLException {
    	// Warning: Leads to inconsistency in the view if the atomicity of the flush and DB update
		// are not taken in to account
		boolean isCubePresent = false;
		try {		
		for (RolapSchema schema : RolapSchema.getRolapSchemas()) {
        	Cube [] cubeArr = schema.getCubes();
        	CacheControl cacheControlObj = schema.getInternalConnection().getCacheControl(null);
          for (Cube cube : cubeArr)
          {
        	  if (cube.getName().equals(cubeName)) {
        		isCubePresent = true;
              	LOGGER.debug("cube entered is same as cube and is flushing the cube");                
                cacheControlObj.flush(cacheControlObj.createMeasuresRegion(cube));                
              }
          }            
        }
		if (!isCubePresent)
		{
			return "<output>Cube " + cubeName + " was not found </output>";
		}

     
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.error("Error in RequestProcessEngine | invalidateCache " + e.getMessage());
			e.printStackTrace();			
			return "<output>Error in clearing cache for cube " + cubeName + " | " + e.getMessage() 
					+ "</output>";
		}
		return "<output>Cache clearance for Cube " + cubeName + " is successful</output>";
	}
	
	
	private static String parseCubeXml(String fileName, String cubeName){
        StreamResult result = null;
        StringWriter sw = new StringWriter();
        Document doc = null;
        NodeList nodeList = null;
        int count = 0;
        try{
        	ArrayList<Object> arrList= getNodeList(fileName, "Schema/Cube", true);        	
    		for (Object o: arrList)
    		{
    			if (o instanceof Document)
    			{
    				doc = (Document) o;
    			}
    			else if (o instanceof NodeList)
    			{
    				nodeList = (NodeList) o;
    			}
    		}
            TransformerFactory tFactory = TransformerFactory.newInstance();
            Transformer transformer = tFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            result = new StreamResult(sw);

            DOMSource source = null;
            for(int x = 0;x < nodeList.getLength();x++)
            {
                Node e = nodeList.item(x);
                NamedNodeMap attrs = e.getAttributes();   
                if ("".equalsIgnoreCase(cubeName)) {
					if (e.getNodeType() == Node.ELEMENT_NODE) {
						source = new DOMSource(e);
						transformer.transform(source, result);
						count = count + 1;
					}
				}
                else
                {
                	if (attrs.getNamedItem("name") != null)
                	{
                		String nameToMatch = attrs.getNamedItem("name").getNodeValue();
                		if (nameToMatch.equals(cubeName))
                		{
                			source = new DOMSource(e);
    						transformer.transform(source, result);
    						break;
                		}
                	}
                	
                }
            }
            LOGGER.info("Total cubes in the file =" + count);

        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return sw.toString();
    }
    
	private static ArrayList<Object> getNodeList(String textXml, String xpathStr, boolean isFilename)
    {
    	ArrayList<Object> resArrList = new ArrayList<Object>();
    	Document doc = null;
    	NodeList nl = null;
    	try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			if (isFilename){
				doc = builder.parse(new File(textXml));
			}
			else
			{
				InputSource sourceXML = new InputSource(new StringReader(textXml));
			    doc = builder.parse(sourceXML);
			}
			 
			doc.getDocumentElement().normalize();

			TransformerFactory tFactory = TransformerFactory.newInstance();
			Transformer transformer = tFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			XPath xpath = XPathFactory.newInstance().newXPath();
			XPathExpression expr = xpath.compile(xpathStr);
			Object exprResult = expr.evaluate(doc, XPathConstants.NODESET);
			nl = (NodeList) exprResult;
			resArrList.add(nl);
			resArrList.add(doc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return resArrList;
    }
	
	
	private String getCube(String fileName, final String cubeNameToSearch, final String catalogNameToSearch){
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            boolean searchForCatalog = false;
            DefaultHandler handler = new DefaultHandler() {
                boolean bCatalog = false;
                boolean bDefinition = false;
                boolean bDataSourceInfo = false;
                boolean isGlobalDataSourcePresent = false;
                StringBuilder globalDataSourceInfo = new StringBuilder(); // Had to use this as sax parser escapes the special strings and visits each part of the string.
                StringBuilder localDataSourceInfo = new StringBuilder();
                String catalogName = "";
                public void startElement(String uri, String localName,String qName,
                                         Attributes attributes) throws SAXException {
                    if (qName.equalsIgnoreCase("Catalog")) {
                        catalogName = attributes.getValue("name");
                        LOGGER.info("catalog names =" + catalogName);
                        LOGGER.info("Attributes = " + attributes.getValue("name"));
                        if (catalogNameToSearch == null || "".equalsIgnoreCase(catalogNameToSearch))
                        {
                            bCatalog = true;
                        }
                        else if (catalogNameToSearch.equalsIgnoreCase(catalogName))
                        {
                            bCatalog = true;
                        }
                    }
                    if (qName.equalsIgnoreCase("Definition")) {
                        bDefinition = true;
                    }
                    if (qName.equalsIgnoreCase("DataSourceInfo")) {
                        bDataSourceInfo = true;
                    }

                }
                public void endElement(String uri, String localName,
                                       String qName) throws SAXException {
                    if (qName.equalsIgnoreCase("DataSourceInfo")) {
                    	 bDataSourceInfo = false;
                         if ("".equalsIgnoreCase(catalogName)){ // Checks whether it is a global datasourceinfo or not as ...if it is a global none of the catalogs are reached hence the name will be empty.

                    		isGlobalDataSourcePresent = true;
                    	}   
                    }

                    if (qName.equalsIgnoreCase("Catalog")) {
                        bCatalog = false;
                        LOGGER.info("Before deleting ....datasourceinfo =" + localDataSourceInfo);
                        if ( localDataSourceInfo.length() > 0 )
                        {
                            localDataSourceInfo = localDataSourceInfo.delete(0, localDataSourceInfo.length());
                        }
                    }

                    if (qName.equalsIgnoreCase("Definition")) {
                        bDefinition = false;
                    }
                }
                public void characters(char ch[], int start, int length) throws SAXException {
                    if (bCatalog) {
                        String tempStr = new String(ch, start, length);
                        tempStr = tempStr.replaceAll("\n", "").trim();
                    }
                    if (bDefinition){
                        if (catalogNameToSearch != null && !"".equalsIgnoreCase(catalogNameToSearch))
                        {
                            if (bCatalog) {
                                String defStr = new String(ch, start, length);
                                defStr = defStr.replaceAll("\n", "").trim();
                                LOGGER.info("Definition : " + defStr);
                                String tempOutput = "";
                                try {
                                    if ("".equalsIgnoreCase(cubeNameToSearch)) {
                                        tempOutput = parseCubeXml(defStr, "");
                                    } else {
                                        tempOutput = parseCubeXml(defStr,
                                                cubeNameToSearch);
                                    }
                                    if (!"".equalsIgnoreCase(tempOutput)) {
                                    	if (localDataSourceInfo.length() != 0){
                                            tempOutput = "<Catalog name=\""
                                                    + catalogName + "\" datasourceinfo=\"" + localDataSourceInfo.toString().replaceAll("&", "&#38;") + "\">"
                                                    + tempOutput + "</Catalog>";
                                        }
                                        else
                                        {
                                            tempOutput = "<Catalog name=\""
                                                    + catalogName + "\" datasourceinfo=\"" + globalDataSourceInfo.toString().replaceAll("&", "&#38;") + "\">"
                                                    + tempOutput + "</Catalog>";
                                        }
                                    }
                                    result = result + tempOutput;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        else{
                            String defStr = new String(ch, start, length);
                            defStr = defStr.replaceAll("\n", "").trim();
                            LOGGER.info("Definition : " + defStr);
                            String tempOutput = "";
                            try {
                                if ("".equalsIgnoreCase(cubeNameToSearch)) {
                                    tempOutput = parseCubeXml(defStr, "");
                                } else {
                                    tempOutput = parseCubeXml(defStr,
                                            cubeNameToSearch);
                                }
                                if (!"".equalsIgnoreCase(tempOutput)) {
                                	if (localDataSourceInfo.length() != 0){
                                        tempOutput = "<Catalog name=\""
                                                + catalogName + "\" datasourceinfo=\"" + localDataSourceInfo.toString().replaceAll("&", "&#38;") + "\">"
                                                + tempOutput + "</Catalog>";
                                    }
                                    else
                                    {
                                        tempOutput = "<Catalog name=\""
                                                + catalogName + "\" datasourceinfo=\"" + globalDataSourceInfo.toString().replaceAll("&", "&#38;") + "\">"
                                                + tempOutput + "</Catalog>";
                                    }
                                }
                                result = result + tempOutput;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }


                    if (bDataSourceInfo){
                        if (bCatalog && isGlobalDataSourcePresent){
                            String localDataSourceInfoTemp = new String(ch, start, length);
                            localDataSourceInfoTemp = localDataSourceInfoTemp.replaceAll("\n", "").trim();
                            localDataSourceInfo.append(localDataSourceInfoTemp);
                            System.out.println("local......." + localDataSourceInfo);
                        }
                        else if (!isGlobalDataSourcePresent){
                            String globalDataSourceInfoTemp = new String(ch, start, length);
                            globalDataSourceInfoTemp = globalDataSourceInfoTemp.replaceAll("\n", "").trim();
                            globalDataSourceInfo.append(globalDataSourceInfoTemp);
                            System.out.println("global......." + globalDataSourceInfo);
                        }
                    }

                }
            };
            saxParser.parse(fileName, handler);
        } catch (Exception e) {
            e.printStackTrace();
        }
        result = wrapperOutput(result);
        return result;
        }
	
	 private static String wrapperOutput(String output){
	    	String finalResult = "";
	    	if ("".equalsIgnoreCase(output)){
	    		finalResult = "<output>No Cubes found.</output>";
	    	}
	    	else
	    	{
	    		finalResult = "<output>" + output + "</output>";
	    	}
	    	return finalResult;
	    }
	    
	 
	 private String addCube(String inputXml, String catalogName, String cubeName){
	    	String result = "";
	    	StreamResult resultStream = null;
	        StringWriter sw = new StringWriter();
	    	DOMSource source = null;
	    	Document doc = null;
	        NodeList nodeList = null;
	        boolean isCubePresent = false;
	        boolean isDataSourcePresent = false;
	        
	        try {
	        	ArrayList<Object> arrList= getNodeList(inputXml, "CubeDefinition", false);        	
	        	for (Object o: arrList)
	        	{
	        		if (o instanceof Document)
	        		{
	        			doc = (Document) o;
	        		}
	        		else if (o instanceof NodeList)
	        		{
	        			nodeList = (NodeList) o;
	        		}
	        	}
				for (int x = 0; x < nodeList.getLength(); x++) {
					Node e = nodeList.item(x);
					NodeList nl = e.getChildNodes();
					String dataSourceInfo = "";
					String cubeInfo = "";
					System.out.println(" Length of the node list =" + nl.getLength());
					if (nl.getLength() >= 2)
					{
						for (int i = 0; i < nl.getLength(); i++)
						{
							Node ni = nl.item(i);
														
							if (ni.getNodeName().equalsIgnoreCase("DataSource"))
							{								
								dataSourceInfo = ni.getTextContent();
								if (dataSourceInfo == null || "".equalsIgnoreCase(dataSourceInfo))
								{
									return "Invalid Input Request | DataSourceInfo is missing";
								}
								else{
									LOGGER.info("Datasourceinfo =" + dataSourceInfo);
								}
								isDataSourcePresent = true;
							}
							else if (ni.getNodeName().equalsIgnoreCase("Cube"))
							{
								NamedNodeMap nmapAttr = ni.getAttributes();
					 			String cubeNameFromXml = nmapAttr.getNamedItem("name").getNodeValue();
								if ("".equalsIgnoreCase(cubeNameFromXml))
								{
									return "Invalid Input Request | Cube name is missing";
								}
								else if (!cubeName.equalsIgnoreCase(cubeNameFromXml))
								{
									return "Invalid Input Request | Cube name in the URL and xml do not match";
								}
								TransformerFactory tFactory = TransformerFactory.newInstance();
								Transformer transformer = tFactory.newTransformer();
								transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
										"yes");
								source = new DOMSource(ni);
								resultStream = new StreamResult(sw);
								transformer.transform(source, resultStream);	
								cubeInfo = sw.toString();
								isCubePresent = true;
							}
						}
						
						if (!isCubePresent || !isDataSourcePresent)
						{
							return "Invalid Input Request | Elements: DataSource and Cube are mandatory";
						}
						
						String isPresentStr = isCubePresent(cubeName, catalogName);
						String isPresentStrArr[] = isPresentStr.split("\\|");
						String isCubeStr = isPresentStrArr[0];
						String isCatalogStr = isPresentStrArr[1];
						LOGGER.info( "result ="+ isCubeStr + isCatalogStr);
						if("true".equals(isCubeStr) && "true".equals(isCatalogStr))
						{
							overwriteCatalogDefinition(cubeName, sw.toString());
							LOGGER.info("Cube and catalog are present");
							result = "Cube modified successfully";
						}
						else if ("true".equals(isCatalogStr) && "false".equals(isCubeStr))
						{
							appendCatalogDefinition(cubeName, sw.toString());
							LOGGER.info("Catalog present but the Cube is not.");
							result = "Cube is successfully added to an existing catalog.";
						}
						else{
							LOGGER.info("Both catalog and cube does not exist.");
							String catalogDefFileName = addCatalogInDataSource(catalogName, dataSourceInfo);
							createCatalogDefinition(catalogName, catalogDefFileName, cubeInfo);
							result = "Cube and catalog are successfully added.";							
						}
						isPresentCubeHand = false;
						isPresentCubeHand = false;
					}
					else {
						return "Invalid Input Request | Elements: DataSource and Cube are mandatory";
					}
				}
				
	        } catch (Exception e) {
	            e.printStackTrace();
	            result = e.getMessage();
	        }        

	        return result;
	    }
	    
	    private static boolean createCatalogDefinition(String catalogName, String catalogFileName, String cubeInfo)
	    {
	    	boolean succCreation = false;
	    	try{
	    		cubeInfo = "<Schema name=\"" + catalogName + "\">" + cubeInfo + "</Schema>";
	    		createFile(catalogFileName, cubeInfo);
	    		succCreation = true;
	    		
	    	} catch (Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    	
	    	return succCreation;
	    }
	    
	    private static String addCatalogInDataSource(String catalogName, String dataSourceInfo) throws Exception
	    {
	    	String userDir = System.getProperty("user.dir");
	    	String userDirArr[] = userDir.split("/");	  
	    	String dirCatalogFiles = "/";
			for (String folderName : userDirArr)
			{
				if (folderName != null && !"".equalsIgnoreCase(folderName))
				{
					dirCatalogFiles= dirCatalogFiles + folderName + "/";
					if (folderName.contains("apache"))
					{
						break;
					}					
				}
			}
			
			dirCatalogFiles = dirCatalogFiles + "webapps/mondrian/WEB-INF/queries/";
			
			
	    	String newCatalogDefFileName = dirCatalogFiles + catalogName + ".xml"; 
	    	Document doc = null;
	    	NodeList nodeList = null;
	    	try {	    	
	    		dataSourceInfo = dataSourceInfo.trim() + "Catalog=file:/" + newCatalogDefFileName + ";";
				
	    		ArrayList<Object> arrList= getNodeList(DATASOURCE_PATH, "DataSources/DataSource/Catalogs", true);        	
	        	for (Object o: arrList)
	        	{
	        		if (o instanceof Document)
	        		{
	        			doc = (Document) o;
	        		}
	        		else if (o instanceof NodeList)
	        		{
	        			nodeList = (NodeList) o;
	        		}
	        	}
	        	
				if (nodeList.getLength() > 0)
				{
					Node catalogsNode = nodeList.item(0);
					String xmlForCatalog ="<Catalog name=\""+ catalogName +"\"><DataSourceInfo>" + 
					dataSourceInfo.replaceAll("&", "&#38;") + "</DataSourceInfo><Definition>"
							+ newCatalogDefFileName + "</Definition></Catalog>";
					
					Element e1 = createDOM(xmlForCatalog);
					
					Node importedNode = doc.importNode(e1, true);
					catalogsNode.appendChild(importedNode);
					
				}
				
				StreamResult resultStreamNew = new StreamResult(new StringWriter());
				TransformerFactory tFactoryNew = TransformerFactory.newInstance();
				Transformer transformerNew = tFactoryNew.newTransformer();
				transformerNew.setOutputProperty("indent", "yes");
				transformerNew.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
							"yes");
				DOMSource sourceNew = new DOMSource(doc);
				transformerNew.transform(sourceNew, resultStreamNew);
				String finalXml = resultStreamNew.getWriter().toString();
				deleteFile(DATASOURCE_PATH);
				createFile(DATASOURCE_PATH, finalXml);
				
	    	}catch (Exception e)
	    	{
	    		e.printStackTrace();
	    		throw e;
	    	}
	    	return newCatalogDefFileName;
	    }
	    
	    private static boolean appendCatalogDefinition(String cubeName, String cubeInfo)
	    {
	    	boolean isSucess = false;
	    	Document doc= null;
	    	NodeList nodeList = null;
	    	LOGGER.info("Inside appendCatalogDefinition with cubeinfo="+ catalogFileForAdd);
	    	try {
	    		ArrayList<Object> arrList= getNodeList(catalogFileForAdd, "Schema", true);        	
	        	for (Object o: arrList)
	        	{
	        		if (o instanceof Document)
	        		{
	        			doc = (Document) o;
	        		}
	        		else if (o instanceof NodeList)
	        		{
	        			nodeList = (NodeList) o;
	        		}
	        	}				
				if (nodeList.getLength()==1) {
					NodeList nl = nodeList.item(0).getChildNodes();
					if (nl.getLength() > 0) {
						Node n = nl.item(0);
						Node parent = n.getParentNode();
						Element e1 = createDOM(cubeInfo);
						Node importedNode = doc.importNode(e1, true);
						parent.appendChild(importedNode);
						isSucess = true;
					}
					StreamResult resultStreamNew = new StreamResult(
							new StringWriter());
					TransformerFactory tFactoryNew = TransformerFactory
							.newInstance();
					Transformer transformerNew = tFactoryNew.newTransformer();
					transformerNew.setOutputProperty(
							OutputKeys.OMIT_XML_DECLARATION, "yes");
					DOMSource sourceNew = new DOMSource(doc);
					transformerNew.transform(sourceNew, resultStreamNew);
					String finalXml = resultStreamNew.getWriter().toString();
					LOGGER.info("final XML-----------" + finalXml);
					deleteFile(catalogFileForAdd);
					createFile(catalogFileForAdd, finalXml);
					catalogFileForAdd = "";
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	return isSucess;
	    }
	    
		private static boolean overwriteCatalogDefinition(String cubeName, String cubeInfo)
	    {
	    	boolean isSucess = false;
	    	NodeList nodeList = null;
	    	Document doc = null;
	    	LOGGER.info("Inside overwriteCatalogDefinition with cubeinfo="+ cubeInfo);
	    	try {
	    		ArrayList<Object> arrList= getNodeList(catalogFileForAdd, "CubeDefinition", true);        	
	        	for (Object o: arrList)
	        	{
	        		if (o instanceof Document)
	        		{
	        			doc = (Document) o;
	        		}
	        		else if (o instanceof NodeList)
	        		{
	        			nodeList = (NodeList) o;
	        		}
	        	}
				
				NodeList nl = doc.getElementsByTagName("Cube");
				
				for (int x = 0;x < nl.getLength();x++)
				{
					Node e = nl.item(x);
					NamedNodeMap attrs = e.getAttributes();       
					String nameToMatch = attrs.getNamedItem("name").getNodeValue();
	        		
					if (cubeName.equalsIgnoreCase(nameToMatch))
					{						
						Node parent = e.getParentNode();
						parent.removeChild(e);
						Element e1 = createDOM(cubeInfo);						
						Node importedNode = doc.importNode(e1, true);
						parent.appendChild(importedNode);
						isSucess = true;
						break;
					}
				}
				
				StreamResult resultStreamNew = new StreamResult(new StringWriter());
				TransformerFactory tFactoryNew = TransformerFactory.newInstance();
				Transformer transformerNew = tFactoryNew.newTransformer();
				transformerNew.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
							"yes");
				DOMSource sourceNew = new DOMSource(doc);
				transformerNew.transform(sourceNew, resultStreamNew);
				String finalXml = resultStreamNew.getWriter().toString();
				
				LOGGER.info("final XML-----------" + finalXml);
				deleteFile(catalogFileForAdd);
				createFile(catalogFileForAdd, finalXml);
				catalogFileForAdd = "";
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	return isSucess;
	    }
	    
		public static final Element createDOM(String strXML) 
			    throws ParserConfigurationException, SAXException, IOException {
			    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			    DocumentBuilder db = dbf.newDocumentBuilder();
			    InputSource sourceXML = new InputSource(new StringReader(strXML));
			    Document xmlDoc = db.parse(sourceXML);
			    Element e = xmlDoc.getDocumentElement();			    
			    e.normalize();
			    return e;
			}
	    
	    private String isCubePresent(String cubeName, String catalogName){
	    	LOGGER.info("Inside in isCubePresent");
	    	String isPresent = "false|false";
	    	isPresentCubeHand = false;
	    	isPresentCatalogHand = false;
	    	
	    	final String catalogNameTemp = catalogName;
	    	final String cubeNameTemp = cubeName;
	    	try {

	            SAXParserFactory factory = SAXParserFactory.newInstance();
	            SAXParser saxParser = factory.newSAXParser();
	            
	            DefaultHandler handler = new DefaultHandler() {

	                boolean bCatalog = false;
	                boolean bDefinition = false;
	                String catalogName = "";
	                public void startElement(String uri, String localName,String qName,
	                                         Attributes attributes) throws SAXException {

	                    if (qName.equalsIgnoreCase("Catalog")) {
	                        catalogName = attributes.getValue("name");
	                        if (catalogName.equalsIgnoreCase(catalogNameTemp))
	                        {
	                        	LOGGER.info("Attributes = " + attributes.getValue("name"));
	                        	isPresentCatalogHand = true;
	                            bCatalog = true;
	                        }                        
	                    }
	                    if (qName.equalsIgnoreCase("Definition") && bCatalog) {
	                        bDefinition = true;
	                    }
	                }

	                public void endElement(String uri, String localName,
	                                       String qName) throws SAXException {
	                }

	                public void characters(char ch[], int start, int length) throws SAXException {

	                    if (bDefinition){
	                        String defStr = new String(ch, start, length);
	                        defStr = defStr.replaceAll("\n", "").trim();
	                        LOGGER.info("Definition : " + defStr);
	                        String tempOutput = parseCubeXml(defStr, cubeNameTemp);
	                        if (!"".equalsIgnoreCase(tempOutput))
	                        {
	                        	LOGGER.info("Cube exists in Inside");                        	
	                        	isPresentCubeHand = true;
	                        }
	                        catalogFileForAdd = defStr;
	                        bDefinition = false;
	                        bCatalog = false;
	                    }
	                    
	                }

	            };           
	            saxParser.parse(DATASOURCE_PATH, handler);
	            LOGGER.info("Value of isPresentCubeHand =" + isPresentCubeHand);
	            LOGGER.info("Value of isPresentCatalogHand =" + isPresentCatalogHand);
	            if (isPresentCubeHand && isPresentCatalogHand)
	            {
	            	isPresent = "true|true";            	
	            }
	            else if (!isPresentCubeHand && isPresentCatalogHand)
	            {
	            	isPresent = "false|true"; 
	            }
	            else 
	            {
	            	isPresent = "false|false"; 
	            }

	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	      
	    	return isPresent;
	    }

	    private static boolean createFile(String fileName, String contents) throws Exception
	    {
	    	boolean createSucc = false;
	    	BufferedWriter bw = null;
	    	try{    	
	    		bw = new BufferedWriter(new FileWriter(new File(fileName)));
	    		bw.write(contents);
	    		bw.flush();
	    		createSucc = true;    		
	    	}catch (Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    	finally{
	    		if (bw != null)
	    		{
	    			bw.close();
	    		}	    		
	    	}
	    	return createSucc;
	    }
	    
	    private boolean deleteCube(String cubeName, String catalogName) throws Exception
	    {
	    	StreamResult resultStream = null;
			StringWriter sw = new StringWriter();
			String cubeNameFromXml ="";
	    	boolean deleteSucc = false;
	    	Document doc = null;
	    	NodeList nodeList = null;
	    	String resStr = isCubePresent(cubeName,catalogName);
	    	String resStrArr [] = resStr.split("\\|");
	    	LOGGER.info("result from iscubepresent = " + resStr);
	    	if ("false".equalsIgnoreCase(resStrArr[1]))
	    	{
	    		throw new Exception("The catalog could not be found");
	    	}
	    	else if ("false".equalsIgnoreCase(resStrArr[0]) && "true".equalsIgnoreCase(resStrArr[1]))
	    	{
	    		throw new Exception("The cube could not be found");
	    	}
	    	
	        String catalogFileName = catalogFileForAdd;
	        catalogFileForAdd = "";
			try {
				
				ArrayList<Object> arrList= getNodeList(catalogFileName, "Schema", true);        	
	    		for (Object o: arrList)
	    		{
	    			if (o instanceof Document)
	    			{
	    				doc = (Document) o;
	    			}
	    			else if (o instanceof NodeList)
	    			{
	    				nodeList = (NodeList) o;
	    			}
	    		}
		        resultStream = new StreamResult(sw);
		        LOGGER.info("Length for schema length=" + nodeList.getLength());
			for (int x = 0; x < nodeList.getLength(); x++) {
				Node e = nodeList.item(x);
				NodeList nl = e.getChildNodes();
				LOGGER.info("Length of the list =" + nl.getLength());
				for (int i = 0; i < nl.getLength(); i++)
				{
					Node n = nl.item(i);
					NamedNodeMap nmapAttr = n.getAttributes();
					if ("Cube".equalsIgnoreCase(n.getNodeName()))
					{
						cubeNameFromXml= nmapAttr.getNamedItem("name").getNodeValue();
						
						
						if (cubeName.equalsIgnoreCase(cubeNameFromXml))
						{
							e.removeChild(n);
							TransformerFactory tFactory = TransformerFactory.newInstance();
							Transformer transformer = tFactory.newTransformer();							
							transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
									"yes");
							DOMSource sourceNew = new DOMSource(doc);
							transformer.transform(sourceNew, resultStream);
							String finalXml = resultStream.getWriter().toString();
							LOGGER.info(finalXml);
							
							NodeList cubesList = e.getChildNodes();
							boolean anyMoreCubesPresent = false;
							for (int j = 0; j < nl.getLength(); j++)
							{
								Node nlNode = nl.item(j);
								if (nlNode.getNodeName().equalsIgnoreCase("Cube"))
								{
									anyMoreCubesPresent = true;
									break;
								}
							}
							if (anyMoreCubesPresent)
							{
								deleteFile(catalogFileName);
								createFile(catalogFileName, finalXml);
							}
							else
							{
								deleteFile(catalogFileName);
								deleteCatalogFromDatasource(catalogName);
							}
							deleteSucc = true;
							break;
							
						}
					}
				}
			}
	    	}catch (Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    	
	    	return deleteSucc;
	    }
	    
	    private boolean deleteCatalogFromDatasource(String catalogName)
	    {
	    	boolean delSucc = false;
	    	Document doc = null;
	    	NodeList nodeList = null;
	    	try{
	    		ArrayList<Object> arrList= getNodeList(DATASOURCE_PATH, "DataSources/DataSource/Catalogs/Catalog", true);        	
	    		for (Object o: arrList)
	    		{
	    			if (o instanceof Document)
	    			{
	    				doc = (Document) o;
	    			}
	    			else if (o instanceof NodeList)
	    			{
	    				nodeList = (NodeList) o;
	    			}
	    		}
	    		
	        	if (nodeList.getLength() > 0)
	        	{
	        		for (int x=0; x<nodeList.getLength();x++)
	        		{
	        			Node n = nodeList.item(x);
	        			NamedNodeMap nmap = n.getAttributes();
	        			String catalogNameFromXml = nmap.getNamedItem("name").getNodeValue();
	        			if (catalogName.equalsIgnoreCase(catalogNameFromXml))
	        			{
	        				Node parent = n.getParentNode();
	        				parent.removeChild(n);
	        				break;
	        			}
	        		}
	        	}

	        	StreamResult resultStreamNew = new StreamResult(new StringWriter());
	        	TransformerFactory tFactoryNew = TransformerFactory.newInstance();
	        	Transformer transformerNew = tFactoryNew.newTransformer();
	        	transformerNew.setOutputProperty("indent", "yes");
	        	transformerNew.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
	        			"yes");
	        	DOMSource sourceNew = new DOMSource(doc);
	        	transformerNew.transform(sourceNew, resultStreamNew);
	        	String finalXml = resultStreamNew.getWriter().toString();
	        	deleteFile(DATASOURCE_PATH);
	        	createFile(DATASOURCE_PATH, finalXml);    			
	        	
	    	}catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    	return delSucc;
	    }
	    
	    private static boolean deleteFile(String fileName) throws Exception
	    {
	    	boolean deleteSucc = false;
	    	try{    	
	    	
	    		File file = new File(fileName);
	    		file.delete();
	    		deleteSucc = true;
	    		
	    	}catch (Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    	return deleteSucc;
	    }
	    
}


