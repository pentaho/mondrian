// This class is generated. Do NOT modify it, or
// add it to source control or the J++ project.

package mondrian.olap;

import java.util.Locale;

/**
 * This class was generated
 * by class mondrian.resource.ResourceGen
 * from Resources/MdxResource_en.xml
 * on Fri Feb 08 02:24:28 PST 2002.
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 **/

public class MondrianResource extends mondrian.resource.ResourceBase implements mondrian.resource.Resource
{
	public MondrianResource() {}
	public MondrianResource(java.net.URL url, java.util.Locale locale) throws java.io.IOException {
		init(url, locale);
	}
	public MondrianResource(String base, java.util.Locale locale) throws java.io.IOException {
		init(
			new java.net.URL(base + "/MondrianResource_" + locale + ".xml"),
			locale);
	}
	// begin of included code
// implement ResourceBase
	public Error newInternalError(String s) {
		return newInternal(s);
	}
	public Error newInternalError(Throwable e, String s) {
		return newInternal(e, s);
	}
	private static MondrianResource instance;
	public static synchronized MondrianResource instance() {
		if (instance == null) {
			try {
				String resourceURL = System.getProperty(
					"mondrian.resourceURL");
				instance = new MondrianResource(resourceURL, Locale.ENGLISH);
			} catch (java.io.IOException e) {
				throw new Error("Exception while initializing resource: " + e);
			}
		}
		return instance;
	}
	// end of included code
	/** e: Internal error: %1 */
	public static final int Internal = 320000;
	public String getInternal(String p0) {
		return formatError(320000, new Object[] {p0});
	}
	public mondrian.resource.Error newInternal(String p0) {
		return new mondrian.resource.Error(null, this, Internal, new Object[] {p0});
	}
	public mondrian.resource.Error newInternal(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, Internal, new Object[] {p0});
	}
	/** e: MDX cube '%1' not found or not processed */
	public static final int MdxCubeNotFound = 321000;
	public String getMdxCubeNotFound(String p0) {
		return formatError(321000, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCubeNotFound(String p0) {
		return new mondrian.resource.Error(null, this, MdxCubeNotFound, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCubeNotFound(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxCubeNotFound, new Object[] {p0});
	}
	/** e: MDX object '%1' not found in %2 */
	public static final int MdxChildObjectNotFound = 321010;
	public String getMdxChildObjectNotFound(String p0, String p1) {
		return formatError(321010, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxChildObjectNotFound(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxChildObjectNotFound, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxChildObjectNotFound(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxChildObjectNotFound, new Object[] {p0, p1});
	}
	/** i: cube '%1' */
	public static final int MdxCubeName = 322000;
	public String getMdxCubeName(String p0) {
		return formatError(322000, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCubeName(String p0) {
		return new mondrian.resource.Error(null, this, MdxCubeName, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCubeName(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxCubeName, new Object[] {p0});
	}
	/** i: hierarchy '%1' */
	public static final int MdxHierarchyName = 322010;
	public String getMdxHierarchyName(String p0) {
		return formatError(322010, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxHierarchyName(String p0) {
		return new mondrian.resource.Error(null, this, MdxHierarchyName, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxHierarchyName(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxHierarchyName, new Object[] {p0});
	}
	/** i: dimension '%1' */
	public static final int MdxDimensionName = 322020;
	public String getMdxDimensionName(String p0) {
		return formatError(322020, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxDimensionName(String p0) {
		return new mondrian.resource.Error(null, this, MdxDimensionName, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxDimensionName(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxDimensionName, new Object[] {p0});
	}
	/** i: level '%1' */
	public static final int MdxLevelName = 322030;
	public String getMdxLevelName(String p0) {
		return formatError(322030, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxLevelName(String p0) {
		return new mondrian.resource.Error(null, this, MdxLevelName, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxLevelName(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxLevelName, new Object[] {p0});
	}
	/** i: member '%1' */
	public static final int MdxMemberName = 322040;
	public String getMdxMemberName(String p0) {
		return formatError(322040, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxMemberName(String p0) {
		return new mondrian.resource.Error(null, this, MdxMemberName, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxMemberName(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxMemberName, new Object[] {p0});
	}
	/** e: Error while parsing MDX statement '%1' */
	public static final int WhileParsingMdx = 350000;
	public String getWhileParsingMdx(String p0) {
		return formatError(350000, new Object[] {p0});
	}
	public mondrian.resource.Error newWhileParsingMdx(String p0) {
		return new mondrian.resource.Error(null, this, WhileParsingMdx, new Object[] {p0});
	}
	public mondrian.resource.Error newWhileParsingMdx(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, WhileParsingMdx, new Object[] {p0});
	}
	/** e: MDX parser cannot recover from previous error(s) */
	public static final int MdxFatalError = 350010;
	public String getMdxFatalError() {
		return formatError(350010, new Object[] {});
	}
	public mondrian.resource.Error newMdxFatalError() {
		return new mondrian.resource.Error(null, this, MdxFatalError, new Object[] {});
	}
	public mondrian.resource.Error newMdxFatalError(Throwable err) {
		return new mondrian.resource.Error(err, this, MdxFatalError, new Object[] {});
	}
	/** e: Error: %1 */
	public static final int MdxError = 350020;
	public String getMdxError(String p0) {
		return formatError(350020, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxError(String p0) {
		return new mondrian.resource.Error(null, this, MdxError, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxError(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxError, new Object[] {p0});
	}
	/** e: Syntax error at token '%1' */
	public static final int MdxSyntaxError = 350030;
	public String getMdxSyntaxError(String p0) {
		return formatError(350030, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxSyntaxError(String p0) {
		return new mondrian.resource.Error(null, this, MdxSyntaxError, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxSyntaxError(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxSyntaxError, new Object[] {p0});
	}
	/** e: Syntax error at line %2, column %3, token '%1' */
	public static final int MdxSyntaxErrorAt = 350040;
	public String getMdxSyntaxErrorAt(String p0, String p1, String p2) {
		return formatError(350040, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxSyntaxErrorAt(String p0, String p1, String p2) {
		return new mondrian.resource.Error(null, this, MdxSyntaxErrorAt, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxSyntaxErrorAt(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.Error(err, this, MdxSyntaxErrorAt, new Object[] {p0, p1, p2});
	}
	/** i: Couldn't repair and continue parse */
	public static final int MdxFatalSyntaxError = 350050;
	public String getMdxFatalSyntaxError() {
		return formatError(350050, new Object[] {});
	}
	public mondrian.resource.Error newMdxFatalSyntaxError() {
		return new mondrian.resource.Error(null, this, MdxFatalSyntaxError, new Object[] {});
	}
	public mondrian.resource.Error newMdxFatalSyntaxError(Throwable err) {
		return new mondrian.resource.Error(err, this, MdxFatalSyntaxError, new Object[] {});
	}
	/** e: User does not have rights to run query, which contains %1 */
	public static final int UserDoesNotHaveRightsTo = 350060;
	public String getUserDoesNotHaveRightsTo(String p0) {
		return formatError(350060, new Object[] {p0});
	}
	public mondrian.resource.Error newUserDoesNotHaveRightsTo(String p0) {
		return new mondrian.resource.Error(null, this, UserDoesNotHaveRightsTo, new Object[] {p0});
	}
	public mondrian.resource.Error newUserDoesNotHaveRightsTo(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, UserDoesNotHaveRightsTo, new Object[] {p0});
	}
	/** e: Failed to add Cube Slicer with member '%1' for hierarchy '%2' on cube '%3' */
	public static final int MdxCubeSlicerMemberError = 350070;
	public String getMdxCubeSlicerMemberError(String p0, String p1, String p2) {
		return formatError(350070, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxCubeSlicerMemberError(String p0, String p1, String p2) {
		return new mondrian.resource.Error(null, this, MdxCubeSlicerMemberError, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxCubeSlicerMemberError(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.Error(err, this, MdxCubeSlicerMemberError, new Object[] {p0, p1, p2});
	}
	/** e: Failed to add Cube Slicer for hierarchy '%1' on cube '%2' */
	public static final int MdxCubeSlicerHierarchyError = 350080;
	public String getMdxCubeSlicerHierarchyError(String p0, String p1) {
		return formatError(350080, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxCubeSlicerHierarchyError(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxCubeSlicerHierarchyError, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxCubeSlicerHierarchyError(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxCubeSlicerHierarchyError, new Object[] {p0, p1});
	}
	/** e: Connection error was received using connect string '%1' on catalog '%2' */
	public static final int MdxOlapServerConnectError = 350090;
	public String getMdxOlapServerConnectError(String p0, String p1) {
		return formatError(350090, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxOlapServerConnectError(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxOlapServerConnectError, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxOlapServerConnectError(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxOlapServerConnectError, new Object[] {p0, p1});
	}
	/** e: Invalid member identifier '%1' */
	public static final int MdxInvalidMember = 350100;
	public String getMdxInvalidMember(String p0) {
		return formatError(350100, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxInvalidMember(String p0) {
		return new mondrian.resource.Error(null, this, MdxInvalidMember, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxInvalidMember(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxInvalidMember, new Object[] {p0});
	}
	/** e: Hierarchy for calculated member '%1' not found */
	public static final int MdxCalculatedHierarchyError = 350110;
	public String getMdxCalculatedHierarchyError(String p0) {
		return formatError(350110, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCalculatedHierarchyError(String p0) {
		return new mondrian.resource.Error(null, this, MdxCalculatedHierarchyError, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCalculatedHierarchyError(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxCalculatedHierarchyError, new Object[] {p0});
	}
	/** e: Axis '%1' expression is not a set */
	public static final int MdxAxisIsNotSet = 350120;
	public String getMdxAxisIsNotSet(String p0) {
		return formatError(350120, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxAxisIsNotSet(String p0) {
		return new mondrian.resource.Error(null, this, MdxAxisIsNotSet, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxAxisIsNotSet(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxAxisIsNotSet, new Object[] {p0});
	}
	/** e: Member expression '%1' must not be a set */
	public static final int MdxMemberExpIsSet = 350130;
	public String getMdxMemberExpIsSet(String p0) {
		return formatError(350130, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxMemberExpIsSet(String p0) {
		return new mondrian.resource.Error(null, this, MdxMemberExpIsSet, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxMemberExpIsSet(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxMemberExpIsSet, new Object[] {p0});
	}
	/** e: Set expression '%1' must be a set */
	public static final int MdxSetExpNotSet = 350140;
	public String getMdxSetExpNotSet(String p0) {
		return formatError(350140, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxSetExpNotSet(String p0) {
		return new mondrian.resource.Error(null, this, MdxSetExpNotSet, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxSetExpNotSet(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxSetExpNotSet, new Object[] {p0});
	}
	/** e: Function '%1' must have at least 2 arguments */
	public static final int MdxFuncArgumentsNum = 350150;
	public String getMdxFuncArgumentsNum(String p0) {
		return formatError(350150, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncArgumentsNum(String p0) {
		return new mondrian.resource.Error(null, this, MdxFuncArgumentsNum, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncArgumentsNum(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxFuncArgumentsNum, new Object[] {p0});
	}
	/** e: Argument '%i1' of function '%2' must be a hierarchy */
	public static final int MdxFuncNotHier = 350160;
	public String getMdxFuncNotHier(int p0, String p1) {
		return formatError(350160, new Object[] {Integer.toString(p0), p1});
	}
	public mondrian.resource.Error newMdxFuncNotHier(int p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxFuncNotHier, new Object[] {Integer.toString(p0), p1});
	}
	public mondrian.resource.Error newMdxFuncNotHier(Throwable err, int p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxFuncNotHier, new Object[] {Integer.toString(p0), p1});
	}
	/** e: Method function '%1' must have at least one argument */
	public static final int MdxFuncMethodNoArg = 350170;
	public String getMdxFuncMethodNoArg(String p0) {
		return formatError(350170, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncMethodNoArg(String p0) {
		return new mondrian.resource.Error(null, this, MdxFuncMethodNoArg, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncMethodNoArg(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxFuncMethodNoArg, new Object[] {p0});
	}
	/** e: Property function '%1' must not have arguments.  (To access a member of a property, like 'Members', which returns a collection, write 'Members.Item(n)' instead of 'Members(n)'.) */
	public static final int MdxFuncPropertyArg = 350180;
	public String getMdxFuncPropertyArg(String p0) {
		return formatError(350180, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncPropertyArg(String p0) {
		return new mondrian.resource.Error(null, this, MdxFuncPropertyArg, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncPropertyArg(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxFuncPropertyArg, new Object[] {p0});
	}
	/** e: unknown overloaded function '%1' */
	public static final int MdxFuncUnknown = 350190;
	public String getMdxFuncUnknown(String p0) {
		return formatError(350190, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncUnknown(String p0) {
		return new mondrian.resource.Error(null, this, MdxFuncUnknown, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxFuncUnknown(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxFuncUnknown, new Object[] {p0});
	}
	/** e: Could not locate DefaultHierarchy for '%1' */
	public static final int MdxDefaultHierNotFound = 350200;
	public String getMdxDefaultHierNotFound(String p0) {
		return formatError(350200, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxDefaultHierNotFound(String p0) {
		return new mondrian.resource.Error(null, this, MdxDefaultHierNotFound, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxDefaultHierNotFound(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxDefaultHierNotFound, new Object[] {p0});
	}
	/** e: Could not locate DefaultMember for '%1' */
	public static final int MdxDefaultMemNotFound = 350210;
	public String getMdxDefaultMemNotFound(String p0) {
		return formatError(350210, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxDefaultMemNotFound(String p0) {
		return new mondrian.resource.Error(null, this, MdxDefaultMemNotFound, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxDefaultMemNotFound(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxDefaultMemNotFound, new Object[] {p0});
	}
	/** e: Could not locate member  '%1' in query '%2' */
	public static final int MdxMemberNotFound = 350215;
	public String getMdxMemberNotFound(String p0, String p1) {
		return formatError(350215, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxMemberNotFound(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxMemberNotFound, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxMemberNotFound(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxMemberNotFound, new Object[] {p0, p1});
	}
	/** e: Argument '%1' of Parameter '%2' has to be a quoted string */
	public static final int MdxParamArgInvalid = 350220;
	public String getMdxParamArgInvalid(String p0, String p1) {
		return formatError(350220, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxParamArgInvalid(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxParamArgInvalid, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxParamArgInvalid(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxParamArgInvalid, new Object[] {p0, p1});
	}
	/** e: Type '%1' of Parameter '%2' has to be a quoted string or dimension */
	public static final int MdxParamTypeInvalid = 350230;
	public String getMdxParamTypeInvalid(String p0, String p1) {
		return formatError(350230, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxParamTypeInvalid(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxParamTypeInvalid, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxParamTypeInvalid(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxParamTypeInvalid, new Object[] {p0, p1});
	}
	/** e: Parameter '%1' is defined '%i2' times */
	public static final int MdxParamMultipleDef = 350240;
	public String getMdxParamMultipleDef(String p0, int p1) {
		return formatError(350240, new Object[] {p0, Integer.toString(p1)});
	}
	public mondrian.resource.Error newMdxParamMultipleDef(String p0, int p1) {
		return new mondrian.resource.Error(null, this, MdxParamMultipleDef, new Object[] {p0, Integer.toString(p1)});
	}
	public mondrian.resource.Error newMdxParamMultipleDef(Throwable err, String p0, int p1) {
		return new mondrian.resource.Error(err, this, MdxParamMultipleDef, new Object[] {p0, Integer.toString(p1)});
	}
	/** e: Parameter's '%1' value not found */
	public static final int MdxParamValueNotFound = 350250;
	public String getMdxParamValueNotFound(String p0) {
		return formatError(350250, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxParamValueNotFound(String p0) {
		return new mondrian.resource.Error(null, this, MdxParamValueNotFound, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxParamValueNotFound(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxParamValueNotFound, new Object[] {p0});
	}
	/** e: Parameter '%1' not found */
	public static final int MdxParamNotFound = 350255;
	public String getMdxParamNotFound(String p0) {
		return formatError(350255, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxParamNotFound(String p0) {
		return new mondrian.resource.Error(null, this, MdxParamNotFound, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxParamNotFound(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxParamNotFound, new Object[] {p0});
	}
	/** e: Hierarchy '%1' is already used */
	public static final int MdxHierarchyUsed = 350260;
	public String getMdxHierarchyUsed(String p0) {
		return formatError(350260, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxHierarchyUsed(String p0) {
		return new mondrian.resource.Error(null, this, MdxHierarchyUsed, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxHierarchyUsed(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxHierarchyUsed, new Object[] {p0});
	}
	/** e: Hierarchy '%1' is not used */
	public static final int MdxHierarchyNotUsed = 350270;
	public String getMdxHierarchyNotUsed(String p0) {
		return formatError(350270, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxHierarchyNotUsed(String p0) {
		return new mondrian.resource.Error(null, this, MdxHierarchyNotUsed, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxHierarchyNotUsed(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxHierarchyNotUsed, new Object[] {p0});
	}
	/** e: Calculated MdxMember '%1' can not have children */
	public static final int MdxCalcMemberCanNotHaveChildren = 350280;
	public String getMdxCalcMemberCanNotHaveChildren(String p0) {
		return formatError(350280, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCalcMemberCanNotHaveChildren(String p0) {
		return new mondrian.resource.Error(null, this, MdxCalcMemberCanNotHaveChildren, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCalcMemberCanNotHaveChildren(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxCalcMemberCanNotHaveChildren, new Object[] {p0});
	}
	/** e: Calculated %1 '%2' has not been found in query '%3' */
	public static final int MdxFormulaNotFound = 350290;
	public String getMdxFormulaNotFound(String p0, String p1, String p2) {
		return formatError(350290, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxFormulaNotFound(String p0, String p1, String p2) {
		return new mondrian.resource.Error(null, this, MdxFormulaNotFound, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxFormulaNotFound(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.Error(err, this, MdxFormulaNotFound, new Object[] {p0, p1, p2});
	}
	/** e: Calculated %1 '%2' is already defined in query '%3' */
	public static final int MdxFormulaAlreadyExists = 350300;
	public String getMdxFormulaAlreadyExists(String p0, String p1, String p2) {
		return formatError(350300, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxFormulaAlreadyExists(String p0, String p1, String p2) {
		return new mondrian.resource.Error(null, this, MdxFormulaAlreadyExists, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxFormulaAlreadyExists(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.Error(err, this, MdxFormulaAlreadyExists, new Object[] {p0, p1, p2});
	}
	/** e: Cannot find MDX member '%1'. Make sure it is indeed a member and not a level or a hierarchy. */
	public static final int MdxCantFindMember = 350310;
	public String getMdxCantFindMember(String p0) {
		return formatError(350310, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCantFindMember(String p0) {
		return new mondrian.resource.Error(null, this, MdxCantFindMember, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxCantFindMember(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxCantFindMember, new Object[] {p0});
	}
	/** i: member */
	public static final int Member = 350320;
	public String getMember() {
		return formatError(350320, new Object[] {});
	}
	public mondrian.resource.Error newMember() {
		return new mondrian.resource.Error(null, this, Member, new Object[] {});
	}
	public mondrian.resource.Error newMember(Throwable err) {
		return new mondrian.resource.Error(err, this, Member, new Object[] {});
	}
	/** i: calculated member */
	public static final int CalculatedMember = 350325;
	public String getCalculatedMember() {
		return formatError(350325, new Object[] {});
	}
	public mondrian.resource.Error newCalculatedMember() {
		return new mondrian.resource.Error(null, this, CalculatedMember, new Object[] {});
	}
	public mondrian.resource.Error newCalculatedMember(Throwable err) {
		return new mondrian.resource.Error(err, this, CalculatedMember, new Object[] {});
	}
	/** i: set */
	public static final int Set = 350330;
	public String getSet() {
		return formatError(350330, new Object[] {});
	}
	public mondrian.resource.Error newSet() {
		return new mondrian.resource.Error(null, this, Set, new Object[] {});
	}
	public mondrian.resource.Error newSet(Throwable err) {
		return new mondrian.resource.Error(err, this, Set, new Object[] {});
	}
	/** i: calculated set */
	public static final int CalculatedSet = 350335;
	public String getCalculatedSet() {
		return formatError(350335, new Object[] {});
	}
	public mondrian.resource.Error newCalculatedSet() {
		return new mondrian.resource.Error(null, this, CalculatedSet, new Object[] {});
	}
	public mondrian.resource.Error newCalculatedSet(Throwable err) {
		return new mondrian.resource.Error(err, this, CalculatedSet, new Object[] {});
	}
	/** e: Cannot delete %1 '%2'. It is used on %3 axis. */
	public static final int MdxCalculatedFormulaUsedOnAxis = 350340;
	public String getMdxCalculatedFormulaUsedOnAxis(String p0, String p1, String p2) {
		return formatError(350340, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedOnAxis(String p0, String p1, String p2) {
		return new mondrian.resource.Error(null, this, MdxCalculatedFormulaUsedOnAxis, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedOnAxis(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.Error(err, this, MdxCalculatedFormulaUsedOnAxis, new Object[] {p0, p1, p2});
	}
	/** e: Cannot delete %1 '%2'. It is used on slicer. */
	public static final int MdxCalculatedFormulaUsedOnSlicer = 350350;
	public String getMdxCalculatedFormulaUsedOnSlicer(String p0, String p1) {
		return formatError(350350, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedOnSlicer(String p0, String p1) {
		return new mondrian.resource.Error(null, this, MdxCalculatedFormulaUsedOnSlicer, new Object[] {p0, p1});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedOnSlicer(Throwable err, String p0, String p1) {
		return new mondrian.resource.Error(err, this, MdxCalculatedFormulaUsedOnSlicer, new Object[] {p0, p1});
	}
	/** e: Cannot delete %1 '%2'. It is used in definition of %3 '%4'. */
	public static final int MdxCalculatedFormulaUsedInFormula = 350360;
	public String getMdxCalculatedFormulaUsedInFormula(String p0, String p1, String p2, String p3) {
		return formatError(350360, new Object[] {p0, p1, p2, p3});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedInFormula(String p0, String p1, String p2, String p3) {
		return new mondrian.resource.Error(null, this, MdxCalculatedFormulaUsedInFormula, new Object[] {p0, p1, p2, p3});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedInFormula(Throwable err, String p0, String p1, String p2, String p3) {
		return new mondrian.resource.Error(err, this, MdxCalculatedFormulaUsedInFormula, new Object[] {p0, p1, p2, p3});
	}
	/** e: Cannot delete %1 '%2'. It is used in query '%3'. */
	public static final int MdxCalculatedFormulaUsedInQuery = 350370;
	public String getMdxCalculatedFormulaUsedInQuery(String p0, String p1, String p2) {
		return formatError(350370, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedInQuery(String p0, String p1, String p2) {
		return new mondrian.resource.Error(null, this, MdxCalculatedFormulaUsedInQuery, new Object[] {p0, p1, p2});
	}
	public mondrian.resource.Error newMdxCalculatedFormulaUsedInQuery(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.Error(err, this, MdxCalculatedFormulaUsedInQuery, new Object[] {p0, p1, p2});
	}
	/** e: Top/Bottom functions require at least one sort Member. */
	public static final int MdxTopBottomNRequireSortMember = 350380;
	public String getMdxTopBottomNRequireSortMember() {
		return formatError(350380, new Object[] {});
	}
	public mondrian.resource.Error newMdxTopBottomNRequireSortMember() {
		return new mondrian.resource.Error(null, this, MdxTopBottomNRequireSortMember, new Object[] {});
	}
	public mondrian.resource.Error newMdxTopBottomNRequireSortMember(Throwable err) {
		return new mondrian.resource.Error(err, this, MdxTopBottomNRequireSortMember, new Object[] {});
	}
	/** e: Unknown Top/Bottom function name '%1'. Supported names are 'TopCount', 'BottomCount', 'TopPercent', 'BottomPercent'. */
	public static final int MdxTopBottomInvalidFunctionName = 350390;
	public String getMdxTopBottomInvalidFunctionName(String p0) {
		return formatError(350390, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxTopBottomInvalidFunctionName(String p0) {
		return new mondrian.resource.Error(null, this, MdxTopBottomInvalidFunctionName, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxTopBottomInvalidFunctionName(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxTopBottomInvalidFunctionName, new Object[] {p0});
	}
	/** e: Sort on axis '%1' is not supported. */
	public static final int MdxAxisSortNotSupported = 350400;
	public String getMdxAxisSortNotSupported(String p0) {
		return formatError(350400, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxAxisSortNotSupported(String p0) {
		return new mondrian.resource.Error(null, this, MdxAxisSortNotSupported, new Object[] {p0});
	}
	public mondrian.resource.Error newMdxAxisSortNotSupported(Throwable err, String p0) {
		return new mondrian.resource.Error(err, this, MdxAxisSortNotSupported, new Object[] {p0});
	}
	/** e: Show/hide subtotals operation on axis '%i1' is not supported. */
	public static final int MdxAxisShowSubtotalsNotSupported = 350410;
	public String getMdxAxisShowSubtotalsNotSupported(int p0) {
		return formatError(350410, new Object[] {Integer.toString(p0)});
	}
	public mondrian.resource.Error newMdxAxisShowSubtotalsNotSupported(int p0) {
		return new mondrian.resource.Error(null, this, MdxAxisShowSubtotalsNotSupported, new Object[] {Integer.toString(p0)});
	}
	public mondrian.resource.Error newMdxAxisShowSubtotalsNotSupported(Throwable err, int p0) {
		return new mondrian.resource.Error(err, this, MdxAxisShowSubtotalsNotSupported, new Object[] {Integer.toString(p0)});
	}
	/** e: Show/hide empty cells operation on axis '%i1' is not supported. */
	public static final int MdxAxisShowEmptyCellsNotSupported = 350420;
	public String getMdxAxisShowEmptyCellsNotSupported(int p0) {
		return formatError(350420, new Object[] {Integer.toString(p0)});
	}
	public mondrian.resource.Error newMdxAxisShowEmptyCellsNotSupported(int p0) {
		return new mondrian.resource.Error(null, this, MdxAxisShowEmptyCellsNotSupported, new Object[] {Integer.toString(p0)});
	}
	public mondrian.resource.Error newMdxAxisShowEmptyCellsNotSupported(Throwable err, int p0) {
		return new mondrian.resource.Error(err, this, MdxAxisShowEmptyCellsNotSupported, new Object[] {Integer.toString(p0)});
	}

}
