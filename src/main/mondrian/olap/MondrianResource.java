// This class is generated. Do NOT modify it, or
// add it to source control or the J++ project.

package mondrian.olap;

import mondrian.resource.ResourceDefinition;
import mondrian.resource.ResourceInstance;
import mondrian.resource.ShadowResourceBundle;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * This class was generated
 * by class mondrian.resource.ResourceGen
 * from Resources/MdxResource_en.xml
 * on Fri Feb 08 02:24:28 PST 2002.
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 **/

public class MondrianResource extends ShadowResourceBundle {
	public MondrianResource() throws IOException {
	}
	private static String baseName = MondrianResource.class.getName();
	/**
	 * Retrieves the singleton instance of {@link MondrianResource}. If
	 * the application has called {@link #setThreadLocale}, returns the
	 * resource for the thread's locale.
	 */
	public static synchronized MondrianResource instance() {
		return (MondrianResource) instance(baseName);
	}
	/**
	 * Retrieves the instance of {@link MondrianResource} for the given locale.
	 */
	public static synchronized MondrianResource instance(Locale locale) {
		return (MondrianResource) instance(baseName, locale);
	}

	public static final ResourceDefinition Internal = new ResourceDefinition("Internal", "Internal error: {1}");
	public String getInternal(String p0) {
		return Internal.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableRuntimeException newInternal(String p0) {
		return new mondrian.resource.ChainableRuntimeException(Internal.instantiate(this, new Object[] {p0}), null);
	}
	public mondrian.resource.ChainableRuntimeException newInternal(Throwable err, String p0) {
		return new mondrian.resource.ChainableRuntimeException(Internal.instantiate(this, new Object[] {p0}), err);
	}
	public static final ResourceDefinition MdxCubeNotFound = new ResourceDefinition("MdxCubeNotFound", "MDX cube '%1' not found or not processed");
	public String getMdxCubeNotFound(String p0) {
		return MdxCubeNotFound.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxCubeNotFound(String p0) {
		return new mondrian.resource.ChainableError(null, MdxCubeNotFound.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxCubeNotFound(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxCubeNotFound.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxChildObjectNotFound = new ResourceDefinition("MdxChildObjectNotFound", "MDX object '%1' not found in %2");
	public String getMdxChildObjectNotFound(String p0, String p1) {
		return MdxChildObjectNotFound.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxChildObjectNotFound(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxChildObjectNotFound.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxChildObjectNotFound(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxChildObjectNotFound.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxCubeName = new ResourceDefinition("MdxCubeName", "cube '%1'");
	public String getMdxCubeName(String p0) {
		return MdxCubeName.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxCubeName(String p0) {
		return new mondrian.resource.ChainableError(null, MdxCubeName.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxCubeName(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxCubeName.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxHierarchyName = new ResourceDefinition("MdxHierarchyName", "hierarchy '%1'");
	public String getMdxHierarchyName(String p0) {
		return MdxHierarchyName.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxHierarchyName(String p0) {
		return new mondrian.resource.ChainableError(null, MdxHierarchyName.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxHierarchyName(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxHierarchyName.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxDimensionName = new ResourceDefinition("MdxDimensionName", "dimension '%1'");
	public String getMdxDimensionName(String p0) {
		return MdxDimensionName.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxDimensionName(String p0) {
		return new mondrian.resource.ChainableError(null, MdxDimensionName.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxDimensionName(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxDimensionName.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxLevelName = new ResourceDefinition("MdxLevelName", "level '%1'");
	public String getMdxLevelName(String p0) {
		return MdxLevelName.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxLevelName(String p0) {
		return new mondrian.resource.ChainableError(null, MdxLevelName.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxLevelName(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxLevelName.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxMemberName = new ResourceDefinition("MdxMemberName", "member '%1'");
	public String getMdxMemberName(String p0) {
		return MdxMemberName.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxMemberName(String p0) {
		return new mondrian.resource.ChainableError(null, MdxMemberName.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxMemberName(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxMemberName.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition WhileParsingMdx = new ResourceDefinition("WhileParsingMdx", "Error while parsing MDX statement '%1'");
	public String getWhileParsingMdx(String p0) {
		return WhileParsingMdx.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newWhileParsingMdx(String p0) {
		return new mondrian.resource.ChainableError(null, WhileParsingMdx.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newWhileParsingMdx(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, WhileParsingMdx.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxFatalError = new ResourceDefinition("MdxFatalError", "MDX parser cannot recover from previous error(s)");
	public String getMdxFatalError() {
		return MdxFatalError.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newMdxFatalError() {
		return new mondrian.resource.ChainableError(null, MdxFatalError.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newMdxFatalError(Throwable err) {
		return new mondrian.resource.ChainableError(err, MdxFatalError.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition MdxError = new ResourceDefinition("MdxError", "Error: %1");
	public String getMdxError(String p0) {
		return MdxError.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxError(String p0) {
		return new mondrian.resource.ChainableError(null, MdxError.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxError(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxError.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxSyntaxError = new ResourceDefinition("MdxSyntaxError", "Syntax error at token '%1'");
	public String getMdxSyntaxError(String p0) {
		return MdxSyntaxError.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxSyntaxError(String p0) {
		return new mondrian.resource.ChainableError(null, MdxSyntaxError.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxSyntaxError(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxSyntaxError.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxSyntaxErrorAt = new ResourceDefinition("MdxSyntaxErrorAt", "Syntax error at line %2, column %3, token '%1'");
	public String getMdxSyntaxErrorAt(String p0, String p1, String p2) {
		return MdxSyntaxErrorAt.instantiate(this, new Object[] {p0, p1, p2}).toString();
	}
	public mondrian.resource.ChainableError newMdxSyntaxErrorAt(String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(null, MdxSyntaxErrorAt.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public mondrian.resource.ChainableError newMdxSyntaxErrorAt(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(err, MdxSyntaxErrorAt.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public static final ResourceDefinition MdxFatalSyntaxError = new ResourceDefinition("MdxFatalSyntaxError", "Couldn't repair and continue parse");
	public String getMdxFatalSyntaxError() {
		return MdxFatalSyntaxError.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newMdxFatalSyntaxError() {
		return new mondrian.resource.ChainableError(null, MdxFatalSyntaxError.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newMdxFatalSyntaxError(Throwable err) {
		return new mondrian.resource.ChainableError(err, MdxFatalSyntaxError.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition UserDoesNotHaveRightsTo = new ResourceDefinition("UserDoesNotHaveRightsTo", "User does not have rights to run query, which contains %1");
	public String getUserDoesNotHaveRightsTo(String p0) {
		return UserDoesNotHaveRightsTo.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newUserDoesNotHaveRightsTo(String p0) {
		return new mondrian.resource.ChainableError(null, UserDoesNotHaveRightsTo.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newUserDoesNotHaveRightsTo(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, UserDoesNotHaveRightsTo.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxCubeSlicerMemberError = new ResourceDefinition("MdxCubeSlicerMemberError", "Failed to add Cube Slicer with member '%1' for hierarchy '%2' on cube '%3'");
	public String getMdxCubeSlicerMemberError(String p0, String p1, String p2) {
		return MdxCubeSlicerMemberError.instantiate(this, new Object[] {p0, p1, p2}).toString();
	}
	public mondrian.resource.ChainableError newMdxCubeSlicerMemberError(String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(null, MdxCubeSlicerMemberError.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public mondrian.resource.ChainableError newMdxCubeSlicerMemberError(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(err, MdxCubeSlicerMemberError.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public static final ResourceDefinition MdxCubeSlicerHierarchyError = new ResourceDefinition("MdxCubeSlicerHierarchyError", "Failed to add Cube Slicer for hierarchy '%1' on cube '%2'");
	public String getMdxCubeSlicerHierarchyError(String p0, String p1) {
		return MdxCubeSlicerHierarchyError.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxCubeSlicerHierarchyError(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxCubeSlicerHierarchyError.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxCubeSlicerHierarchyError(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxCubeSlicerHierarchyError.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxOlapServerConnectError = new ResourceDefinition("MdxOlapServerConnectError", "Connection error was received using connect string '%1' on catalog '%2'");
	public String getMdxOlapServerConnectError(String p0, String p1) {
		return MdxOlapServerConnectError.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxOlapServerConnectError(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxOlapServerConnectError.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxOlapServerConnectError(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxOlapServerConnectError.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxInvalidMember = new ResourceDefinition("MdxInvalidMember", "Invalid member identifier '%1'");
	public String getMdxInvalidMember(String p0) {
		return MdxInvalidMember.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxInvalidMember(String p0) {
		return new mondrian.resource.ChainableError(null, MdxInvalidMember.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxInvalidMember(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxInvalidMember.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxCalculatedHierarchyError = new ResourceDefinition("MdxCalculatedHierarchyError", "Hierarchy for calculated member '%1' not found");
	public String getMdxCalculatedHierarchyError(String p0) {
		return MdxCalculatedHierarchyError.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxCalculatedHierarchyError(String p0) {
		return new mondrian.resource.ChainableError(null, MdxCalculatedHierarchyError.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxCalculatedHierarchyError(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxCalculatedHierarchyError.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxAxisIsNotSet = new ResourceDefinition("MdxAxisIsNotSet", "Axis '%1' expression is not a set");
	public String getMdxAxisIsNotSet(String p0) {
		return MdxAxisIsNotSet.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxAxisIsNotSet(String p0) {
		return new mondrian.resource.ChainableError(null, MdxAxisIsNotSet.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxAxisIsNotSet(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxAxisIsNotSet.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxMemberExpIsSet = new ResourceDefinition("MdxMemberExpIsSet", "Member expression '%1' must not be a set");
	public String getMdxMemberExpIsSet(String p0) {
		return MdxMemberExpIsSet.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxMemberExpIsSet(String p0) {
		return new mondrian.resource.ChainableError(null, MdxMemberExpIsSet.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxMemberExpIsSet(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxMemberExpIsSet.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxSetExpNotSet = new ResourceDefinition("MdxSetExpNotSet", "Set expression '%1' must be a set");
	public String getMdxSetExpNotSet(String p0) {
		return MdxSetExpNotSet.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxSetExpNotSet(String p0) {
		return new mondrian.resource.ChainableError(null, MdxSetExpNotSet.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxSetExpNotSet(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxSetExpNotSet.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxFuncArgumentsNum = new ResourceDefinition("MdxFuncArgumentsNum", "Function '%1' must have at least 2 arguments");
	public String getMdxFuncArgumentsNum(String p0) {
		return MdxFuncArgumentsNum.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxFuncArgumentsNum(String p0) {
		return new mondrian.resource.ChainableError(null, MdxFuncArgumentsNum.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxFuncArgumentsNum(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxFuncArgumentsNum.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxFuncNotHier = new ResourceDefinition("MdxFuncNotHier", "Argument '%i1' of function '%2' must be a hierarchy");
	public String getMdxFuncNotHier(int p0, String p1) {
		return MdxFuncNotHier.instantiate(this, new Object[] {Integer.toString(p0), p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxFuncNotHier(int p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxFuncNotHier.instantiate(this, new Object[] {Integer.toString(p0), p1}));
	}
	public mondrian.resource.ChainableError newMdxFuncNotHier(Throwable err, int p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxFuncNotHier.instantiate(this, new Object[] {Integer.toString(p0), p1}));
	}
	public static final ResourceDefinition MdxFuncMethodNoArg = new ResourceDefinition("MdxFuncMethodNoArg", "Method function '%1' must have at least one argument");
	public String getMdxFuncMethodNoArg(String p0) {
		return MdxFuncMethodNoArg.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxFuncMethodNoArg(String p0) {
		return new mondrian.resource.ChainableError(null, MdxFuncMethodNoArg.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxFuncMethodNoArg(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxFuncMethodNoArg.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxFuncPropertyArg = new ResourceDefinition("MdxFuncPropertyArg", "Property function '%1' must not have arguments.  (To access a member of a property, like 'Members', which returns a collection, write 'Members.Item(n)' instead of 'Members(n)'.)");
	public String getMdxFuncPropertyArg(String p0) {
		return MdxFuncPropertyArg.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxFuncPropertyArg(String p0) {
		return new mondrian.resource.ChainableError(null, MdxFuncPropertyArg.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxFuncPropertyArg(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxFuncPropertyArg.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxFuncUnknown = new ResourceDefinition("MdxFuncUnknown", "unknown overloaded function '%1'");
	public String getMdxFuncUnknown(String p0) {
		return MdxFuncUnknown.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxFuncUnknown(String p0) {
		return new mondrian.resource.ChainableError(null, MdxFuncUnknown.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxFuncUnknown(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxFuncUnknown.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxDefaultHierNotFound = new ResourceDefinition("MdxDefaultHierNotFound", "Could not locate DefaultHierarchy for '%1'");
	public String getMdxDefaultHierNotFound(String p0) {
		return MdxDefaultHierNotFound.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxDefaultHierNotFound(String p0) {
		return new mondrian.resource.ChainableError(null, MdxDefaultHierNotFound.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxDefaultHierNotFound(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxDefaultHierNotFound.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxDefaultMemNotFound = new ResourceDefinition("MdxDefaultMemNotFound", "Could not locate DefaultMember for '%1'");
	public String getMdxDefaultMemNotFound(String p0) {
		return MdxDefaultMemNotFound.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxDefaultMemNotFound(String p0) {
		return new mondrian.resource.ChainableError(null, MdxDefaultMemNotFound.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxDefaultMemNotFound(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxDefaultMemNotFound.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxMemberNotFound = new ResourceDefinition("MdxMemberNotFound", "Could not locate member  '%1' in query '%2'");
	public String getMdxMemberNotFound(String p0, String p1) {
		return MdxMemberNotFound.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxMemberNotFound(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxMemberNotFound.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxMemberNotFound(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxMemberNotFound.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxParamArgInvalid = new ResourceDefinition("MdxParamArgInvalid", "Argument '%1' of Parameter '%2' has to be a quoted string");
	public String getMdxParamArgInvalid(String p0, String p1) {
		return MdxParamArgInvalid.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxParamArgInvalid(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxParamArgInvalid.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxParamArgInvalid(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxParamArgInvalid.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxParamTypeInvalid = new ResourceDefinition("MdxParamTypeInvalid", "Type '%1' of Parameter '%2' has to be a quoted string or dimension");
	public String getMdxParamTypeInvalid(String p0, String p1) {
		return MdxParamTypeInvalid.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxParamTypeInvalid(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxParamTypeInvalid.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxParamTypeInvalid(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxParamTypeInvalid.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxParamMultipleDef = new ResourceDefinition("MdxParamMultipleDef", "Parameter '%1' is defined '%i2' times");
	public String getMdxParamMultipleDef(String p0, int p1) {
		return MdxParamMultipleDef.instantiate(this, new Object[] {p0, Integer.toString(p1)}).toString();
	}
	public mondrian.resource.ChainableError newMdxParamMultipleDef(String p0, int p1) {
		return new mondrian.resource.ChainableError(null, MdxParamMultipleDef.instantiate(this, new Object[] {p0, Integer.toString(p1)}));
	}
	public mondrian.resource.ChainableError newMdxParamMultipleDef(Throwable err, String p0, int p1) {
		return new mondrian.resource.ChainableError(err, MdxParamMultipleDef.instantiate(this, new Object[] {p0, Integer.toString(p1)}));
	}
	public static final ResourceDefinition MdxParamValueNotFound = new ResourceDefinition("MdxParamValueNotFound", "Parameter's '%1' value not found");
	public String getMdxParamValueNotFound(String p0) {
		return MdxParamValueNotFound.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxParamValueNotFound(String p0) {
		return new mondrian.resource.ChainableError(null, MdxParamValueNotFound.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxParamValueNotFound(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxParamValueNotFound.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxParamNotFound = new ResourceDefinition("MdxParamNotFound", "Parameter '%1' not found");
	public String getMdxParamNotFound(String p0) {
		return MdxParamNotFound.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxParamNotFound(String p0) {
		return new mondrian.resource.ChainableError(null, MdxParamNotFound.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxParamNotFound(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxParamNotFound.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxHierarchyUsed = new ResourceDefinition("MdxHierarchyUsed", "Hierarchy '%1' is already used");
	public String getMdxHierarchyUsed(String p0) {
		return MdxHierarchyUsed.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxHierarchyUsed(String p0) {
		return new mondrian.resource.ChainableError(null, MdxHierarchyUsed.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxHierarchyUsed(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxHierarchyUsed.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxHierarchyNotUsed = new ResourceDefinition("MdxHierarchyNotUsed", "Hierarchy '%1' is not used");
	public String getMdxHierarchyNotUsed(String p0) {
		return MdxHierarchyNotUsed.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxHierarchyNotUsed(String p0) {
		return new mondrian.resource.ChainableError(null, MdxHierarchyNotUsed.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxHierarchyNotUsed(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxHierarchyNotUsed.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxCalcMemberCanNotHaveChildren = new ResourceDefinition("MdxCalcMemberCanNotHaveChildren", "Calculated MdxMember '%1' can not have children");
	public String getMdxCalcMemberCanNotHaveChildren(String p0) {
		return MdxCalcMemberCanNotHaveChildren.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxCalcMemberCanNotHaveChildren(String p0) {
		return new mondrian.resource.ChainableError(null, MdxCalcMemberCanNotHaveChildren.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxCalcMemberCanNotHaveChildren(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxCalcMemberCanNotHaveChildren.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxFormulaNotFound = new ResourceDefinition("MdxFormulaNotFound", "Calculated %1 '%2' has not been found in query '%3'");
	public String getMdxFormulaNotFound(String p0, String p1, String p2) {
		return MdxFormulaNotFound.instantiate(this, new Object[] {p0, p1, p2}).toString();
	}
	public mondrian.resource.ChainableError newMdxFormulaNotFound(String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(null, MdxFormulaNotFound.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public mondrian.resource.ChainableError newMdxFormulaNotFound(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(err, MdxFormulaNotFound.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public static final ResourceDefinition MdxFormulaAlreadyExists = new ResourceDefinition("MdxFormulaAlreadyExists", "Calculated %1 '%2' is already defined in query '%3'");
	public String getMdxFormulaAlreadyExists(String p0, String p1, String p2) {
		return MdxFormulaAlreadyExists.instantiate(this, new Object[] {p0, p1, p2}).toString();
	}
	public mondrian.resource.ChainableError newMdxFormulaAlreadyExists(String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(null, MdxFormulaAlreadyExists.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public mondrian.resource.ChainableError newMdxFormulaAlreadyExists(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(err, MdxFormulaAlreadyExists.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public static final ResourceDefinition MdxCantFindMember = new ResourceDefinition("MdxCantFindMember", "Cannot find MDX member '%1'. Make sure it is indeed a member and not a level or a hierarchy.");
	public String getMdxCantFindMember(String p0) {
		return MdxCantFindMember.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxCantFindMember(String p0) {
		return new mondrian.resource.ChainableError(null, MdxCantFindMember.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxCantFindMember(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxCantFindMember.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition Member = new ResourceDefinition("Member", "member");
	public String getMember() {
		return Member.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newMember() {
		return new mondrian.resource.ChainableError(null, Member.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newMember(Throwable err) {
		return new mondrian.resource.ChainableError(err, Member.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition CalculatedMember = new ResourceDefinition("CalculatedMember", "calculated member");
	public String getCalculatedMember() {
		return CalculatedMember.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newCalculatedMember() {
		return new mondrian.resource.ChainableError(null, CalculatedMember.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newCalculatedMember(Throwable err) {
		return new mondrian.resource.ChainableError(err, CalculatedMember.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition Set = new ResourceDefinition("Set", "set");
	public String getSet() {
		return Set.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newSet() {
		return new mondrian.resource.ChainableError(null, Set.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newSet(Throwable err) {
		return new mondrian.resource.ChainableError(err, Set.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition CalculatedSet = new ResourceDefinition("CalculatedSet", "calculated set");
	public String getCalculatedSet() {
		return CalculatedSet.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newCalculatedSet() {
		return new mondrian.resource.ChainableError(null, CalculatedSet.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newCalculatedSet(Throwable err) {
		return new mondrian.resource.ChainableError(err, CalculatedSet.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition MdxCalculatedFormulaUsedOnAxis = new ResourceDefinition("MdxCalculatedFormulaUsedOnAxis", "Cannot delete %1 '%2'. It is used on %3 axis.");
	public String getMdxCalculatedFormulaUsedOnAxis(String p0, String p1, String p2) {
		return MdxCalculatedFormulaUsedOnAxis.instantiate(this, new Object[] {p0, p1, p2}).toString();
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedOnAxis(String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(null, MdxCalculatedFormulaUsedOnAxis.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedOnAxis(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(err, MdxCalculatedFormulaUsedOnAxis.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public static final ResourceDefinition MdxCalculatedFormulaUsedOnSlicer = new ResourceDefinition("MdxCalculatedFormulaUsedOnSlicer", "Cannot delete %1 '%2'. It is used on slicer.");
	public String getMdxCalculatedFormulaUsedOnSlicer(String p0, String p1) {
		return MdxCalculatedFormulaUsedOnSlicer.instantiate(this, new Object[] {p0, p1}).toString();
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedOnSlicer(String p0, String p1) {
		return new mondrian.resource.ChainableError(null, MdxCalculatedFormulaUsedOnSlicer.instantiate(this, new Object[] {p0, p1}));
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedOnSlicer(Throwable err, String p0, String p1) {
		return new mondrian.resource.ChainableError(err, MdxCalculatedFormulaUsedOnSlicer.instantiate(this, new Object[] {p0, p1}));
	}
	public static final ResourceDefinition MdxCalculatedFormulaUsedInFormula = new ResourceDefinition("MdxCalculatedFormulaUsedInFormula", "Cannot delete %1 '%2'. It is used in definition of %3 '%4'.");
	public String getMdxCalculatedFormulaUsedInFormula(String p0, String p1, String p2, String p3) {
		return MdxCalculatedFormulaUsedInFormula.instantiate(this, new Object[] {p0, p1, p2, p3}).toString();
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedInFormula(String p0, String p1, String p2, String p3) {
		return new mondrian.resource.ChainableError(null, MdxCalculatedFormulaUsedInFormula.instantiate(this, new Object[] {p0, p1, p2, p3}));
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedInFormula(Throwable err, String p0, String p1, String p2, String p3) {
		return new mondrian.resource.ChainableError(err, MdxCalculatedFormulaUsedInFormula.instantiate(this, new Object[] {p0, p1, p2, p3}));
	}
	public static final ResourceDefinition MdxCalculatedFormulaUsedInQuery = new ResourceDefinition("MdxCalculatedFormulaUsedInQuery", "Cannot delete %1 '%2'. It is used in query '%3'.");
	public String getMdxCalculatedFormulaUsedInQuery(String p0, String p1, String p2) {
		return MdxCalculatedFormulaUsedInQuery.instantiate(this, new Object[] {p0, p1, p2}).toString();
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedInQuery(String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(null, MdxCalculatedFormulaUsedInQuery.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public mondrian.resource.ChainableError newMdxCalculatedFormulaUsedInQuery(Throwable err, String p0, String p1, String p2) {
		return new mondrian.resource.ChainableError(err, MdxCalculatedFormulaUsedInQuery.instantiate(this, new Object[] {p0, p1, p2}));
	}
	public static final ResourceDefinition MdxTopBottomNRequireSortMember = new ResourceDefinition("MdxTopBottomNRequireSortMember", "Top/Bottom functions require at least one sort Member.");
	public String getMdxTopBottomNRequireSortMember() {
		return MdxTopBottomNRequireSortMember.instantiate(this, new Object[] {}).toString();
	}
	public mondrian.resource.ChainableError newMdxTopBottomNRequireSortMember() {
		return new mondrian.resource.ChainableError(null, MdxTopBottomNRequireSortMember.instantiate(this, new Object[] {}));
	}
	public mondrian.resource.ChainableError newMdxTopBottomNRequireSortMember(Throwable err) {
		return new mondrian.resource.ChainableError(err, MdxTopBottomNRequireSortMember.instantiate(this, new Object[] {}));
	}
	public static final ResourceDefinition MdxTopBottomInvalidFunctionName = new ResourceDefinition("MdxTopBottomInvalidFunctionName", "Unknown Top/Bottom function name '%1'. Supported names are 'TopCount', 'BottomCount', 'TopPercent', 'BottomPercent'.");
	public String getMdxTopBottomInvalidFunctionName(String p0) {
		return MdxTopBottomInvalidFunctionName.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxTopBottomInvalidFunctionName(String p0) {
		return new mondrian.resource.ChainableError(null, MdxTopBottomInvalidFunctionName.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxTopBottomInvalidFunctionName(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxTopBottomInvalidFunctionName.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxAxisSortNotSupported = new ResourceDefinition("MdxAxisSortNotSupported", "Sort on axis '%1' is not supported.");
	public String getMdxAxisSortNotSupported(String p0) {
		return MdxAxisSortNotSupported.instantiate(this, new Object[] {p0}).toString();
	}
	public mondrian.resource.ChainableError newMdxAxisSortNotSupported(String p0) {
		return new mondrian.resource.ChainableError(null, MdxAxisSortNotSupported.instantiate(this, new Object[] {p0}));
	}
	public mondrian.resource.ChainableError newMdxAxisSortNotSupported(Throwable err, String p0) {
		return new mondrian.resource.ChainableError(err, MdxAxisSortNotSupported.instantiate(this, new Object[] {p0}));
	}
	public static final ResourceDefinition MdxAxisShowSubtotalsNotSupported = new ResourceDefinition("MdxAxisShowSubtotalsNotSupported", "Show/hide subtotals operation on axis '%i1' is not supported.");
	public String getMdxAxisShowSubtotalsNotSupported(int p0) {
		return MdxAxisShowSubtotalsNotSupported.instantiate(this, new Object[] {Integer.toString(p0)}).toString();
	}
	public mondrian.resource.ChainableError newMdxAxisShowSubtotalsNotSupported(int p0) {
		return new mondrian.resource.ChainableError(null, MdxAxisShowSubtotalsNotSupported.instantiate(this, new Object[] {Integer.toString(p0)}));
	}
	public mondrian.resource.ChainableError newMdxAxisShowSubtotalsNotSupported(Throwable err, int p0) {
		return new mondrian.resource.ChainableError(err, MdxAxisShowSubtotalsNotSupported.instantiate(this, new Object[] {Integer.toString(p0)}));
	}
	public static final ResourceDefinition MdxAxisShowEmptyCellsNotSupported = new ResourceDefinition("MdxAxisShowEmptyCellsNotSupported", "Show/hide empty cells operation on axis '%i1' is not supported.");
	public String getMdxAxisShowEmptyCellsNotSupported(int p0) {
		return MdxAxisShowEmptyCellsNotSupported.instantiate(this, new Object[] {Integer.toString(p0)}).toString();
	}
	public mondrian.resource.ChainableError newMdxAxisShowEmptyCellsNotSupported(int p0) {
		return new mondrian.resource.ChainableError(null, MdxAxisShowEmptyCellsNotSupported.instantiate(this, new Object[] {Integer.toString(p0)}));
	}
	public mondrian.resource.ChainableError newMdxAxisShowEmptyCellsNotSupported(Throwable err, int p0) {
		return new mondrian.resource.ChainableError(err, MdxAxisShowEmptyCellsNotSupported.instantiate(this, new Object[] {Integer.toString(p0)}));
	}

}
