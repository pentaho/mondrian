package mondrian.junit5;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.sql.SQLException;
import java.util.Locale;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.DockerClientFactory;

import mondrian.resource.MondrianResource;

public class MondrianRuntimeExtension
	implements ExecutionCondition, BeforeAllCallback, ExtensionContext.Store.CloseableResource {


    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext context) {
	if (!started) {

	    started = true;
	    // registers a callback hook when the root test context is shut down
	    context.getRoot().getStore(GLOBAL).put("MondrianRuntimeExtensionClosableCallbackHook", this);
	    defineLocale();

	    System.out.println("##############################################################");
	}
    }

    private void defineLocale() {
	MondrianResource.setThreadLocale( Locale.US );
    }


    @Override
    public void close() {
	// Your "after all tests" logic goes here

    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

	try {
	    DockerClientFactory.instance().client();
	    return ConditionEvaluationResult.enabled("Docker is available");
	} catch (Throwable ex) {
	    return ConditionEvaluationResult.disabled("Docker is not available", ex.getMessage());
	}

    }

}