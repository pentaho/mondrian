package mondrian.junit5;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;

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
	System.out.println("### MondrianRuntimeExtension:beforeAll");
	if (!started) {

	    started = true;
	    // registers a callback hook when the root test context is shut down
	    context.getRoot().getStore(GLOBAL).put("MondrianRuntimeExtensionClosableCallbackHook", this);
	    defineLocale();
	    if (context.getTestClass().isPresent()) {
		MondrianRuntimeSupport annotation = context.getTestClass().get()
			.getAnnotation(MondrianRuntimeSupport.class);

		Class<? extends DatabaseHandler> dbHandlerClass = annotation.database();
		try {
		    DatabaseHandler databaseHandler = dbHandlerClass.getConstructor().newInstance();
		    databaseHandler.startDatabase(Map.of());
		    String jdbcUrl = databaseHandler.getJdbcUrl();

		    Class<? extends DataLoader> dataLoaderClass = annotation.dataLoader();

		    DataLoader dataLoader = dataLoaderClass.getConstructor().newInstance();
		    dataLoader.loadData(jdbcUrl);
		} catch (Exception e) {
		    e.printStackTrace();
		}

	    }

	}
    }

    private void defineLocale() {
	MondrianResource.setThreadLocale(Locale.US);
    }

    @Override
    public void close() {
	// Your "after all tests" logic goes here
	System.out.println("### MondrianRuntimeExtension:close");

    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
	System.out.println("### Evaluate Docker");

	try {
	    DockerClientFactory.instance().client();
	    return ConditionEvaluationResult.enabled("Docker is available");
	} catch (Throwable ex) {
	    return ConditionEvaluationResult.disabled("Docker is not available", ex.getMessage());
	}

    }

}