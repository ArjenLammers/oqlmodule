package oql.implementation;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataColumnSchema;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTable;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTableSchema;
import com.mendix.systemwideinterfaces.connectionbus.requests.IParameterMap;
import com.mendix.systemwideinterfaces.connectionbus.requests.IRetrievalSchema;
import com.mendix.systemwideinterfaces.connectionbus.requests.types.IOQLTextGetRequest;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaAssociation;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class OQL {

	static ThreadLocal<Map<String, Object>> nextParameters = new ThreadLocal<Map<String, Object>>();

	private static final ILogNode LOGGER = Core.getLogger(OQL.class.getSimpleName());

	public static Map<String, Object> getNextParameters() {
		if (nextParameters.get() == null) {
			resetParameters();
		}
		return nextParameters.get();
	}

	public static void resetParameters() {
		nextParameters.set(new HashMap<>());
	}

	public static void addParameter(String name, Object value) {
		getNextParameters().put(name, value);
	}

	public static Long countRowsOQL(IContext context, String statement, Long amount, Map<String, Object> parameters) throws CoreException {
		IOQLTextGetRequest request = prepareRequest(statement, amount, 0l);

		IDataTable results = runQuery(statement, context, request);
		return (long) results.getRowCount();
	}

	static IDataTable runQuery(String statement, IContext context, IOQLTextGetRequest request) throws CoreException {
		LOGGER.debug(String.format("Executing query %s", statement));
		IDataTable results = Core.retrieveOQLDataTable(context, request);
		return results;
	}

	static IOQLTextGetRequest prepareRequest(String statement, Long amount, Long offset) {
		IOQLTextGetRequest request = Core.createOQLTextGetRequest();
		request.setQuery(statement);
		IParameterMap parameterMap = request.createParameterMap();
		OQL.getNextParameters().forEach(parameterMap::put);
		request.setParameters(parameterMap);

		IRetrievalSchema schema = Core.createRetrievalSchema();
		schema.setAmount(amount);
		schema.setOffset(offset);
		request.setRetrievalSchema(schema);

		return request;
	}

	public static List<IMendixObject> executeOQL(IContext context, String statement, String returnEntity, Long amount, Long offset, Map<String, Object> parameters) throws CoreException {
		IOQLTextGetRequest request = prepareRequest(statement, amount, offset);

		List<IMendixObject> result = new ArrayList<>();
		IDataTable results = runQuery(statement, context, request);
		LOGGER.debug(String.format("Mapping %d results.", results.getRowCount()));
		IDataTableSchema tableSchema = results.getSchema();

		results.getRows().forEach(row -> {
			IMendixObject targetObj = Core.instantiate(context, returnEntity);
			tableSchema.getColumnSchemas().forEach(columnSchema -> {
				LOGGER.trace("Mapping column " + columnSchema.getName());
				Optional.ofNullable(row.getValue(context, columnSchema))
					.ifPresent(value -> {
						if (value instanceof IMendixIdentifier) {
							copyAssociationValueToTarget(targetObj, columnSchema, value, context);
						} else {
							copyPrimitiveValueToTarget(targetObj, columnSchema, value, context);
						}
					});
			});
			result.add(targetObj);
		});

		return result;
	}

	static void copyPrimitiveValueToTarget(IMendixObject targetObj, IDataColumnSchema columnSchema, Object value, IContext context) throws NumberFormatException {
		LOGGER.trace("Treating as value");

		IMetaObject targetMeta = targetObj.getMetaObject();
		IMetaPrimitive primitive = targetMeta.getMetaPrimitive(columnSchema.getName());

		if (value instanceof Integer && primitive.getType() == PrimitiveType.Long) {
			value = (Long) ((Integer) value).longValue();
		} else if (value instanceof Long && primitive.getType() == PrimitiveType.Integer) {
			value = Integer.parseInt(((Long) value).toString()); // not so happy way of conversion
		}
		targetObj.setValue(context, columnSchema.getName(), value);
	}

	static void copyAssociationValueToTarget(IMendixObject targetObj, IDataColumnSchema columnSchema, Object value, IContext context) {
		LOGGER.trace("Treating as association");
		/* Escaping an alias as described at https://docs.mendix.com/refguide7/oql-select-clause
		 * leads to an error when using dots e.g. (OQL.ExamplePerson_ExamplePersonResult).
		 * Therefore this action accepts the ExamplePerson_ExamplePersonResult part and searches for the
		 * association that has this in it.
		 */
		targetObj.getMetaObject().getDeclaredMetaAssociationsParent()
			.stream()
			.filter(equalsColumnSchemaName(columnSchema.getName()))
			.forEach(ima -> targetObj.setValue(context, ima.getName(), value));
	}

	private static Predicate<IMetaAssociation> equalsColumnSchemaName(String csName) {
		return ima -> ima.getName()
			.substring(ima.getName().indexOf('.') + 1)
			.equals(csName);
	}

}
