package oql.implementation;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.MendixRuntimeException;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataColumnSchema;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTable;
import com.mendix.systemwideinterfaces.connectionbus.requests.IRetrievalSchema;
import com.mendix.systemwideinterfaces.connectionbus.requests.types.IOQLTextGetRequest;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaAssociation;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class OQL {

	private static final ThreadLocal<Map<String, Object>> NEXT_PARAMETERS = new ThreadLocal<Map<String, Object>>();

	private static final ILogNode LOGGER = Core.getLogger(OQL.class.getSimpleName());

	public static Map<String, Object> getNextParameters() {
		if (NEXT_PARAMETERS.get() == null) {
			resetParameters();
		}
		return NEXT_PARAMETERS.get();
	}

	public static void resetParameters() {
		NEXT_PARAMETERS.set(new HashMap<>());
	}

	public static void addParameter(String name, Object value) {
		getNextParameters().put(name, value);
	}

	public static Long countRowsOQL(IContext context, String statement, Long amount) throws CoreException {
		IOQLTextGetRequest request = prepareRequest(statement, amount, 0l);

		var results = Core.retrieveOQLDataTable(context, request);
		return (long) results.getRowCount();
	}

	static IOQLTextGetRequest prepareRequest(String statement, Long amount, Long offset) {
		LOGGER.debug(String.format("Preparing query %s", statement));
		var request = Core.createOQLTextGetRequest();
		request.setQuery(statement);
		var parameterMap = request.createParameterMap();
		OQL.getNextParameters().forEach(parameterMap::put); // IParameterMap does not support putAll().
		request.setParameters(parameterMap);

		IRetrievalSchema schema = Core.createRetrievalSchema();
		schema.setAmount(amount);
		schema.setOffset(offset);
		request.setRetrievalSchema(schema);

		return request;
	}

	public static List<IMendixObject> executeOQL(IContext context, String statement, String returnEntity, Long amount, Long offset) throws CoreException {
		var request = prepareRequest(statement, amount, offset);

		var results = Core.retrieveOQLDataTable(context, request);

		return mapDataRowsToObjectList(context, results, returnEntity);
	}

	static List<IMendixObject> mapDataRowsToObjectList(IContext context, IDataTable dataTable, String returnEntity) throws CoreException {

		LOGGER.debug(String.format("Mapping %d results.", dataTable.getRowCount()));

		List<IMendixObject> result = new ArrayList<>();

		dataTable.getRows().forEach(row -> {

			var targetObj = Core.instantiate(context, returnEntity);
			LOGGER.trace(String.format("targetObject type: %s", targetObj.getMetaObject().getName()));

			dataTable.getSchema().getColumnSchemas().forEach(columnSchema -> {
				LOGGER.trace(String.format("Mapping column %s", columnSchema.getName()));

				Optional.ofNullable(row.getValue(context, columnSchema))
					.ifPresentOrElse(
						value -> copyValueToTarget(value, targetObj, columnSchema, context),
						() -> LOGGER.warn(String.format("Skipping null value for column %s", columnSchema.getName())));
			});
			result.add(targetObj);
		});

		return result;
	}

	static void copyValueToTarget(Object value, IMendixObject targetObj, IDataColumnSchema columnSchema, IContext context) {
		if (value instanceof IMendixIdentifier) {
			copyAssociationValueToTarget(value, targetObj, columnSchema, context);
		} else {
			copyPrimitiveValueToTarget(value, targetObj, columnSchema, context);
		}
	}

	static void copyPrimitiveValueToTarget(Object value, IMendixObject targetObj, IDataColumnSchema columnSchema, IContext context) throws NumberFormatException {
		LOGGER.trace("Treating as value");

		var optPrimitive = Optional.ofNullable(targetObj.getMetaObject().getMetaPrimitive(columnSchema.getName()));
		var primitive = optPrimitive.orElseThrow(
			() -> new MendixRuntimeException(String.format("Could not find result attribute %s in target object", columnSchema.getName()))
		);

		if (value instanceof Integer && primitive.getType() == PrimitiveType.Long) {
			value = (Long) ((Integer) value).longValue();
		} else if (value instanceof Long && primitive.getType() == PrimitiveType.Integer) {
			value = Math.toIntExact((Long) value);
		}
		targetObj.setValue(context, columnSchema.getName(), value);
	}

	static void copyAssociationValueToTarget(Object value, IMendixObject targetObj, IDataColumnSchema columnSchema, IContext context) {
		LOGGER.trace("Treating as association");
		/* Escaping an alias as described at https://docs.mendix.com/refguide7/oql-select-clause
		 * leads to an error when using dots e.g. (OQL.ExamplePerson_ExamplePersonResult).
		 * Therefore this action accepts the ExamplePerson_ExamplePersonResult part and searches for the
		 * association that has this in it.
		 */
		targetObj.getMetaObject().getDeclaredMetaAssociationsParent()
			.stream()
			.filter(equalsColumnSchemaName(columnSchema.getName()))
			.peek(ima -> targetObj.setValue(context, ima.getName(), value))
			.findAny()
			.orElseThrow(() -> new MendixRuntimeException(String.format("Could not find result association %s in target object", columnSchema.getName())));
	}

	private static Predicate<IMetaAssociation> equalsColumnSchemaName(String csName) {
		return ima -> ima.getName()
			.substring(ima.getName().indexOf('.') + 1)
			.equals(csName);
	}

}
