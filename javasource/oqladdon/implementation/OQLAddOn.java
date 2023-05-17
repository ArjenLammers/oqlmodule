package oqladdon.implementation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTable;
import com.mendix.systemwideinterfaces.connectionbus.requests.IParameterMap;
import com.mendix.systemwideinterfaces.connectionbus.requests.types.IOQLTextGetRequest;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import oql.implementation.OQL;

public class OQLAddOn {

	private static ILogNode logger = Core.getLogger(oqladdon.proxies.LogNodes.OQLAddOn.toString());

	public static List<IMendixObject> executeOQLQuery(IContext ctx, String query, boolean preserveParameters)
			throws CoreException {
		IOQLTextGetRequest request = Core.createOQLTextGetRequest();

		logger.debug("Mapping parameters.");
		String statement = query;
		
		IParameterMap parameterMap = request.createParameterMap();

		for (Entry<String, Object> entry : OQL.getNextParameters().entrySet()) {
			if (entry.getValue() instanceof ArrayList<?>) {
				// When it is a list
				ArrayList<?> list = (ArrayList<?>) entry.getValue();

				// Update the query with safe parameters
				List<String> newQueryParameters = IntStream.range(0, list.size()).mapToObj(i -> "\\$" + entry.getKey() + "_" + i).collect(Collectors.toList());
				String updatedParameter = String.join(",", newQueryParameters);
				
				// Replace the current parameter with the new list of parameters
				//e.g. convert 	$EmployeeList 
				// into 		$EmployeeList_0, $EmployeeList_1, $EmployeeList_2, $EmployeeList_3
				statement = statement.replaceAll("\\$" + entry.getKey(), updatedParameter);
				
				// Construct the parameternames and then map the list to the parametermap
				List<String> newParameterMapNames = IntStream.range(0, list.size()).mapToObj(i -> entry.getKey() + "_" + i).collect(Collectors.toList());
				for (int i = 0; i < list.size(); i++) {
					parameterMap.put(newParameterMapNames.get(i), list.get(i));	
				}
				
			} else {
				
				// Just a normal parameter, add it to the parameter map.
				parameterMap.put(entry.getKey(), entry.getValue());
			}
		}
		
		request.setQuery(statement);

		request.setParameters(parameterMap);

		logger.debug(String.format("Executing OQL: `%s`",request.getQuery()));
		IDataTable resultDT = Core.retrieveOQLDataTable(ctx, request);

		List<IMendixIdentifier> idlist = new ArrayList<IMendixIdentifier>();

		logger.debug("Processing result");
		resultDT.forEach(row -> {
			idlist.add(row.getValue(ctx, 0));
		});

		if (!preserveParameters)
			OQL.resetParameters();

		logger.debug("Returning result");
		return Core.retrieveIdList(ctx, idlist);
	}
}
