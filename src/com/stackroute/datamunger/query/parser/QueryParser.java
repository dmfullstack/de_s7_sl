package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;
/**
 *This class is used to the parse the query. 
 * It means split the query into various tokens and extract file name, selected fields, where condition
 * group by field, aggregate field, order by field
 * Also decide what type of query it is and set to QueryParameter instance.
 * Once the QueryParameter instance is created, this object can be used to execute the query.
 * 
 **/
public class QueryParser {

	private QueryParameter queryParameter = new QueryParameter();

/**
 *This method used to parse the query. (See the comment above the class)
 * Note:  Better to write separate private method to extract each part of the query
 * Ex: private List<String> getFields(String baseQuery) 
 * Ex: private List<String> getLogicalOperators()
 * etc.,
 * This is the way you can achieve modularity and readability.
 **/
	public QueryParameter parseQuery(String queryString) {
		queryParameter.setQueryString(queryString);
		queryParameter.setQUERY_TYPE("SIMPLE_QUERY");
		String baseQuery = queryString.split("where|ordery by|group by")[0].trim();
		queryParameter.setBaseQuery(baseQuery);

		String file = baseQuery.split("from")[1].trim().split("\\s+")[0];
		queryParameter.setFile(file.trim());
		queryParameter.setFields(getFields(baseQuery));
		queryParameter.setRestrictions(getRescritions());
		queryParameter.setLogicalOperators(getLogicalOperators());
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));

		return queryParameter;
	}

	private List<String> getOrderByFields(String queryString) {
		List<String> orderByFieldList=null;
		if(hasOrderByField(queryString))
		{
			String orderByFields[] = queryString.split("\\s+order by\\s+")[1].split("\\s+group by\\s+")[0].split(",");
			orderByFieldList = new ArrayList<>();
			for (String  orderByField : orderByFields) {
				orderByFieldList.add(orderByField);
			}
		}
		
		return orderByFieldList;
	}
	
	private List<String> getGroupByFields(String queryString) {
		List<String> groupByFieldList=null;
		if(hasGroupByField(queryString))
		{
			String groupByFields[] = queryString.split("\\s+group by\\s+")[1].split("\\s+order by\\s+")[0].split(",");
			groupByFieldList = new ArrayList<>();
			for (String  groupByField : groupByFields) {
				groupByFieldList.add(groupByField);
			}
		}
		
		return groupByFieldList;
	}

	private boolean hasOrderByField(String queryString) {
		if(queryString.contains("order by"))
		{
			if(queryParameter.getQUERY_TYPE().equals("GROUP_BY_QUERY"))
			{
				queryParameter.setQUERY_TYPE("GROUP_BY_ORDER_BY_QUERY");
			}
			else
			{
				queryParameter.setQUERY_TYPE("ORDER_BY_QUERY");
			}
			
			return true;
		}
		else
		{
			return false;
		}
		
	}
	
	private boolean hasGroupByField(String queryString) {
		if(queryString.contains("group by"))
		{
			if(queryParameter.getQUERY_TYPE().equals("ORDER_BY_QUERY"))
			{
				queryParameter.setQUERY_TYPE("GROUP_BY_ORDER_BY_QUERY");
			}
			else
			{
				queryParameter.setQUERY_TYPE("GROUP_BY_QUERY");
			}
			
			return true;
		}
		else
		{
			return false;
		}
		
	}

	private List<String> getFields(String baseQuery) {
        //will use arrays.asList???
		String[] fields = baseQuery.trim().split("select")[1].split("from")[0].trim().split(",");
		
		List<String> fieldList = new ArrayList<>();
		for (String field : fields) {
			/*if(field.contains("sum")||field.contains("count")||field.contains("min")||field.contains("max")||field.contains("avg")) {
				continue;
			}*/
			fieldList.add(field.trim());
		}
		return fieldList;
	}

	private List<Restriction> getRescritions() {
		// select * from table where field='val'
		// extract where conditions
		String queryString = queryParameter.getQueryString();

		List<Restriction> restrictions = null;
		if (queryString.contains("where")) {
			queryParameter.setQUERY_TYPE("WHERE_CLAUSE_QUERY");
			
			String whereClauseQuery = queryString.split("where")[1].split("order by")[0].split("group by")[0];

			String[] expressions = whereClauseQuery.split("\\s+and\\s+|\\s+or\\s+");

			String propertyName;
			String propertyValue;
			String condition;
			Restriction restriction;
			String propertyNameAndValue[];
			restrictions = new ArrayList<Restriction>();
			if (whereClauseQuery != null) {
				for (String expression : expressions) {
					expression = expression.trim();
					// propertyNameAndValue =
					// expression.split("\\s+<\\s+=|\\s+>\\s+=\\s+|\\s+!\\s+=\\s+|\\s+<\\s+|\\s+>\\s+");
					propertyNameAndValue = expression.split("<=|>=|!=|<|>|=");
					propertyName = propertyNameAndValue[0].trim();
					propertyValue = propertyNameAndValue[1].trim().replace("'", "");
					// salary<10000
					condition = expression.split(propertyName)[1].trim().split(propertyValue)[0].replace("'", "");

					restriction = new Restriction(propertyName, propertyValue, condition);
					restrictions.add(restriction);
				}
			}

		}
		return restrictions;

	}

	private List<String> getLogicalOperators() {
		List<String> logicalOperators = null;
		String queryString = queryParameter.getQueryString();
		if (queryString.contains("where")) {
			String whereClauseQuery = queryString.split("where")[1].split("group by|order by")[0];

			String[] expressions = whereClauseQuery.split("\\s+and\\s+|\\s+or\\s+");

			logicalOperators = new ArrayList<String>();

			int size = expressions.length;
			int i = 0;
			for (String expression : expressions) {
				if (i++ < size - 1)
					logicalOperators.add(whereClauseQuery.split(expression.trim())[1].split("\\s+")[1].trim());

			}

		}
		return logicalOperators;
	}

	private List<AggregateFunction> getAggregateFunctions(String queryString) {
		if (hasAggregateFunctions(queryString)) {

			queryString = queryString.trim();
			String aggregateFunctions[] = queryString.split("from")[0].split("select")[1].split(",");
			int size = aggregateFunctions.length;
			String aggregate;
			String function;
			String aggregateField;
			List<AggregateFunction> agregateFunctionList = new ArrayList<AggregateFunction>();
			AggregateFunction agregateFunction;
			
			for (int i = 0; i < size; i++) {
				aggregate = aggregateFunctions[i].trim();
				if(aggregate.contains("("))
				{
					function = aggregate.split("\\(")[0].trim();
					aggregateField = aggregate.split("\\(")[1].trim().split("\\)")[0];
					agregateFunction = new AggregateFunction();
					agregateFunction.setField(aggregateField);
					agregateFunction.setFunction(function);
					agregateFunctionList.add(agregateFunction);
				}
				

			}

			return agregateFunctionList;
		}
		return null;

	}

	private boolean hasAggregateFunctions(String queryString) {

		if (queryString.contains("sum") || queryString.contains("min") || queryString.contains("max")
				|| queryString.contains("avg") || queryString.contains("count")) {
			queryParameter.setQUERY_TYPE("AGGREGATE_QUERY");
			return true;
		}
		return false;
	}

}
