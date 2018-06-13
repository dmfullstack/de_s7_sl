package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

//This class is like utility class.
//We can define common methods which can be used acrross the project.
//Like filterFields, isRequiredRecord etc.,
//You can write private methods to do subtask of the above methods.
public class Filter {
	
	private Map<String, Integer> header;
	
	/**
	 * This method take the record and remove the fields from the record.
	 * The record consist of all the fields, you have to keep what use selected in the query.
	 */
	public List<String> filterFields(QueryParameter queryParameter, List<String> record) {
		
		List<String> selectedFields= queryParameter.getFields();
		header = queryParameter.getHeader();
	
		int fieldPosition;
		List<String> filteredRecord = new ArrayList<String>();
		for (String field : selectedFields) {
			fieldPosition =header.get(field);
			filteredRecord.add(record.get(fieldPosition));
			
		}
		return filteredRecord;

	}
	
	/**
	 *This method takes record as input and return true if the record is required based on 'where'  condition given in the query.
	 * Multiple conditions may be exist in "where" part of the query.  
	 **/

	public boolean isRequiredRecord(QueryParameter queryParameter, List<String> record) {
		header = queryParameter.getHeader();
		List<Restriction> restrictions = queryParameter.getRestrictions();
		List<String> logicationOperators = queryParameter.getLogicalOperators();
		Map<String, Integer> header = queryParameter.getHeader();
		boolean flag = true;
		int expressionPosotion = 0;
		if (restrictions != null && !restrictions.isEmpty()) {
			// evaluate first expression
			Restriction restriction = restrictions.get(expressionPosotion);
			flag = evaluateRelationalExpression(record, restriction);

			// evaluate remaining expressions
			for (String logicationOperator : logicationOperators) {
				expressionPosotion++;
				switch (logicationOperator) {
				case "and":
					restriction = restrictions.get(expressionPosotion);
					flag = flag && evaluateRelationalExpression(record, restriction);
					break;
				case "or":
					restriction = restrictions.get(expressionPosotion);
					flag = flag || evaluateRelationalExpression(record, restriction);
					break;
				case "not":
					restriction = restrictions.get(expressionPosotion);
					flag = flag && !evaluateRelationalExpression(record, restriction);
				}
			}

		}
		return flag;

	}

	/**/

	private boolean evaluateRelationalExpression(List<String> record, Restriction restriction) {
		boolean flag = true;
		String condition = restriction.getCondition();
		String propertyName = restriction.getPropertyName();

		String whereConditionValue = restriction.getPropertyValue();

		int propertPosition = header.get(propertyName);
		String recordValue = record.get(propertPosition);

		switch (condition.trim()) {
		case "=":
			if (whereConditionValue.equalsIgnoreCase(recordValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}
			break;
		case "!=":

			if (!whereConditionValue.equalsIgnoreCase(recordValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}
			break;

		case ">":

			if (Double.parseDouble(recordValue) > Double.parseDouble(whereConditionValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}

			break;
			
		case ">=":

			if (Double.parseDouble(recordValue) >= Double.parseDouble(whereConditionValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}

			break;
			
		case "<=":

			if (Double.parseDouble(recordValue) <= Double.parseDouble(whereConditionValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}

			break;

		case "<":

			if (Double.parseDouble(recordValue) < Double.parseDouble(whereConditionValue)) {
				flag = flag && true;
			} else {
				flag = flag && false;
			}

		}
		
		

		return flag;
	}

}
