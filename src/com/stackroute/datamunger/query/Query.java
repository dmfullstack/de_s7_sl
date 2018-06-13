package com.stackroute.datamunger.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvOrderByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;
import com.stackroute.datamunger.reader.CsvWhereQueryProcessor;
import com.stackroute.datamunger.reader.QueryProcessingEngine;

/**
 * This class is used to execute the query with the help of different Query Processors.
 * Decide what type of Query processors you required.
 * Hint:  For each different query type you need different Query Procesor.
 * You can write any private methods if required(to be used in this class only)
 **/
public class Query {

	QueryParser queryParser = null;
	QueryParameter queryParameter = null;
	private Map<String, Integer> header;

	public DataSet executeQuery(String queryString) {
		
		queryParser = new QueryParser();
		queryParameter = queryParser.parseQuery(queryString);
		setHeader();
		QueryProcessingEngine queryEngine = null;

		// checking type of Query
		switch (queryParameter.getQUERY_TYPE()) {
		// queries without aggregate functions, order by clause or group by clause
		case "SIMPLE_QUERY":
			queryEngine = new CsvQueryProcessor();
			break;
		case "WHERE_CLAUSE_QUERY":
			queryEngine = new CsvWhereQueryProcessor();
			break;
		case "ORDER_BY_QUERY":
			queryEngine = new CsvOrderByQueryProcessor();
			break;
		//queries with aggregate functions
		case "AGGREGATE_QUERY":
			queryEngine=new CsvAggregateQueryProcessor();
			break;
		//Queries with group by clause
		case "GROUP_BY_ORDER_BY_QUERY":
			queryEngine = new CsvGroupByQueryProcessor();
			break;
		case "GROUP_BY_QUERY":
			queryEngine=new CsvGroupByQueryProcessor();

		}

		return queryEngine.executeQuery(queryParameter);
	}
	
	
	private void setHeader() {
		header = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read the header record
			List<String> record = Arrays.asList(reader.readLine().split(","));
			int columnSize = record.size();
			for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
				header.put(record.get(columnIndex), columnIndex);
			}

			queryParameter.setHeader(header);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void setSelectedFieldIndex() {
		if (queryParameter.getFields() != null) {

			List<String> selectedFields = queryParameter.getFields();
			List<Integer> selectedFieldIndexes = new ArrayList<Integer>();

			selectedFields.forEach(field -> {
				selectedFieldIndexes.add(header.get(field));
			});

			queryParameter.setSelectedFieldIndexes(selectedFieldIndexes);

		}

	}

}
