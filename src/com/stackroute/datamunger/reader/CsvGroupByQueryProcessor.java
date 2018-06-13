package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

//This class will read from CSV file and process and return the resultSet based on Group by fields
//Need to use Java 8 concepts like streams and filters
//Need to use Java 8 built in classes like IntSummaryStatistics OR DoubleSummaryStatistics
public class CsvGroupByQueryProcessor implements QueryProcessingEngine {

	private DataSet dataSet;
	private List<String> record;
	private Map<String, Integer> header;
	private Filter filter;

	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {
		//Map<String, List<List<String>>> groupByResult;

		dataSet = new DataSet();
		header = queryParameter.getHeader();
		filter = new Filter();

		List<List<String>> result = new ArrayList<List<String>>();
		List<String> selectedFields = queryParameter.getFields();
		List<AggregateFunction> aggregateFunctions = queryParameter.getAggregateFunctions();
		
		try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read header
			reader.readLine().split(",");
			String line;
			// read the remaining records
			while ((line = reader.readLine()) != null) {
				
				record = Arrays.asList(line.split(","));

				if (filter.isRequiredRecord(queryParameter, record)) {
					if(aggregateFunctions!=null) {
					if (!selectedFields.get(0).equals("*") && aggregateFunctions.isEmpty()) {
						record = filter.filterFields(queryParameter, record);
					}
					}
					result.add(record);
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (queryParameter.getFields() == null || queryParameter.getAggregateFunctions() == null) {
			dataSet.setGroupByResult(calclulateGroupByWithFields(result, queryParameter));

		} else {
			dataSet.setGroupByAggregateResult(calclulateGroupByWithAggregates(result, queryParameter));

		}

		return dataSet;
	}
	
	/**
	 * This method is used to calculate group by summary with fields.  It means along with group by, some other fields
	 * also present in given query.
	 **/

	private Map<String, List<List<String>>> calclulateGroupByWithFields(List<List<String>> result,
			QueryParameter queryParameter) {
		// Map<String, Integer> header = queryParameter.getHeader();
		String groupByField = queryParameter.getGroupByFields().get(0);
		List<String> selectedFields = queryParameter.getFields();
		int groupByFieldIndex = getGroupByFieldIndex(selectedFields, groupByField);

		Map<String, List<List<String>>> groupBy = result.stream()
				.collect(Collectors.groupingBy((record) -> record.get(groupByFieldIndex)));

		return groupBy;
	}
/**
 * This method is use to get index/position of group by field
 **/
	private int getGroupByFieldIndex(List<String> selectedFields, String groupByField) {
		int noOfFields = selectedFields.size();
		for (int index = 0; index < noOfFields; index++) {
			if (selectedFields.get(index).equals(groupByField)) {
				return index;
			}
		}
		
		return 0;
	}

	/**
	 * This method is used to calculate group by  along with other aggregate functions like min, max, avg, sum, count.
	 * Used streaps and filters of java 8
	 * Used DoubleSummaryStatistics a java 8 built-in class to store aggregate functions
	 **/
	private Map<String, DoubleSummaryStatistics> calclulateGroupByWithAggregates(List<List<String>> result,
			QueryParameter queryParameter) {

		Map<String, Integer> header = queryParameter.getHeader();
		int groupByFieldIndex = header.get(queryParameter.getGroupByFields().get(0));
		List<AggregateFunction> aggregates = queryParameter.getAggregateFunctions();
		AggregateFunction aggregate = aggregates.get(0); // only one aggregate
		int aggregateFieldIndex;
		if (aggregate.getField().equals("*")) {
			aggregateFieldIndex = 0;
		} else {
			aggregateFieldIndex = header.get(aggregate.getField());

		}

		Map<String, DoubleSummaryStatistics> groupByStatistics = null;
		try {
			groupByStatistics = result.stream().collect(Collectors.groupingBy((record) -> record.get(groupByFieldIndex),
					Collectors.summarizingDouble(record -> Integer.parseInt(record.get(aggregateFieldIndex)))));
		} catch (NumberFormatException e) {

			e.printStackTrace();
		}

		return groupByStatistics;
	}

}
