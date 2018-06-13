package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

//This class will read from CSV file and process and return the resultSet based on Aggregate functions
//Need to use Java 8 concepts like streams and filters
//Need to use Java 8 built in classes like IntSummaryStatistics OR DoubleSummaryStatistics
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {

		private DataSet dataSet;
	    private List<AggregateFunction> aggregates;
			@Override
		public DataSet executeQuery(QueryParameter queryParameter) {
				dataSet = new DataSet();

				aggregates = queryParameter.getAggregateFunctions();
				Map<String, Integer> header = queryParameter.getHeader();
				
				List<List<String>> result = new ArrayList<List<String>>();
					try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
					//read header
					reader.readLine().split(",");
					String line;
					// read the remaining records
					while ((line =reader.readLine()) != null) {
						result.add(Arrays.asList(line.split(",")));
					
					}

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					
					// find min, max, sum and count based on given aggregate function
					calclulateAggregates(result,  header,aggregates);

				
					dataSet.setAggregateFunctions(aggregates);
					List<String> aggregateResults=new ArrayList<String>();
					for(AggregateFunction aggregate:aggregates) {
						aggregateResults.add(aggregate.getFunction()+"("+aggregate.getField()+") :"+aggregate.getResult());
					}
					List<List<String>> aggregateResultsList=new ArrayList<List<String>>();
					aggregateResultsList.add(aggregateResults);
					dataSet.setResult(aggregateResultsList);
				return dataSet;
			}
			
			/**
			 *This method is used the calculate aggregates like min, max, sum, avg, sum
			 * Used IntSummaryStatistics a java 8 built in class
			 * Used Streams and filters -  a java 8 concept
			 **/
			private List<AggregateFunction> calclulateAggregates(List<List<String>> result, Map<String, Integer> header,
					 			List<AggregateFunction> aggregates) {
					 		IntSummaryStatistics stats = null;
					 		int count = 0;
					 		for (AggregateFunction aggregate : aggregates) {
					 			
					 
					 			try {
					 				stats = result.stream()
					 						.mapToInt((record) -> Integer.parseInt(record.get(header.get(aggregate.getField()))))
					 						.summaryStatistics();
					 			} catch (NumberFormatException e) {
					 				count = (int) result.stream().filter(record -> !record.get(header.get(aggregate.getField())).isEmpty())
					 						.count();
					 
					 			}
					 
					 			switch (aggregate.getFunction()) {
					 			case "sum":
					 				aggregate.setResult((int) stats.getSum());
					 				break;
					 			case "max":
					 				aggregate.setResult((int) stats.getMax());
					 				break;
					 			case "min":
					 				aggregate.setResult((int) stats.getMin());
					 				break;
					 			case "count":
					 				if (stats == null) {
					 					aggregate.setResult(count);
					 				} else {
					 					aggregate.setResult((int) stats.getCount());
					 				}
					 				break;
					 			case "avg":
					 				aggregate.setResult((int) stats.getAverage());
					  			}
					 		}
					 			return aggregates;
					 	}
	
	
}
