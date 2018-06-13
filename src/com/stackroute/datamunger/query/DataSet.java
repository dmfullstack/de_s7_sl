package com.stackroute.datamunger.query;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.AggregateFunction;

//This class will be acting as the ResultSet containing multiple rows.
//The type (data type) of the result will different for different types of query.
//Like ,simple query, aggregate, group by etc.,

//You have to decide what should be data type for each type of query.
//You can use List<> OR Map<> based on requirement.

//Example:
//Ex:1 - For Simple Query(without aggregate, group by)
//  private List<List<String>> result;

//Ex:2 - For Group by
// 	private Map<String, List<List<String>>> groupByResult;

//Note:  There are few built in classes in java 8. We have to use them
//like DoubleSummaryStatistics
public class DataSet {
	
	private List<List<String>> result;

	private List<AggregateFunction> aggregateFunctions;

	//Group by without aggregate
	private Map<String, List<List<String>>> groupByResult;
	
	//Groupby with aggregate functions.
	private Map<String, DoubleSummaryStatistics> groupByAggregateResult;
	
	

	public Map<String, DoubleSummaryStatistics> getGroupByAggregateResult() {
		return groupByAggregateResult;
	}

	public void setGroupByAggregateResult(Map<String, DoubleSummaryStatistics> groupByAggregateResult) {
		this.groupByAggregateResult = groupByAggregateResult;
	}

	public Map<String, List<List<String>>> getGroupByResult() {
		return groupByResult;
	}

	public void setGroupByResult(Map<String, List<List<String>>> groupByResult) {
		this.groupByResult = groupByResult;
	}

	public List<List<String>> getResult() {
		return result;
	}

	public void setResult(List<List<String>> result) {
		this.result = result;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}	

}
