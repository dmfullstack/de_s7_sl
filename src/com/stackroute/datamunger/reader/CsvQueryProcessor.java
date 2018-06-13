package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * This class read the data from csv.
 * Filter the fields based on the query
 * Note:  Filter the fields logic can be written in another class(util class), 
 * so can be used in other query processors.
 **/

public class CsvQueryProcessor implements QueryProcessingEngine{
	
	private DataSet dataSet;
	private List<String> record;
	//header contains key as field name and value as index
	private Map<String, Integer> header;
	//Filter is user defined class which is used to filter the fields, filter the records.
	private Filter filter;

	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {
		dataSet = new DataSet();
		filter=new Filter();
		List<String> selectedFields = queryParameter.getFields();
		
		List<List<String>> result = new ArrayList<List<String>>();
			try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			//read header
			reader.readLine().split(",");
			String line;
			header = queryParameter.getHeader();
			// read the remaining records
			while ((line =reader.readLine()) != null) {
				//result.add(Arrays.asList(line.split(",")));
				record = Arrays.asList(line.split(","));			
				 //result.add(Arrays.asList(line.split(",")));
				if(!selectedFields.get(0).equals("*"))
				{
				    //filter the fields based on query parameters.
				 record=filter.filterFields(queryParameter,record);
				}
				 result.add(record);
			
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			dataSet.setResult(result);
		return dataSet;
	}

}
