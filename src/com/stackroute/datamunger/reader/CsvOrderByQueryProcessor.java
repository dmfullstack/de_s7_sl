package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * This class will read data from CSV file and sort based on order by field.
 * 
 */
public class CsvOrderByQueryProcessor implements QueryProcessingEngine{
	
	private List<String> record;
	private Filter filter;
	
	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {
		List<String> selectedFields = queryParameter.getFields();
		DataSet dataSet = new DataSet();
		filter=new Filter();
		List<List<String>> result = new ArrayList<List<String>>();
		List<List<String>> orderedResult = new ArrayList<List<String>>();
		
			try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			//read header
			reader.readLine().split(",");
			String line;
			// read the remaining records
			while ((line =reader.readLine()) != null) {
				
				record = Arrays.asList(line.split(","));

				if (filter.isRequiredRecord(queryParameter, record)) {
					result.add(record);
				}
			
			}
           
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			result = sortRecords(queryParameter,result);
			for(List<String> record:result) {
				if(!selectedFields.get(0).equals("*"))
				{
				 record=filter.filterFields(queryParameter,record);
				}
				orderedResult.add(record);
			}
			dataSet.setResult(orderedResult);
		return dataSet;
	}

	private List<List<String>> sortRecords(QueryParameter queryParameter, List<List<String>> result) {
		
		filter=new Filter();
		int orderByIndex =queryParameter.getHeader().get(queryParameter.getOrderByFields().get(0));
		result.sort((r1, r2) -> r1.get(orderByIndex).compareTo(r2.get(orderByIndex)));
		
		return result;
	}


}
