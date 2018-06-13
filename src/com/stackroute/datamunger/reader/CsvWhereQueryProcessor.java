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
 *This class will read from CSV file and process and return the resultSet based on Where clause
*Filter is user defined utility class where we defined the methods related to filter fields,
* filter records.
 **/
public class CsvWhereQueryProcessor implements QueryProcessingEngine{

	private DataSet dataSet;
	private List<String> record;
	private Map<String, Integer> header;
	private Filter filter;

/**
 *This method used to read the data from csv and filter the fields with the help of  Filter class.
 **/
	public DataSet executeQuery(QueryParameter queryParameter) {
		dataSet = new DataSet();
		header = queryParameter.getHeader();
		filter = new Filter();
		List<List<String>> result = new ArrayList<List<String>>();
		List<String> selectedFields = queryParameter.getFields();

		try (BufferedReader reader = new BufferedReader(new FileReader(queryParameter.getFile()))) {
			// read header
			reader.readLine().split(",");
			String line;
			// read the remaining records
			while ((line = reader.readLine()) != null) {
				record = Arrays.asList(line.split(","));

                //Check wither the record is required or not based on 'where' coniditin in the query parameter
				if (filter.isRequiredRecord(queryParameter, record)) {
					if (!selectedFields.get(0).equals("*")) {
					    //filter the fields based on query(selected fields)
						record = filter.filterFields(queryParameter, record);
					}
					result.add(record);
				}
				
				

				

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dataSet.setResult(result);
		return dataSet;
	}

}
