package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * QueryProcessingEngine is the interface which has a method executeQuery(QueryParameter queryParameter)
 * and one method getHeader with definition.
 * Note: In java 8 onwards we can have defult implementation.
 * The methods which are common the all the implemented classes can be define using 'default'
 * This interface implemented by all the different Query Processors
 **/
public interface QueryProcessingEngine {
	
	public DataSet executeQuery(QueryParameter queryParameter);
	
	
	//Read the header ( i.e., firt row) from the csv file.
	 default List<String> getHeader(String file)
	 {
				try (BufferedReader br = new BufferedReader(new FileReader(file))) {

					String line;
					if ((line = br.readLine()) != null) {
						return Arrays.asList(line.split(","));
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			return null;

	 }

}
