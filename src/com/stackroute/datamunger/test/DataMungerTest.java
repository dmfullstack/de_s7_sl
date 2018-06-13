package com.stackroute.datamunger.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.query.parser.Restriction;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataMungerTest{

	private static Query query;

	private static QueryParser queryParser;
	private static QueryParameter queryParameter;
	private String queryString;

	@Before
	public void setup() {
		// setup methods runs, before every test case runs
		// This method is used to initialize the required variables
		query = new Query();
		queryParser = new QueryParser();
	}

	@After
	public void teardown() {
		// teardown method runs, after every test case run
		// This method is used to clear the initialized variables
		query = null;
		queryParser = null;

	}

	/*
	 * The following test cases are used to check whether the parsing is working
	 * properly or not
	 */

	@Test
	public void testGetFileName() {
		queryString = "select * from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetFileName() : Parsing the query returns null object", queryParameter);
		assertEquals(
				"testGetFileName(): File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFileNameFailure() {
		queryString = "select * from data/ipl1.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetFileNameFailure() : Parsing the query returns null object", queryParameter);
		assertNotEquals(
				"testGetFileNameFailure(): File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFields() {
		queryString = "select city, winner, team1,team2 from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetFields() : Parsing the query returns null object", queryParameter);
		List<String> expectedFields = new ArrayList<>();
		expectedFields.add("city");
		expectedFields.add("winner");
		expectedFields.add("team1");
		expectedFields.add("team2");
		assertEquals(
				"testGetFields() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				expectedFields, queryParameter.getFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFieldsFailure() {
		queryString = "select city, winner, team1,team2 from data/ipl.csv";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetFieldsFailure() : Parsing the query returns null object", queryParameter);
		assertNotNull(
				"testGetFieldsFailure() : Invalid Column / Field values. Please note that the query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				queryParameter.getFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetFieldsAndRestrictions() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetFieldsAndRestrictions() : Parsing the query returns null object", queryParameter);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetFieldsAndRestrictions() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetFieldsAndRestrictions() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetFieldsAndRestrictions() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());
		assertTrue(
				"testGetFieldsAndRestrictions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string. ",
				restrictions.get(0).getPropertyName().toString().contains("season"));
		assertTrue(
				"testGetFieldsAndRestrictions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string. ",
				restrictions.get(0).getPropertyValue().toString().contains("2014"));
		assertTrue(
				"testGetFieldsAndRestrictions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string. ",
				restrictions.get(0).getCondition().toString().contains(">"));

		display(queryString, queryParameter);
	}

	@Test
	public void testGetFieldsAndRestrictionsFailure() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetFieldsAndRestrictionsFailure() : Parsing the query returns null object", queryParameter);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(
				"testGetFieldsAndRestrictionsFailure() : Hint: extract the conditions from the query string(if exists). for each condition, we need to capture the following: 1. Name of field, 2. condition, 3. value, please note the query might contain multiple conditions separated by OR/AND operators",
				restrictions);

		display(queryString, queryParameter);
	}

	@Test
	public void testGetRestrictionsAndAggregateFunctions() {
		boolean aggregatestatus = false;

		queryString = "select count(city),sum(win_by_runs),min(season),max(win_by_wickets) from data/ipl.csv where season > 2014 and city ='Bangalore'";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetRestrictionsAndAggregateFunctions() : Parsing the query returns null object",
				queryParameter);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		List<AggregateFunction> aggfunction = queryParameter.getAggregateFunctions();

		assertNotNull("testGetRestrictionsAndAggregateFunctions() : Empty Restrictions list", restrictions);
		assertNotNull("testGetRestrictionsAndAggregateFunctions() : Empty Aggregates list", aggfunction);

		List<String> fields = new ArrayList<String>();
		fields.add("count(city)");
		fields.add("sum(win_by_runs)");
		fields.add("min(season)");
		fields.add("max(win_by_wickets)");

		List<String> logicalop = new ArrayList<String>();
		logicalop.add("and");

		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select count(city),sum(win_by_runs),min(season),max(win_by_wickets) from data/ipl.csv",
				queryParameter.getBaseQuery());

		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyName().toString().contains("season"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyValue().toString().contains("2014"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getCondition().toString().contains(">"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyName().toString().contains("city"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyValue().toString().contains("Bangalore"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getCondition().contains("="));

		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(0).getFunction().contains("count"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(1).getFunction().contains("sum"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(2).getFunction().contains("min"));
		assertTrue(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Aggregate part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				aggfunction.get(3).getFunction().contains("max"));

		assertEquals(
				"testGetRestrictionsAndAggregateFunctions() : Retrieval of Logical Operators failed. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc.",
				logicalop, queryParameter.getLogicalOperators());

		display(queryString, queryParameter);
	}

	@Test
	public void testGetGroupByOrderByClause() {

		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city='Bangalore' group by winner order by city";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetGroupByOrderByClause() : Parsing the query returns null object", queryParameter);

		List<Restriction> restrictions = queryParameter.getRestrictions();

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		List<String> logicalop = new ArrayList<String>();
		logicalop.add("and");

		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("city");

		List<String> groupByFields = new ArrayList<String>();
		groupByFields.add("winner");

		assertEquals(
				"testGetGroupByOrderByClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetGroupByOrderByClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetGroupByOrderByClause() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());

		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyName().contains("season"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyValue().contains("2014"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getCondition().contains(">"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyName().contains("city"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyValue().contains("Bangalore"));
		assertTrue(
				"testGetGroupByOrderByClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getCondition().contains("="));

		assertEquals(
				"testGetGroupByOrderByClause() : Retrieval of Logical Operators failed. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc.",
				logicalop, queryParameter.getLogicalOperators());

		assertEquals(
				"testGetGroupByOrderByClause() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				groupByFields, queryParameter.getGroupByFields());

		assertEquals(
				"testGetGroupByOrderByClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());

		display(queryString, queryParameter);

	}

	@Test
	public void testGetGroupByOrderByClauseFailure() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city='Bangalore' group by winner order by city";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetGroupByOrderByClauseFailure() : Parsing the query returns null object", queryParameter);

		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(
				"testGetGroupByOrderByClauseFailure() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				queryParameter.getGroupByFields());
		assertNotNull(
				"testGetGroupByOrderByClauseFailure() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists.",
				queryParameter.getOrderByFields());
		assertNotNull(
				"testGetGroupByOrderByClauseFailure() : Hint: extract the conditions from the query string(if exists). for each condition, we need to capture the following: 1. Name of field, 2. condition, 3. value, please note the query might contain multiple conditions separated by OR/AND operators",
				restrictions);

		display(queryString, queryParameter);
	}

	@Test
	public void testGetGroupByClause() {
		queryString = "select city,winner,player_of_match from data/ipl.csv group by city";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetGroupByClause() : Parsing the query returns null object", queryParameter);

		List<String> fields = new ArrayList<String>();
		fields.add("city");

		assertEquals(
				"testGetGroupByClause() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				fields, queryParameter.getGroupByFields());
		assertNotNull(
				"testGetGroupByClause() : Hint: Check getGroupByFields() method. The query string can contain more than one group by fields. it is also possible thant the query string might not contain group by clause at all. The field names, condition values might contain 'group' as a substring. For eg: newsgroup_name",
				queryParameter.getGroupByFields());

		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetGroupByClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetGroupByClause() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());
		assertEquals(
				"testGetGroupByClause() : Retrieval of conditions part is not returning null. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string",
				null, queryParameter.getRestrictions());
		assertEquals(
				"testGetGroupByClause() : Logical Operators should be null. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc",
				null, queryParameter.getLogicalOperators());

		display(queryString, queryParameter);

	}

	@Test
	public void testGetOrderByAndWhereConditionClause() {

		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city ='Bangalore' order by city";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetOrderByAndWhereConditionClause() : Parsing the query returns null object",
				queryParameter);
		List<Restriction> restrictions = queryParameter.getRestrictions();

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		List<String> logicalop = new ArrayList<String>();
		logicalop.add("and");

		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("city");

		assertEquals(
				"testGetOrderByAndWhereConditionClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Retrieval of Base Query failed. BaseQuery contains from the beginning of the query till the where clause",
				"select city,winner,player_of_match from data/ipl.csv", queryParameter.getBaseQuery());

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyName().contains("season"));

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getPropertyValue().contains("2014"));

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(0).getCondition().contains(">"));

		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyName().contains("city"));
		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getPropertyValue().contains("Bangalore"));
		assertTrue(
				"testGetOrderByAndWhereConditionClause() : Retrieval of conditions part failed. The conditions part contains starting from where keyword till the next keyword, which is either group by or order by clause. In case of absence of both group by and order by clause, it will contain till the end of the query string.",
				restrictions.get(1).getCondition().contains("="));

		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Retrieval of Logical Operators failed. AND/OR keyword will exist in the query only if where conditions exists and it contains multiple conditions.The extracted logical operators will be stored in a String array which will be returned by the method. Please note that AND/OR can exist as a substring in the conditions as well. For eg: name='Alexander',color='Red' etc.",
				logicalop, queryParameter.getLogicalOperators());

		assertEquals(
				"testGetOrderByAndWhereConditionClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());

		display(queryString, queryParameter);

	}

	@Test
	public void testGetOrderByAndWhereConditionClauseFailure() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where season > 2014 and city ='Bangalore' order by city";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetOrderByAndWhereConditionClauseFailure() : Parsing the query returns null object",
				queryParameter);
		List<Restriction> restrictions = queryParameter.getRestrictions();
		assertNotNull(
				"testGetOrderByAndWhereConditionClauseFailure() : Hint: extract the conditions from the query string(if exists). for each condition, we need to capture the following: 1. Name of field, 2. condition, 3. value, please note the query might contain multiple conditions separated by OR/AND operators",
				restrictions);
		assertNotNull(
				"testGetOrderByAndWhereConditionClauseFailure() :Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists.",
				queryParameter.getOrderByFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetOrderByClause() {
		queryString = "select city,winner,player_of_match from data/ipl.csv where city='Bangalore' order by winner";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetOrderByClause() : Parsing the query returns null object", queryParameter);
		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("winner");

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetOrderByClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetOrderByClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetOrderByClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());
		display(queryString, queryParameter);
	}

	@Test
	public void testGetOrderByWithoutWhereClause() {
		queryString = "select city,winner,player_of_match from data/ipl.csv order by city";
		queryParameter = queryParser.parseQuery(queryString);
		assertNotNull("testGetOrderByWithoutWhereClause() : Parsing the query returns null object", queryParameter);
		List<String> orderByFields = new ArrayList<String>();
		orderByFields.add("city");

		List<String> fields = new ArrayList<String>();
		fields.add("city");
		fields.add("winner");
		fields.add("player_of_match");

		assertEquals(
				"testGetOrderByWithoutWhereClause() : File name extraction failed. Check getFile() method. File name can be found after a space after from clause. Note: CSV file can contain a field that contains from as a part of the column name. For eg: from_date,from_hrs etc",
				"data/ipl.csv", queryParameter.getFile());
		assertEquals(
				"testGetOrderByWithoutWhereClause() : Select fields extractions failed. The query string can have multiple fields separated by comma after the 'select' keyword. The extracted fields is supposed to be stored in a String array which is to be returned by the method getFields(). Check getFields() method",
				fields, queryParameter.getFields());
		assertEquals(
				"testGetOrderByWithoutWhereClause() : Hint: Please note that we will need to extract the field(s) after 'order by' clause in the query, if at all the order by clause exists",
				orderByFields, queryParameter.getOrderByFields());

		display(queryString, queryParameter);

	}

	private void display(String queryString, QueryParameter queryParameter) {
		System.out.println("\nQuery : " + queryString);
		System.out.println("--------------------------------------------------");
		System.out.println("Base Query:" + queryParameter.getBaseQuery());
		System.out.println("File:" + queryParameter.getFile());
		System.out.println("Query Type:" + queryParameter.getQUERY_TYPE());
		List<String> fields = queryParameter.getFields();
		System.out.println("Selected field(s):");
		if (fields == null || fields.isEmpty()) {
			System.out.println("*");
		} else {
			for (String field : fields) {
				System.out.println("\t" + field);
			}
		}

		List<Restriction> restrictions = queryParameter.getRestrictions();

		if (restrictions != null && !restrictions.isEmpty()) {
			System.out.println("Where Conditions : ");
			int conditionCount = 1;
			for (Restriction restriction : restrictions) {
				System.out.println("\tCondition : " + conditionCount++);
				System.out.println("\t\tName : " + restriction.getPropertyName());
				System.out.println("\t\tCondition : " + restriction.getCondition());
				System.out.println("\t\tValue : " + restriction.getPropertyValue());
			}
		}
		List<AggregateFunction> aggregateFunctions = queryParameter.getAggregateFunctions();
		if (aggregateFunctions != null && !aggregateFunctions.isEmpty()) {

			System.out.println("Aggregate Functions : ");
			int funtionCount = 1;
			for (AggregateFunction aggregateFunction : aggregateFunctions) {
				System.out.println("\t Aggregate Function : " + funtionCount++);
				System.out.println("\t\t function : " + aggregateFunction.getFunction());
				System.out.println("\t\t  field : " + aggregateFunction.getField());
			}

		}

		List<String> orderByFields = queryParameter.getOrderByFields();
		if (orderByFields != null && !orderByFields.isEmpty()) {

			System.out.println(" Order by fields : ");
			for (String orderByField : orderByFields) {
				System.out.println("\t " + orderByField);

			}

		}

		List<String> groupByFields = queryParameter.getGroupByFields();
		if (groupByFields != null && !groupByFields.isEmpty()) {

			System.out.println(" Group by fields : ");
			for (String groupByField : groupByFields) {
				System.out.println("\t " + groupByField);

			}

		}

	}

	/*
	 * The following test cases are used to check whether the query processing are
	 * working properly
	 */

	@Test
	public void testSelectAllWithoutWhere() {

		int expectedrows = 577;

		DataSet dataSet = query.executeQuery("select * from data/ipl.csv");
		assertNotNull("testSelectAllWithoutWhere() : Empty Dataset is returned", dataSet);
		display("testSelectAllWithoutWhere", dataSet);

		assertNull("testSelectAllWithoutWhere() : getAggregateFunctions method should return null",
				dataSet.getAggregateFunctions());
		assertNull("testSelectAllWithoutWhere() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testSelectAllWithoutWhere() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals("testSelectAllWithoutWhere() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		System.out.println("dddd" + dataSet.getResult().get(0));
		assertTrue("testSelectAllWithoutWhere() : Get Result does not return expected values ",
				dataSet.getResult().get(0).toString().contains(
						"1, 2008, Bangalore, 2008-04-18, Kolkata Knight Riders, Royal Challengers Bangalore, Royal Challengers Bangalore, field, normal, 0, Kolkata Knight Riders, 140, 0, BB McCullum, M Chinnaswamy Stadium, Asad Rauf, RE Koertzen"));
		assertTrue("testSelectAllWithoutWhere() : Get Result does not return expected values ",
				dataSet.getResult().get(288).toString().contains(
						"289, 2012, Cuttack, 2012-05-01, Deccan Chargers, Pune Warriors, Deccan Chargers, bat, normal, 0, Deccan Chargers, 13, 0, KC Sangakkara, Barabati Stadium, Aleem Dar, AK Chaudhary"));
		assertTrue("testSelectAllWithoutWhere() : Get Result does not return expected values ",
				dataSet.getResult().get(576).toString().contains(
						"577, 2016, Bangalore, 2016-05-29, Sunrisers Hyderabad, Royal Challengers Bangalore, Sunrisers Hyderabad, bat, normal, 0, Sunrisers Hyderabad, 8, 0, BCJ Cutting, M Chinnaswamy Stadium, HDPK Dharmasena, BNJ Oxenford"));

	}

	@Test
	public void testSelectColumnsWithoutWhere() {
		int expectedrows = 577;
		DataSet dataSet = query.executeQuery("select city,winner,team1,team2 from data/ipl.csv");
		assertNotNull("testSelectColumnsWithoutWhere() : Empty Dataset is returned", dataSet);
		display("testSelectColumnsWithoutWhere", dataSet);

		assertNull(
				"testSelectColumnsWithoutWhere() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testSelectColumnsWithoutWhere() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testSelectColumnsWithoutWhere() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals(
				"testSelectColumnsWithoutWhere() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertTrue("testSelectColumnsWithoutWhere() : Get Result does not return expected values ",
				dataSet.getResult().get(0).toString().contains(
						"Bangalore, Kolkata Knight Riders, Kolkata Knight Riders, Royal Challengers Bangalore"));
		assertTrue("testSelectColumnsWithoutWhere() : Get Result does not return expected values ", dataSet.getResult()
				.get(288).toString().contains("Cuttack, Deccan Chargers, Deccan Chargers, Pune Warriors"));
		assertTrue("testSelectColumnsWithoutWhere() : Get Result does not return expected values ",
				dataSet.getResult().get(576).toString()
						.contains("Bangalore, Sunrisers Hyderabad, Sunrisers Hyderabad, Royal Challengers Bangalore"));

	}

	@Test
	public void testWithWhereGreaterThan() {
		int expectedrows = 60;

		DataSet dataSet = query.executeQuery(
				"select season,city,winner,team1,team2,player_of_match from data/ipl.csv where season > 2015");
		assertNotNull("testWithWhereGreaterThan() : Empty Dataset is returned", dataSet);
		display("testWithWhereGreaterThan", dataSet);

		assertNull(
				"testWithWhereGreaterThan() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testWithWhereGreaterThan() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereGreaterThan() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals("testWithWhereGreaterThan() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertTrue("testWithWhereGreaterThan() : Get Result does not return expected values ",
				dataSet.getResult().get(0).toString().contains(
						"2016, Mumbai, Rising Pune Supergiants, Mumbai Indians, Rising Pune Supergiants, AM Rahane"));
		assertTrue("testWithWhereGreaterThan() : Get Result does not return expected values",
				dataSet.getResult().get(29).toString().contains(
						"2016, Bangalore, Kolkata Knight Riders, Royal Challengers Bangalore, Kolkata Knight Riders, AD Russell"));
		assertTrue("testWithWhereGreaterThan() : Get Result does not return expected values ",
				dataSet.getResult().get(59).toString().contains(
						"2016, Bangalore, Sunrisers Hyderabad, Sunrisers Hyderabad, Royal Challengers Bangalore, BCJ Cutting"));

	}

	@Test
	public void testWithWhereLessThan() {
		int expectedrows = 458;

		DataSet dataSet = query
				.executeQuery("select city,winner,team1,team2,player_of_match from data/ipl.csv where season < 2015");
		assertNotNull("testWithWhereLessThan() : Empty Dataset is returned", dataSet);
		display("testWithWhereLessThan", dataSet);

		assertNull(
				"testWithWhereLessThan() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testWithWhereLessThan() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereLessThan() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals("testWithWhereLessThan() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertTrue("testWithWhereLessThan() : Get Result does not return expected values",
				dataSet.getResult().get(0).toString().contains(
						"Bangalore, Kolkata Knight Riders, Kolkata Knight Riders, Royal Challengers Bangalore, BB McCullum"));
		assertTrue("testWithWhereLessThan() : Get Result does not return expected values",
				dataSet.getResult().get(228).toString().contains(
						"Jaipur, Royal Challengers Bangalore, Rajasthan Royals, Royal Challengers Bangalore, S Aravind"));
		assertTrue("testWithWhereLessThan() : Get Result does not return expected values",
				dataSet.getResult().get(457).toString().contains(
						"Bangalore, Kolkata Knight Riders, Kings XI Punjab, Kolkata Knight Riders, MK Pandey"));

	}

	@Test
	public void testWithWhereLessThanOrEqualTo() {
		int expectedrows = 517;
		DataSet dataSet = query.executeQuery(
				"select season,city,winner,team1,team2,player_of_match from data/ipl.csv where season <= 2015");
		assertNotNull("testWithWhereLessThanOrEqualTo() : Empty Dataset is returned", dataSet);
		display("testWithWhereLessThanOrEqualTo", dataSet);

		assertNull(
				"testWithWhereLessThanOrEqualTo() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testWithWhereLessThanOrEqualTo() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereLessThanOrEqualTo() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals(
				"testWithWhereLessThanOrEqualTo() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereLessThanOrEqualTo() : Get Result does not return expected values ",
				"[2008, Bangalore, Kolkata Knight Riders, Kolkata Knight Riders, Royal Challengers Bangalore, BB McCullum]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereLessThanOrEqualTo() :  Get Result does not return expected values",
				"[2012, Delhi, Delhi Daredevils, Chennai Super Kings, Delhi Daredevils, M Morkel]",
				dataSet.getResult().get(258).toString());
		assertEquals("testWithWhereLessThanOrEqualTo() :  Get Result does not return expected values",
				"[2015, Kolkata, Mumbai Indians, Mumbai Indians, Chennai Super Kings, RG Sharma]",
				dataSet.getResult().get(516).toString());

	}

	@Test
	public void testWithWhereGreaterThanOrEqualTo() {

		int expectedrows = 119;

		DataSet dataSet = query
				.executeQuery("select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2015");
		assertNotNull("testWithWhereGreaterThanOrEqualTo() : Empty Dataset is returned", dataSet);
		display("testWithWhereGreaterThanOrEqualTo", dataSet);

		assertNull(
				"testWithWhereGreaterThanOrEqualTo() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testWithWhereGreaterThanOrEqualTo() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereGreaterThanOrEqualTo() : Group By Result should return null",
				dataSet.getGroupByResult());
		assertEquals(
				"testWithWhereGreaterThanOrEqualTo() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereGreaterThanOrEqualTo() : Get Result does not return expected values",
				"[Kolkata, Kolkata Knight Riders, Mumbai Indians, Kolkata Knight Riders, M Morkel]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereGreaterThanOrEqualTo() : Get Result does not return expected values",
				"[Mumbai, Rising Pune Supergiants, Mumbai Indians, Rising Pune Supergiants, AM Rahane]",
				dataSet.getResult().get(59).toString());
		assertEquals("testWithWhereGreaterThanOrEqualTo() : Get Result does not return expected values",
				"[Bangalore, Sunrisers Hyderabad, Sunrisers Hyderabad, Royal Challengers Bangalore, BCJ Cutting]",
				dataSet.getResult().get(118).toString());

	}

	@Test
	public void testWithWhereNotEqualTo() {
		int expectedrows = 315;

		DataSet dataSet = query.executeQuery(
				"select city,team1,team2,winner,toss_decision from data/ipl.csv where toss_decision != bat");
		assertNotNull("testWithWhereNotEqualTo() : Empty Dataset is returned", dataSet);
		display("testWithWhereNotEqualTo", dataSet);

		assertNull(
				"testWithWhereNotEqualTo() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testWithWhereNotEqualTo() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereNotEqualTo() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals("testWithWhereNotEqualTo() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereNotEqualTo() : Get Result does not return expected values",
				"[Bangalore, Kolkata Knight Riders, Royal Challengers Bangalore, Kolkata Knight Riders, field]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereNotEqualTo() : Get Result does not return expected values",
				"[Hyderabad, Deccan Chargers, Royal Challengers Bangalore, Deccan Chargers, field]",
				dataSet.getResult().get(157).toString());
		assertEquals("testWithWhereNotEqualTo() : Get Result does not return expected values",
				"[Delhi, Gujarat Lions, Sunrisers Hyderabad, Sunrisers Hyderabad, field]",
				dataSet.getResult().get(314).toString());

	}

	@Test
	public void testWithWhereEqualAndNotEqual() {
		int expectedrows = 195;

		DataSet dataSet = query.executeQuery(
				"select season,city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 and season <= 2015");
		assertNotNull("testWithWhereEqualAndNotEqual() : Empty Dataset is returned", dataSet);
		display("testWithWhereEqualAndNotEqual", dataSet);

		assertNull(
				"testWithWhereEqualAndNotEqual() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull("testWithWhereEqualAndNotEqual() : Group by Aggregate Result should return null",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereEqualAndNotEqual() : Group By Result should return null", dataSet.getGroupByResult());
		assertEquals(
				"testWithWhereEqualAndNotEqual() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereEqualAndNotEqual() : Get Result does not return expected values",
				"[2013, Kolkata, Kolkata Knight Riders, Delhi Daredevils, Kolkata Knight Riders, SP Narine]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereEqualAndNotEqual() : Get Result does not return expected values ",
				"[2014, Mumbai, Mumbai Indians, Kings XI Punjab, Mumbai Indians, CJ Anderson]",
				dataSet.getResult().get(97).toString());
		assertEquals("testWithWhereEqualAndNotEqual() : Get Result does not return expected values",
				"[2015, Kolkata, Mumbai Indians, Mumbai Indians, Chennai Super Kings, RG Sharma]",
				dataSet.getResult().get(194).toString());

	}

	@Test
	public void testWithWhereTwoConditionsEqualOrNotEqual() {
		int expectedrows = 155;
		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 and toss_decision != bat");
		assertNotNull("testWithWhereTwoConditionsEqualOrNotEqual() : Empty Dataset is returned", dataSet);
		display("testWithWhereTwoConditionsEqualOrNotEqual", dataSet);

		assertNull(
				"testWithWhereTwoConditionsEqualOrNotEqual() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull(
				"testWithWhereTwoConditionsEqualOrNotEqual() : Group by Aggregate Result is returning some value while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereTwoConditionsEqualOrNotEqual() : Group By Result should return null",
				dataSet.getGroupByResult());
		assertEquals(
				"testWithWhereTwoConditionsEqualOrNotEqual() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereTwoConditionsEqualOrNotEqual() :  Get Result does not return expected values",
				"[Kolkata, Kolkata Knight Riders, Delhi Daredevils, Kolkata Knight Riders, SP Narine]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereTwoConditionsEqualOrNotEqual() :  Get Result does not return expected values",
				"[Mumbai, Kings XI Punjab, Kings XI Punjab, Mumbai Indians, GJ Bailey]",
				dataSet.getResult().get(77).toString());
		assertEquals("testWithWhereTwoConditionsEqualOrNotEqual() : Get Result does not return expected values ",
				"[Delhi, Sunrisers Hyderabad, Gujarat Lions, Sunrisers Hyderabad, DA Warner]",
				dataSet.getResult().get(154).toString());

	}

	@Test
	public void testWithWhereThreeConditionsEqualOrNotEqual() {
		int expectedrows = 51;
		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2015 or toss_decision != bat and city = bangalore");
		assertNotNull("testWithWhereThreeConditionsEqualOrNotEqual() : Empty Dataset is returned", dataSet);
		display("testWithWhereThreeConditionsEqualOrNotEqual", dataSet);

		assertNull(
				"testWithWhereThreeConditionsEqualOrNotEqual() : getAggregateFunctions method is returning some value while it's not expected to",
				dataSet.getAggregateFunctions());
		assertNull(
				"testWithWhereThreeConditionsEqualOrNotEqual() : getGroupByAggregateResult method is returning some value while it's not expected to",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereThreeConditionsEqualOrNotEqual() : Group By Result should return null",
				dataSet.getGroupByResult());
		assertEquals(
				"testWithWhereThreeConditionsEqualOrNotEqual() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereThreeConditionsEqualOrNotEqual() : Get Result does not return expected values",
				"[Bangalore, Kolkata Knight Riders, Kolkata Knight Riders, Royal Challengers Bangalore, BB McCullum]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereThreeConditionsEqualOrNotEqual() :  Get Result does not return expected values",
				"[Bangalore, Royal Challengers Bangalore, Rajasthan Royals, Royal Challengers Bangalore, R Vinay Kumar]",
				dataSet.getResult().get(26).toString());
		assertEquals("testWithWhereThreeConditionsEqualOrNotEqual() :  Get Result does not return expected values",
				"[Bangalore, Sunrisers Hyderabad, Sunrisers Hyderabad, Royal Challengers Bangalore, BCJ Cutting]",
				dataSet.getResult().get(50).toString());

	}

	@Test
	public void testWithWhereThreeConditionsOrderBy() {
		int expectedrows = 53;
		DataSet dataSet = query.executeQuery(
				"select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2013 or toss_decision != bat and city = Bangalore order by winner");
		assertNotNull("testWithWhereThreeConditionsOrderBy() : Empty Dataset is returned", dataSet);
		display("testWithWhereThreeConditionsOrderBy", dataSet);

		assertNull("testWithWhereThreeConditionsOrderBy() : Aggregate Functions should return null",
				dataSet.getAggregateFunctions());
		assertNull(
				"testWithWhereThreeConditionsOrderBy() : Group by Aggregate Result is returning some value while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithWhereThreeConditionsOrderBy() : Group By Result should return null",
				dataSet.getGroupByResult());
		assertEquals(
				"testWithWhereThreeConditionsOrderBy() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithWhereThreeConditionsOrderBy() : Get Result does not return expected values",
				"[Bangalore, , Royal Challengers Bangalore, Rajasthan Royals, ]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithWhereThreeConditionsOrderBy() : Get Result does not return expected values",
				"[Bangalore, Royal Challengers Bangalore, Rajasthan Royals, Royal Challengers Bangalore, JH Kallis]",
				dataSet.getResult().get(26).toString());
		assertEquals("testWithWhereThreeConditionsOrderBy() : Get Result does not return expected values",
				"[Bangalore, Sunrisers Hyderabad, Sunrisers Hyderabad, Royal Challengers Bangalore, BCJ Cutting]",
				dataSet.getResult().get(52).toString());

	}

	@Test
	public void testWithWhereThreeConditionsGroupByOrderBy() {

		queryString = "select city,winner,team1,team2,player_of_match from data/ipl.csv "
				+ "      where season >= 2013 or toss_decision != bat and city = Bangalore "
				+ "              group by team1 order by city";
		DataSet dataSet = query.executeQuery(queryString);
		assertNotNull("testWithWhereThreeConditionsGroupByOrderBy() : Empty Dataset is returned", dataSet);
		displayGroupByResult(queryString, dataSet.getGroupByResult());

		assertNotNull("testWithWhereThreeConditionsGroupByOrderBy() : Group by Resultset does not return any value",
				dataSet.getGroupByResult());
		assertNull(
				"testWithWhereThreeConditionsGroupByOrderBy() : Group by Aggregate Result is returning values while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertNull(
				"testWithWhereThreeConditionsGroupByOrderBy() : Get Aggregate Function returns unexpected value, it should return null",
				dataSet.getAggregateFunctions());
		assertTrue("testWithWhereThreeConditionsGroupByOrderBy() : Get Result does not return expected values",
				dataSet.getGroupByResult().get("Bangalore").toString().contains(
						"1, 2008, Bangalore, 2008-04-18, Kolkata Knight Riders, Royal Challengers Bangalore, Royal Challengers Bangalore"));
	}

	@Test
	public void testWithOrderBy() {
		int expectedrows = 577;
		DataSet dataSet = query
				.executeQuery("select city,winner,team1,team2,player_of_match from data/ipl.csv order by city");
		assertNotNull("testWithOrderBy() : Empty Dataset is returned", dataSet);
		display("testWithOrderBy", dataSet);

		assertNull("testWithOrderBy() : Group by Resultset should not return any value", dataSet.getGroupByResult());
		assertNull("testWithOrderBy() : Aggregate Functions should return null", dataSet.getAggregateFunctions());
		assertNull("testWithOrderBy() : Group by Aggregate Result is returning values while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertNull("testWithOrderBy() : Group By Result method should not return values", dataSet.getGroupByResult());
		assertEquals("testWithOrderBy() : No. of rows returned does not match with the expected resultset",
				expectedrows, dataSet.getResult().size());
		assertEquals("testWithOrderBy() : Get Result does not return expected values",
				"[, Royal Challengers Bangalore, Mumbai Indians, Royal Challengers Bangalore, PA Patel]",
				dataSet.getResult().get(0).toString());
		assertEquals("testWithOrderBy() : Get Result does not return expected values",
				"[Hyderabad, Chennai Super Kings, Deccan Chargers, Chennai Super Kings, SK Raina]",
				dataSet.getResult().get(288).toString());
		assertEquals("testWithOrderBy() : Get Result does not return expected values",
				"[Visakhapatnam, Rising Pune Supergiants, Kings XI Punjab, Rising Pune Supergiants, MS Dhoni]",
				dataSet.getResult().get(576).toString());

	}

	@Test
	public void testSelectCountColumnsWithoutWhere() {
		int expectedrows = 1;
		DataSet dataSet = query.executeQuery("select count(city) from data/ipl.csv");
		assertNotNull("testSelectCountColumnsWithoutWhere() : Empty Dataset is returned", dataSet);
		display("testSelectCountColumnsWithoutWhere", dataSet);

		assertNull("testSelectCountColumnsWithoutWhere2() : Group by Resultset should not return any value",
				dataSet.getGroupByResult());
		assertNull(
				"testSelectCountColumnsWithoutWhere() : Group by Aggregate Result is returning some value while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertEquals(
				"testSelectCountColumnsWithoutWhere() : Aggregate Function does not return the expected no. of records",
				expectedrows, dataSet.getAggregateFunctions().size());
		assertEquals(
				"testSelectCountColumnsWithoutWhere() : Aggregate Function is not returning the correct column name",
				"city", dataSet.getAggregateFunctions().get(0).getField());
		assertEquals(
				"testSelectCountColumnsWithoutWhere() : Aggregate Function is not returning the correct aggregators",
				"count", dataSet.getAggregateFunctions().get(0).getFunction());
		System.out.println("sdsds" + dataSet.getResult());
		assertTrue("testSelectCountColumnsWithoutWhere() : Get Result method does not return the expected row count",
				dataSet.getResult().toString().contains("570"));

	}

	@Test
	public void testSelectSumColumnsWithoutWhere() {
		int expectedrows = 1;
		DataSet dataSet = query.executeQuery("select sum(win_by_runs) from data/ipl.csv");
		assertNotNull("testSelectSumColumnsWithoutWhere() : Empty Dataset is returned", dataSet);
		display("testSelectSumColumnsWithoutWhere", dataSet);

		assertNull("testSelectCountColumnsWithoutWhere2() : Group by Resultset should not return any value",
				dataSet.getGroupByResult());
		assertNull(
				"testSelectSumColumnsWithoutWhere() :  Group by Aggregate Result is returning values while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertEquals(
				"testSelectSumColumnsWithoutWhere() : Aggregate Function does not return the expected no. of records",
				expectedrows, dataSet.getAggregateFunctions().size());
		assertEquals("testSelectSumColumnsWithoutWhere() : Aggregate Function is not returning the correct column name",
				"win_by_runs", dataSet.getAggregateFunctions().get(0).getField());
		assertEquals("testSelectSumColumnsWithoutWhere() : Aggregate Function is not returning the correct aggregators",
				"sum", dataSet.getAggregateFunctions().get(0).getFunction());

		assertTrue("testSelectSumColumnsWithoutWhere() : Get Result method does not return the expected row count",
				dataSet.getResult().toString().contains("7914"));

	}

	@Test
	public void testSelectCountColumnsWithoutWhere2() {
		int expectedrows = 5;
		DataSet dataSet = query.executeQuery(
				"select count(city),sum(win_by_runs),min(win_by_runs),max(win_by_runs),avg(win_by_runs) from data/ipl.csv");
		assertNotNull("testSelectCountColumnsWithoutWhere2() : Empty Dataset is returned", dataSet);
		display("testSelectCountColumnsWithoutWhere2", dataSet);

		assertNull(
				"testSelectCountColumnsWithoutWhere2() : Group by Aggregate Result is returning some value while it's not expected",
				dataSet.getGroupByAggregateResult());
		assertNull("testSelectCountColumnsWithoutWhere2() : Group by Resultset should not return any value",
				dataSet.getGroupByResult());
		assertEquals(
				"testSelectCountColumnsWithoutWhere2() : Aggregate Function does not return the expected no. of records",
				expectedrows, dataSet.getAggregateFunctions().size());
		assertEquals(
				"testSelectCountColumnsWithoutWhere2() : Aggregate Function is not returning the correct column name",
				"city", dataSet.getAggregateFunctions().get(0).getField());
		assertEquals(
				"testSelectCountColumnsWithoutWhere2() : Aggregate Function is not returning the correct aggregators",
				"count", dataSet.getAggregateFunctions().get(0).getFunction());
		assertTrue("testSelectCountColumnsWithoutWhere2() : Get Result method does not return the expected row count",
				dataSet.getResult().toString().contains("570"));

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByCount() {
		int expectedrows = 31;
		queryString = "select city,count(*) from data/ipl.csv group by city";
		DataSet dataSet = query.executeQuery(queryString);
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByCount() : Empty Dataset is returned", dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

		assertNotNull("testSelectColumnsWithoutWhereWithGroupByCount() : Group by Resultset is null",
				dataSet.getGroupByAggregateResult());

		assertEquals(
				"testSelectColumnsWithoutWhereWithGroupByCount() : Group by Aggregate result does not match the expected no. of records",
				expectedrows, dataSet.getGroupByAggregateResult().size());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByCount() : Aggregate Function is returning some value while it's not expected",
				dataSet.getAggregateFunctions());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByCount() : getResult method is returning values while it's not expected to",
				dataSet.getResult());
		assertNotNull(
				"testSelectColumnsWithoutWhereWithGroupByCount() : getGroupByAggregateResult method is not returning values while it's expected to",
				dataSet.getGroupByAggregateResult().toString());
		System.out.println(dataSet.getGroupByAggregateResult());
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByCount() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString()
						.contains("count=7, sum=2870.000000, min=403.000000, average=410.000000, max=418.000000"));

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupBySum() {
		int expectedrows = 31;
		queryString = "select city,sum(season) from data/ipl.csv group by city";
		DataSet dataSet = query.executeQuery(queryString);
		assertNotNull("testSelectColumnsWithoutWhereWithGroupBySum() : Empty Dataset is returned", dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

		assertNotNull("testSelectColumnsWithoutWhereWithGroupBySum() : Group by Resultset is null",
				dataSet.getGroupByAggregateResult());

		assertEquals(
				"testSelectColumnsWithoutWhereWithGroupBySum() : Group by Aggregate result is not matching with the expected result",
				expectedrows, dataSet.getGroupByAggregateResult().size());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupBySum() : Aggregate Function is returning values while it's not expected",
				dataSet.getAggregateFunctions());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupBySum() : getResult method is returning some value while it's not expected to",
				dataSet.getResult());
		assertNotNull(
				"testSelectColumnsWithoutWhereWithGroupBySum() : getGroupByAggregateResult method is not returning values while it's expected to",
				dataSet.getGroupByAggregateResult().toString());
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupBySum() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString()
						.contains("count=7, sum=14098.000000, min=2014.000000, average=2014.000000, max=2014.000000"));
	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByMin() {
		int expectedrows = 31;
		queryString = "select city,min(season) from data/ipl.csv group by city";
		DataSet dataSet = query.executeQuery(queryString);
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByMin() : Empty Dataset is returned", dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

		assertNotNull("testSelectColumnsWithoutWhereWithGroupByMin() : Group by Resultset is null",
				dataSet.getGroupByAggregateResult());

		assertEquals(
				"testSelectColumnsWithoutWhereWithGroupByMin() : Group by Aggregate result is not matching with the expected result",
				expectedrows, dataSet.getGroupByAggregateResult().size());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByMin() : Aggregate Function is returning some value while it's not expected",
				dataSet.getAggregateFunctions());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByMin() : getResult method is returning some value while it's not expected to",
				dataSet.getResult());
		assertNotNull(
				"testSelectColumnsWithoutWhereWithGroupByMin() : getGroupByAggregateResult method is not returning values while it's expected to",
				dataSet.getGroupByAggregateResult().toString());
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMin() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString()
						.contains("count=7, sum=14098.000000, min=2014.000000, average=2014.000000, max=2014.000000"));

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByMax() {
		int expectedrows = 31;
		queryString = "select city,max(win_by_wickets) from data/ipl.csv group by city";
		DataSet dataSet = query.executeQuery(queryString);
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByMax() : Empty Dataset is returned", dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

		assertNotNull("testSelectColumnsWithoutWhereWithGroupByMax() : Group by Resultset is null",
				dataSet.getGroupByAggregateResult());

		assertEquals(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Group by Aggregate result is not matching with the expected result",
				expectedrows, dataSet.getGroupByAggregateResult().size());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Aggregate Function is returning values while it's not expected",
				dataSet.getAggregateFunctions());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByMax() : getResult method is returning some value while it's not expected to",
				dataSet.getResult());
		assertNotNull(
				"testSelectColumnsWithoutWhereWithGroupByMax() : getGroupByAggregateResult method is not returning values while it's expected to",
				dataSet.getGroupByAggregateResult().toString());
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString().contains(
						"Sharjah=DoubleSummaryStatistics{count=6, sum=26.000000, min=0.000000, average=4.333333, max=8.000000}"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByMax() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString().contains(
						"Ranchi=DoubleSummaryStatistics{count=7, sum=24.000000, min=0.000000, average=3.428571, max=6.000000}"));

	}

	@Test
	public void testSelectColumnsWithoutWhereWithGroupByAvg() {

		int expectedrows = 31;
		queryString = "select city,avg(win_by_wickets) from data/ipl.csv group by city";
		DataSet dataSet = query.executeQuery("select city,avg(win_by_wickets) from data/ipl.csv group by city");
		assertNotNull("testSelectColumnsWithoutWhereWithGroupByAvg() : Empty Dataset is returned", dataSet);
		displayGroupByAggregateResult(queryString, dataSet.getGroupByAggregateResult());

		assertNotNull("testSelectColumnsWithoutWhereWithGroupByAvg() : Group by Resultset is null",
				dataSet.getGroupByAggregateResult());

		assertEquals(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Group by Aggregate result is not matching with the expected result",
				expectedrows, dataSet.getGroupByAggregateResult().size());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Aggregate Function is returning values while it's not expected",
				dataSet.getAggregateFunctions());
		assertNull(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : getResult method is returning some value while it's not expected to",
				dataSet.getResult());
		assertNotNull(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Group By Aggregate Result is not returning values",
				dataSet.getGroupByAggregateResult().toString());
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString().contains(
						"Sharjah=DoubleSummaryStatistics{count=6, sum=26.000000, min=0.000000, average=4.333333, max=8.000000}"));
		assertTrue(
				"testSelectColumnsWithoutWhereWithGroupByAvg() : Group by Aggregate result is not returning the expected data",
				dataSet.getGroupByAggregateResult().toString().contains(
						"Ranchi=DoubleSummaryStatistics{count=7, sum=24.000000, min=0.000000, average=3.428571, max=6.000000}"));

	}

	private void display(String testCaseName, DataSet records) {
		System.out.println("\nTest Case Name : " + testCaseName);
		records.getResult().forEach(System.out::println);

	}

	private void displayGroupByAggregateResult(String queryString, Map<String, DoubleSummaryStatistics> groupByResult) {

		System.out.println("\nGiven Query : " + queryString);
		groupByResult.entrySet().forEach(System.out::println);
	}

	private void displayGroupByResult(String queryString, Map<String, List<List<String>>> groupByResult) {

		System.out.println("\nGiven Query : " + queryString);
		if (groupByResult != null && groupByResult.entrySet() != null) {
			groupByResult.entrySet().forEach(System.out::println);
		} else {
			System.out.println("No Group by result");
		}

	}

	private void display(String testCaseName, HashMap dataSet) {

		System.out.println(testCaseName);
		System.out.println("================================================================");
		System.out.println(dataSet);

	}

}