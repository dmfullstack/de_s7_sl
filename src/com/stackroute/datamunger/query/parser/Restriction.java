package com.stackroute.datamunger.query.parser;

public class Restriction {
	
	private String propertyName;
	private String propertyValue;
	private String condition;
	public Restriction(String propertyName, String propertyValue, String condition) {
		this.propertyName = propertyName;
		this.propertyValue = propertyValue;
		this.condition = condition;
	}
	public String getPropertyName() {
		return propertyName;
	}
	public String getPropertyValue() {
		return propertyValue;
	}
	public String getCondition() {
		return condition;
	}

}
