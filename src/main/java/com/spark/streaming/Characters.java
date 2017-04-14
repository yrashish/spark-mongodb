package com.spark.streaming;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Characters implements Serializable {
	String first;
	String last;
	int age;
	String state;

	public void setState(String state) {
		this.state = state;
	}
	public Characters() {
	}
	public String getFirst() {
		return first;
	}

	public String getLast() {
		return last;
	}

	public int getAge() {
		return age;
	}

	public String getState() {
		return state;
	}

	

}
