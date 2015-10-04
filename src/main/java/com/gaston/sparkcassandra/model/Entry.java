package com.gaston.sparkcassandra.model;

import java.io.Serializable;

public class Entry implements Serializable {
	private static final long serialVersionUID = 1L;
	private String date;
	private String info;
	private int source;

	public Entry(String date, String info, int source) {
		super();
		this.date = date;
		this.info = info;
		this.source = source;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public int getSource() {
		return source;
	}

	public void setSource(int source) {
		this.source = source;
	}

}
