package com.html5tools.commoncrawl.ccindex;

import org.json.JSONObject;

public class CCIndexRecord {

	public static final int URL = 0;
	public static final int JSON_FORMAT = 2;

	private String record;
	private String key;
	private String mime;
	private long offset;
	private long length;

	public CCIndexRecord(String record) {
		this.record = record;
		String[] fields = record.split(" ", 3);
		JSONObject json = new JSONObject(fields[JSON_FORMAT]);
		offset = json.getLong("offset");
		length = json.getLong("length");
		key = json.getString("filename");
		setMime(json.getString("mime"));
	}

	// public static String getResponseType(String record){
	// String[] fields = record.split(" ",3);
	// JSONObject json = new JSONObject(fields[JSON_FORMAT]);
	// return json.getString("mime");
	// }

	public String getRecord() {
		return record;
	}

	public void setRecord(String record) {
		this.record = record;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getMime() {
		return mime;
	}

	public void setMime(String mime) {
		this.mime = mime;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

}
