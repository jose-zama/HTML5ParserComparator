package com.html5tools.commoncrawl.ccindex;

public class CCIndexPartRecord {
	
	public static final int URL = 0;
	public static final int INDEX_FILE = 1;
	public static final int OFFSET = 2;
	public static final int LENGTH = 3;
	
	private String record;
	private String url;
	private String indexFile;
	private long offset;
	private long length;
	
	public CCIndexPartRecord(String record) {
		this.record = record;
		String[] fields = record.split("\t");
		url = fields[URL];
		indexFile = fields[INDEX_FILE];
		offset = Long.parseLong(fields[OFFSET]);
		length = Long.parseLong(fields[LENGTH]);
	}

	public String getRecord() {
		return record;
	}

	public void setRecord(String record) {
		this.record = record;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getIndexFile() {
		return indexFile;
	}

	public void setIndexFile(String indexFile) {
		this.indexFile = indexFile;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}
	

}

