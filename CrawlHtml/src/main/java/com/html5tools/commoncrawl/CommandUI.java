package com.html5tools.commoncrawl;

import java.io.IOException;
import java.nio.file.Paths;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

public class CommandUI {

	/**
	 * args[0] = Option
	 * 
	 * For creating a index sample (-sample):
	 * 
	 * args[1] = Sample size
	 * args[2] = Sub indexes files file system location  
	 * args[3] = S3 key of compressed shards/indexes
	 * args[4] = Filename of generated sample.
	 * 
	 * For building the WARC file (-build):
	 * 
	 * args[1] = CDX file
	 * args[2] = Filename of output WARC  
	 * 
	 * Parser runner (-parse):
	 * 
	 * args[1] = Parse name. 
	 * args[2] = Command to run the parser  
	 * args[3] = WARC filename  
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if(args.length == 0 || !args[0].startsWith("-"))
			throw new IllegalArgumentException("Usage -(sample|build|parse) ...");

		String bucketName;
		AmazonS3Client client;
		long ttime = System.currentTimeMillis();
		switch(args[0]){
		case "-sample": 
			bucketName = "aws-publicdatasets";
			client = new AmazonS3Client(
					new ProfileCredentialsProvider("profileConfigFile.properties",
							"default"));
			CrawlIndex ci = new CrawlIndex(args[2], bucketName, args[3]);
			try {
				ci.createSample(Integer.parseInt(args[1]), args[4]);
				// ci.createSample(10, "/home/jose/Downloads/myindex.cdx");
			} catch (NumberFormatException | IOException e1) {
				e1.printStackTrace();
			}
			break;
		case "-build": 
			bucketName = "aws-publicdatasets";
			client = new AmazonS3Client(
					new ProfileCredentialsProvider("profileConfigFile.properties",
							"default"));
			try {
				CrawlIndex.obtainCommonCrawlRecords(Paths.get(args[1]), Paths.get(args[2]),
						client, bucketName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		case "-parse": 
			try {
				String parseName = args[1];
				String cmd = args[2];
				String ccfile = args[3];
				Crawler crawler = new Crawler(parseName,
						 cmd);
				crawler.processLocalFile(ccfile);
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		}
		System.out.println("DONE!!!");
		System.out.println("Total time: "
				+ (System.currentTimeMillis() - ttime));
	}

}
