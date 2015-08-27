package com.html5tools.commoncrawl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.archive.format.gzip.GZIPMemberWriter;
import org.archive.util.zip.GZIPMembersInputStream;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.html5tools.commoncrawl.ccindex.CCIndexPartRecord;
import com.html5tools.commoncrawl.ccindex.CCIndexRecord;

public class CrawlIndex {

	String bucket;
	String remoteFolder;
	AmazonS3 s3Client;

	String indexPath;
	List<Path> indexList;

	public static int SHARD_SIZE = 3000;

	public CrawlIndex(String partsPath, String bucket, String remoteIndexFolder) {
		this.indexPath = partsPath;
		this.bucket = bucket;
		this.remoteFolder = remoteIndexFolder;
		s3Client = new AmazonS3Client(new ProfileCredentialsProvider(
				"profileConfigFile.properties", "default"));
	}

	private Path getRandomSubIndex() {
		int min = 0;
		int max = indexList.size();
		int random = min + (int) (Math.random() * (max - min));
		return indexList.get(random);
	}

	private void setIndexPartsPaths() {
		List<Path> paths = new ArrayList<Path>();
		Path iPath = Paths.get(indexPath);
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(iPath,
				"part-*")) {
			for (Path entry : stream) {
				paths.add(entry);
				// System.out.println(entry.getFileName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		indexList = paths;
	}

	public Set<CCIndexRecord> getSubIndexRandomRecords(Path partFile,
			int numRecords) throws IOException {

		// Get random shards and the number of records to get from each one
		Map<CCIndexPartRecord, Integer> shards = getClusterPartRandomRecordsRepeteable(
				partFile, numRecords);

		// Get the indexes from each block selected randomly
		Set<CCIndexRecord> records = getIndexRecords(shards);

		return records;
	}

	private Set<CCIndexRecord> getIndexRecords(
			Map<CCIndexPartRecord, Integer> shards) {
		Set<CCIndexRecord> records = new HashSet<CCIndexRecord>();
		for (CCIndexPartRecord shard : shards.keySet()) {
			String key = remoteFolder + shard.getIndexFile();
			Set<CCIndexRecord> recordsShard = getHtmlTypeRamdomRecordsFrom3SIndexShard(
					shards.get(shard), s3Client, bucket, key,
					shard.getOffset(), shard.getLength());
			// If it was not possible to retrieve records, don't add it
			if (records != null)
				records.addAll(recordsShard);
		}
		return records;
	}

	/**
	 * 
	 * It appears is not possible to close the stream until all data was read.
	 * 
	 * @param bucketName
	 * @param key
	 * @param offset
	 * @param length
	 * @return
	 */
	public static CCIndexRecord getHtmlTypeIndexRecordFrom3S(AmazonS3 s3Client,
			String bucketName, String key, long offset, long length) {
		CCIndexRecord record = null;

		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);
		long end = offset + ((length - 1) < 5000L ? length - 1 : 5000L);
		// rangeObjectRequest.setRange(offset, offset + length - 1);
		rangeObjectRequest.setRange(offset, end);

		try (S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new GZIPMembersInputStream(
								objectPortion.getObjectContent()),
								Charset.forName("UTF-8")))) {

			for (int attempts = 0; attempts < 10; attempts++) {
				String line = reader.readLine();
				if (line == null)
					break;
				record = new CCIndexRecord(line);
				if (record.getMime().equals("text/html")) {
					break;
				}
			}
			// System.out.println("Record obtained: " + record.getRecord());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return record;
	}

	/**
	 * 
	 * It appears is not possible to close the stream until all data was read.
	 * 
	 * @param bucketName
	 * @param key
	 * @param offset
	 * @param length
	 * @return
	 */
	public static Set<CCIndexRecord> getHtmlTypeRamdomRecordsFrom3SIndexShard(
			int numberRecords, AmazonS3 s3Client, String bucketName,
			String key, long offset, long length) {
		return getHtmlTypeRamdomRecordsFrom3SIndexShard(numberRecords,
				s3Client, bucketName, key, offset, length, 0);
	}

	/**
	 * 
	 * It appears is not possible to close the stream until all data was read.
	 * 
	 * @param bucketName
	 * @param key
	 * @param offset
	 * @param length
	 * @return
	 */
	public static Set<CCIndexRecord> getHtmlTypeRamdomRecordsFrom3SIndexShard(
			int numberRecords, AmazonS3 s3Client, String bucketName,
			String key, long offset, long length, int blockSize) {

		if (blockSize <= 0)
			blockSize = SHARD_SIZE;
		Integer[] randomNumbers = sampleInteger(numberRecords, blockSize);
		List<Integer> randomLines = Arrays.asList(randomNumbers);
		int maxValue = Collections.max(randomLines);

		Set<CCIndexRecord> records = new HashSet<CCIndexRecord>();
		CCIndexRecord record = null;

		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);
		rangeObjectRequest.setRange(offset, offset + length - 1);

		int realBlockSize = 0;
		try (S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new GZIPMembersInputStream(
								objectPortion.getObjectContent()),
								Charset.forName("UTF-8")))) {

			readingRecords: for (int counter = 1; counter <= maxValue; counter++) {
				for (int attempts = 0; attempts < 10; attempts++) {
					String line = reader.readLine();
					if (line == null) {
						realBlockSize = counter;
						break readingRecords;
					}

					if (!randomLines.contains(counter))
						continue readingRecords;

					record = new CCIndexRecord(line);
					if (record.getMime().equals("text/html")) {
						// System.out.println("Record obtained: "
						// + record.getRecord());
						records.add(record);
						continue readingRecords;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// if end of block was reached before finishing sampling run again the
		// sample
		if (realBlockSize != 0) {
			// int missingRecords = numberRecords - records.size();
			if (realBlockSize < numberRecords)
				return getHtmlTypeRamdomRecordsFrom3SIndexShard(realBlockSize,
						s3Client, bucketName, key, offset, length,
						realBlockSize);
			else
				return getHtmlTypeRamdomRecordsFrom3SIndexShard(numberRecords,
						s3Client, bucketName, key, offset, length,
						realBlockSize);
		}
		return records;
	}

	public List<CCIndexPartRecord> getClusterPartRandomRecords(Path file,
			int quantity) throws IOException {

		// Get the offsets of all records of the file
		List<Long> offsets = getCLusterPartLinesOffset(file);
		int totalLines = offsets.size();
		int min = 1;
		int max = totalLines;

		// Select a random set
		List<Long> randomOffsets = new ArrayList<Long>();
		for (int i = 0; i < quantity; i++) {
			int random = min + (int) (Math.random() * (max - min));
			long offset = offsets.get(random);
			if (!randomOffsets.contains(offset))
				randomOffsets.add(offsets.get(random));
			else
				i--;
		}

		// Order the offsets, so the file is streamed once
		Collections.sort(randomOffsets);

		// Get the records
		return getClusterPartRecordsWithOffsets(file, randomOffsets);
	}

	public static Map<CCIndexPartRecord, Integer> getClusterPartRandomRecordsRepeteable(
			Path file, int quantity) throws IOException {

		// Get the offsets of all records of the file
		List<Long> offsets = getCLusterPartLinesOffset(file);
		int totalLines = offsets.size();
		int min = 1;
		int max = totalLines;

		// Select a random set
		Map<Long, Integer> randomOffsets = new HashMap<Long, Integer>();
		for (int i = 0; i < quantity; i++) {
			int random = min + (int) (Math.random() * (max - min));
			long offset = offsets.get(random);
			if (!randomOffsets.containsKey(offset))
				randomOffsets.put(offset, 1);
			else
				randomOffsets.put(offset, randomOffsets.get(offset) + 1);
		}

		// Order the offsets, so the file is streamed once
		List<Long> orderedOffsets = new ArrayList<Long>(randomOffsets.keySet());
		Collections.sort(orderedOffsets);

		// Get the records form the subindex file
		List<CCIndexPartRecord> indexRecords = getClusterPartRecordsWithOffsets(
				file, orderedOffsets);

		Map<CCIndexPartRecord, Integer> records = new HashMap<CCIndexPartRecord, Integer>();

		// Join with the number of times selected randomly
		for (int i = 0; i < indexRecords.size(); i++) {
			records.put(indexRecords.get(i),
					randomOffsets.get(orderedOffsets.get(i)));
		}
		return records;
	}

	public static List<Long> getCLusterPartLinesOffset(Path file)
			throws IOException {
		// if (indexLineOffsets.containsKey(file)) {
		// return indexLineOffsets.get(file);
		// }

		long currentOffset = 0;
		List<Long> offsets = new ArrayList<Long>();
		try (InputStream in = Files.newInputStream(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(in, Charset.forName("UTF-8")))) {
			String line;
			while ((line = reader.readLine()) != null) {
				offsets.add(currentOffset);

				// Add 1 char for the end of line char
				currentOffset = currentOffset + line.length() + 1;
			}
			// System.out.println("Total lines: " + totalLines);
			// indexLineOffsets.put(file, offsets);
		}
		return offsets;
	}

	// Use offsets to reduce processing time and transfered data
	// though it may increase the memory used

	public String getRecordWithOffset(Path file, long offset)
			throws IOException {
		String record = null;
		try (InputStream in = Files.newInputStream(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new GZIPMembersInputStream(in),
								Charset.forName("UTF-8")))) {
			reader.skip(offset);
			record = reader.readLine();
			// System.out.println("Record obtained: " + record);
		}
		return record;
	}

	public static List<CCIndexPartRecord> getClusterPartRecordsWithOffsets(
			Path file, List<Long> orderedOffsets) throws IOException {
		List<CCIndexPartRecord> records = new ArrayList<CCIndexPartRecord>();
		String record = null;
		try (InputStream in = Files.newInputStream(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(in, Charset.forName("UTF-8")))) {
			long covered = 0;
			for (Long offset : orderedOffsets) {
				reader.skip(offset - covered);
				record = reader.readLine();
				// System.out.println("Part Record obtained: " + record);
				records.add(new CCIndexPartRecord(record));
				covered = offset + record.length() + 1;
			}
		}
		return records;
	}

	public static String getChunk3S(String bucketName, String key, long offset,
			long length, AmazonS3 s3Client) throws IOException {
		String record = null;

		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);
		rangeObjectRequest.setRange(offset, offset + length - 1);
		S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

		try (InputStream in = objectPortion.getObjectContent();
				GZIPMembersInputStream reader = new GZIPMembersInputStream(in)) {
			byte[] rawData = IOUtils.toByteArray(reader);
			record = new String(rawData, Charset.forName("UTF-8"));
			// System.out.println("Record obtained: " + record);
		}
		return record;
	}

	public void createSample(int size, String outputfile) throws IOException {
		setIndexPartsPaths();
		Path filePath = Paths.get(outputfile);

		Map<Path, Integer> recordsPerPart = new HashMap<Path, Integer>();
		for (int i = 0; i < size; i++) {
			Path subIndex = getRandomSubIndex();
			if (recordsPerPart.containsKey(subIndex)) {
				recordsPerPart.put(subIndex, recordsPerPart.get(subIndex) + 1);
			} else {
				recordsPerPart.put(subIndex, 1);
			}
		}

		try (BufferedWriter writer = Files.newBufferedWriter(filePath,
				Charset.forName("UTF-8"), StandardOpenOption.CREATE,
				StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
			long time = System.currentTimeMillis();
			try {
				for (Path subIndex : recordsPerPart.keySet()) {

					Set<CCIndexRecord> records = getSubIndexRandomRecords(
							subIndex, recordsPerPart.get(subIndex));

					for (CCIndexRecord record : records) {
						writer.write(record.getRecord(), 0, record.getRecord()
								.length());
						writer.newLine();
						writer.flush();
						size--;
					}
					System.out.println("Records remaining : " + size
							+ " Elapsed time: "
							+ (System.currentTimeMillis() - time));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static int countLines(Path file) throws IOException {

		int totalLines = 0;
		try (InputStream in = Files.newInputStream(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(in, Charset.forName("UTF-8")))) {
			while ((reader.readLine()) != null) {
				totalLines++;
			}
		}
		return totalLines;
	}

	// Similar to Fisher-Yates shuffle
	public static Integer[] sampleInteger(int sampleSize, int maxValue) {
		// Create the universe
		int[] array = new int[maxValue];
		for (int i = 0; i < maxValue; i++) {
			array[i] = i + 1;
		}

		// Random the sampleSize locations of the array
		Random r = new Random();
		for (int i = sampleSize - 1; i >= 0; i--) {
			int index = r.nextInt(array.length);// random position of all the
												// array
			// swap
			int tmp = array[index];// save the random position value
			array[index] = array[i];// swap the values
			array[i] = tmp;
		}

		// Cut the sample locations only
		Integer[] sample = new Integer[sampleSize];
		for (int i = 0; i < sampleSize; i++) {
			sample[i] = array[i];
		}

		return sample;
	}

	public static void obtainCommonCrawlRecords(Path cdxFile, Path outputFile,
			AmazonS3Client s3Client, String bucket) throws IOException {
		int totalLines = countLines(cdxFile);

		OutputStream out;
		try {
			out = Files.newOutputStream(outputFile);
			try {
				GZIPMemberWriter writer = new GZIPMemberWriter(out);
				try (BufferedReader reader = Files.newBufferedReader(cdxFile,
						Charset.forName("UTF-8"))) {

					String line = null;
					while ((line = reader.readLine()) != null) {

						CCIndexRecord index = new CCIndexRecord(line);
						long offset = index.getOffset();
						long length = index.getLength();
						String key = index.getKey();

						String record = getChunk3S(bucket, key, offset, length,
								s3Client);

						InputStream in = IOUtils.toInputStream(record, "UTF-8");

						writer.write(in);
						System.out
								.println("Records remaining: " + --totalLines);
					}
				}
			} finally {
				out.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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
	 * @param args
	 */
	public static void main(String[] args) {

		String bucketName = "aws-publicdatasets";
		//
		AmazonS3Client client = new AmazonS3Client(
				new ProfileCredentialsProvider("profileConfigFile.properties",
						"default"));

		// NewCrawlIndex
		// .getHtmlTypeRamdomRecordsFrom3SIndexShard(
		// 5,
		// client,
		// bucketName,
		// "common-crawl/cc-index/collections/CC-MAIN-2015-22/indexes/cdx-00000.gz",
		// 510390703L, 200181L);

		// String indexesPath =
		// "common-crawl/cc-index/collections/CC-MAIN-2015-22/indexes/";
		// NewCrawlIndex ci = new NewCrawlIndex("/home/jose/Downloads/indexes",
		// bucketName, indexesPath);

		if(args.length == 0 || !args[0].startsWith("-"))
			throw new IllegalArgumentException("Usage -(sample|build) ...");

		long ttime = System.currentTimeMillis();
		switch(args[0]){
		case "-sample": 
			CrawlIndex ci = new CrawlIndex(args[1], bucketName, args[2]);
			try {
				ci.createSample(Integer.parseInt(args[0]), args[3]);
				// ci.createSample(10, "/home/jose/Downloads/myindex.cdx");
			} catch (NumberFormatException | IOException e1) {
				e1.printStackTrace();
			}
			break;
		case "-build": 
			try {
				obtainCommonCrawlRecords(Paths.get(args[0]), Paths.get(args[1]),
						client, bucketName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		}
		System.out.println("Total time: "
				+ (System.currentTimeMillis() - ttime));
	}
}
