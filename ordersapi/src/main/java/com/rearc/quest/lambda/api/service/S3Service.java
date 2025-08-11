package com.rearc.quest.lambda.api.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;
import com.rearc.quest.lambda.api.config.S3ClientConfig;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Service {
	private static final S3Client s3Client = S3ClientConfig.getS3Client();

	private static final String bucketName = System.getenv("BUCKET_NAME");
	private static final String blsGovFilesURI = "https://download.bls.gov/pub/time.series/pr/";
	private static final String blsGovFolder = System.getenv("FOLDER_NAME");
	
	// Checksum is stored in the metadata Map under this key
	private static final String s3FileMetadataKey = "sha256";
	private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

	public void uploadFile() {
		// Empty method
	}

	public static List<String> listS3BucketBlsGovObjects(S3Client s3Client) {
		logger.info("In listS3BucketBlsGovObjects");
		List<String> s3ObjectNames = new ArrayList<>();
		
		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
				.bucket(bucketName).prefix(blsGovFolder).build();

		ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(listObjectsV2Request);

		for (ListObjectsV2Response page : response) {
		    page.contents().forEach((S3Object object) -> {
		    	s3ObjectNames.add((object.key()));
		    });
		}
		return s3ObjectNames;
	}

	public void uploadBlsGovFilesToS3() throws IOException {
		List<String> fileUrls = extractBlsGovFileLinks();
		Map<String, Path> downloadedFiles = downloadFilesToTempDirectory(fileUrls);

		// Upload files to S3 (optional)
		for (Map.Entry<String, Path> entry : downloadedFiles.entrySet()) {
			String s3Key = blsGovFolder + entry.getValue().getFileName();
			Path filePath = entry.getValue();
			PutObjectRequest putRequest = PutObjectRequest.builder()
		            .bucket(bucketName)
		            .key(s3Key)
		            .build();

		    // This is the correct v2 call
		    s3Client.putObject(putRequest, filePath);
		    
		}
	}

	public static void uploadFilesToS3(Map<String, Path> downloadedFiles) throws IOException {
		logger.info("In uploadFilesToS3");
        logger.info("S3Client is " + s3Client.toString());
		List<String> downloadedObjects = new ArrayList<>();
		// Upload files to S3 (optional)
		for (Map.Entry<String, Path> entry : downloadedFiles.entrySet()) {
			logger.info("s3Key =  " + blsGovFolder + formatS3Key(entry.getValue().getFileName().toString()));
			String s3Key = blsGovFolder + formatS3Key(entry.getValue().getFileName().toString());
			downloadedObjects.add(s3Key);
			Path filePath = entry.getValue();
			logger.info("filePath =  " + filePath.toString());
			String checksum = calculateSha256(filePath);
			logger.info("Checksum = " + checksum);
			String s3Checksum = getChecksumMetadata(s3Client, bucketName, s3Key);
			logger.info("s3Checksum =  " + s3Checksum);
			if (s3Checksum == null || s3Checksum != null && !s3Checksum.equals(checksum)) {
				// uploadFileToS3 if the file exists in S3 and the checksum is different
				// or the file does not exist in S3
				logger.info("Checksum is different or null!  Uploading file to S3 " + s3Key);
				try {
					logger.info("In S3Client.putObject()");
					// Build the request with user metadata
				    PutObjectRequest putRequest = PutObjectRequest.builder()
				            .bucket(bucketName)
				            .key(s3Key)
				            .metadata(Map.of(s3FileMetadataKey.toLowerCase(), checksum))  // S3 user metadata keys must be lowercase
				            .build();
				    // Upload using RequestBody.fromFile for SDK v2
				    s3Client.putObject(putRequest, RequestBody.fromFile(filePath));
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			}
		}
		//If the bucket does not have the downloaded file from the bls-gov site, delete it
		logger.info("Calling listS3BucketBlsGovObjects");
		List<String> s3BucketObjects = listS3BucketBlsGovObjects(s3Client);
		logger.info("Calling findObjectsToDelete");
		List<String> objectsToDelete = findObjectsToDelete(s3Client, s3BucketObjects, downloadedObjects);
		logger.info("objectsToDelete =  " + objectsToDelete.toString());
		objectsToDelete.forEach(o -> deleteObjectFromS3(s3Client, bucketName, o));
	}
	
	public void uploadFilesToS3(Map<String, Path> downloadedFiles, S3Client s3Client) throws IOException {
		logger.info("In uploadFilesToS3");
        logger.info("S3Client is " + s3Client.toString());
		List<String> downloadedObjects = new ArrayList<>();
		// Upload files to S3 (optional)
		for (Map.Entry<String, Path> entry : downloadedFiles.entrySet()) {
			logger.info("s3Key =  " + blsGovFolder + formatS3Key(entry.getValue().getFileName().toString()));
			String s3Key = blsGovFolder + formatS3Key(entry.getValue().getFileName().toString());
			downloadedObjects.add(s3Key);
			Path filePath = entry.getValue();
			logger.info("filePath =  " + filePath.toString());
			String checksum = calculateSha256(filePath);
			logger.info("Checksum = " + checksum);
			String s3Checksum = getChecksumMetadata(s3Client, bucketName, s3Key);
			logger.info("s3Checksum =  " + s3Checksum);
			if (s3Checksum == null || s3Checksum != null && !s3Checksum.equals(checksum)) {
				// uploadFileToS3 if the file exists in S3 and the checksum is different
				// or the file does not exist in S3
				logger.info("Checksum is different or null!  Uploading file to S3 " + s3Key);
				try {
					logger.info("In S3Client.putObject()");
					s3Client.putObject(request -> request.bucket(bucketName).key(s3Key)
							.metadata(Map.of(s3FileMetadataKey, checksum)), filePath);
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			}
		}
		//If the bucket does not have the downloaded file from the bls-gov site, delete it
		logger.info("Calling listS3BucketBlsGovObjects");
		List<String> s3BucketObjects = listS3BucketBlsGovObjects(s3Client);
		logger.info("Calling findObjectsToDelete");
		List<String> objectsToDelete = findObjectsToDelete(s3Client, s3BucketObjects, downloadedObjects);
		logger.info("objectsToDelete =  " + objectsToDelete.toString());
		objectsToDelete.forEach(o -> deleteObjectFromS3(s3Client, bucketName, o));
	}
	
	private static List<String> findObjectsToDelete(S3Client s3Client, List<String> s3BucketObjects, List<String> downloadedObjects) {
		logger.info("In findObjectsToDelete");
		List<String> objectsToDelete = s3BucketObjects.stream()
                .filter(e -> !downloadedObjects.contains(e)) 
                .collect(Collectors.toList());
		return objectsToDelete;
	
	}

	public void downloadS3Object(Path filePath) {
		String s3Key = blsGovFolder + formatS3Key(filePath.getFileName().toString());
		PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3Client.putObject(putRequest, filePath);
	}

	private static void deleteObjectFromS3(S3Client s3Client, String bucketName, String objectKey) {
		DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build();

		DeleteObjectResponse response = s3Client.deleteObject(deleteRequest);
		logger.info("Deleted object: " + objectKey + " (Status: " + response.sdkHttpResponse().statusCode() + ")");
	}

	// object key is the name of the file in S3
	private static String getChecksumMetadata(S3Client s3Client, String bucketName, String objectKey) {
		logger.info("In getChecksumMetadata");
		HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(bucketName).key(objectKey).build();
		logger.info("headRequest = " + headRequest.toString());
		HeadObjectResponse response = null;
		try {
			logger.info("Submitting head object with headRequest = " + headRequest.toString());
			response = s3Client.headObject(headRequest);
			logger.info("Got response for head request = " + headRequest.toString());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		if (response != null && response.metadata() != null && response.metadata().get(s3FileMetadataKey) != null) {
			return response.metadata().get(s3FileMetadataKey); // metadata custom key
		} else {
			return null;
		}
	}

	public static String formatS3Key(String s3key) {
		return s3key.substring(s3key.lastIndexOf('_') + 1);
	}

	private static String calculateSha256(Path path) throws IOException {
		logger.debug("In calculateSha256");
		// Convert Path to File
		File file = path.toFile();
		if (!file.exists()) {
			logger.error("File does not exist: " + path.toString());
			return null;
		}
		return com.google.common.io.Files.asByteSource(file).hash(Hashing.sha256()).toString(); // Returns hex string
	}

	public static Map<String, Path> downloadFilesToTempDirectory(List<String> fileUrls) throws IOException {
		logger.info("In downloadFilesToTempDirectory");
		Map<String, Path> downloadedFiles = new HashMap<>();

		try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
			for (String url : fileUrls) {
				HttpGet request = new HttpGet(url);
				request.setHeader("User-Agent",
						"Mozilla/5.0 (compatible; BLSGovDataFetcher/1.0; +mailto:marshallmonica@yahoo.com)");
				byte[] fileContent = httpClient.execute(request, new FileDownloadResponseHandler());

				if (fileContent != null) {
					Path tempFile = Files.createTempFile("bls_", "_" + url.substring(url.lastIndexOf('/') + 1));
					Files.write(tempFile, fileContent);
					downloadedFiles.put(url, tempFile);
					logger.info("Downloaded: " + url);
				} else {
					logger.error("No content downloaded: " + url);
				}
			}
		}

		return downloadedFiles;
	}

	public static List<String> extractBlsGovFileLinks() throws IOException {
		logger.info("In extractBlsGovFileLinks");
		List<String> fileUrls = new ArrayList<>();

		Document doc = null;
		try {
			doc = Jsoup.connect(blsGovFilesURI)
					.userAgent("Mozilla/5.0 (compatible; MyBLSBot/1.0; +mailto:marshallmonica@yahoo.com)")
					.timeout(10_000).get();
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw e;
		}

		Elements links = doc.select("a[href]");

		for (Element link : links) {
			String text = link.childNodes().get(0).toString();
			if (!text.contains("To Parent Directory")) { // Skip parent directory
				fileUrls.add(blsGovFilesURI + text);
			}
		}

		return fileUrls;
	}

	private static class FileDownloadResponseHandler implements HttpClientResponseHandler<byte[]> {
		@Override
		public byte[] handleResponse(ClassicHttpResponse response) throws HttpException, IOException {
			int status = response.getCode();
			if (status >= 200 && status < 300) {
				HttpEntity entity = response.getEntity();
				return entity != null ? EntityUtils.toByteArray(entity) : null;
			} else {
				logger.error("Unexpected response status: " + status);
				throw new HttpException("Unexpected response status: " + status);
			}
		}
	}
}
