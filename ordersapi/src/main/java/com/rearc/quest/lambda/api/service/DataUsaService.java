package com.rearc.quest.lambda.api.service;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rearc.quest.lambda.api.config.S3ClientConfig;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class DataUsaService {
	
	private static final S3Client s3Client = S3ClientConfig.getS3Client();

	private static final Logger logger = LoggerFactory.getLogger(DataUsaService.class);
	private static final String url = 
	"https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population";
	private final static String bucketName = "s3upload-lambda-bucket";
	//private static final String bucketName = System.getenv("BUCKET_NAME");
	private final static String datausaFolder = System.getenv("FOLDER_NAME");

	public static Map<String, Path> downloadPopulation() throws Exception {
		logger.info("In downloadPopulation");
		HttpGet request = null;

		Map<String, Path> downloadedFiles = new HashMap<>();

		try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
			request = new HttpGet(url.substring(0, url.indexOf('?')));
			Map<String, String> queryParams = parseQueryParams(url);

			List<NameValuePair> nvps = new ArrayList<>();

			queryParams.forEach((k, v) -> nvps.add(new BasicNameValuePair(k, v)));

			URI uri = new URIBuilder(new URI(url.substring(0, url.indexOf('?')))).addParameters(nvps).build();
			request.setUri(uri);

			byte[] fileContent = httpClient.execute(request, new FileDownloadResponseHandler());

			if (fileContent != null) {
				Path tempFile = Files.createTempFile("datausa_", "_" + "population");

				Files.write(tempFile, fileContent);
				downloadedFiles.put(url, tempFile);
				logger.info("Downloaded: " + url);
			} else {
				logger.error("No content downloaded: " + url);
			}
		}
		return downloadedFiles;
	}

	public static void uploadFilesToS3(Map<String, Path> downloadedFiles) throws IOException {
		logger.debug("In uploadFilesToS3");
		for (Map.Entry<String, Path> entry : downloadedFiles.entrySet()) {
			Path filePath = entry.getValue();
			String s3Key = datausaFolder + formatS3Key(entry.getValue().getFileName().toString());
			// ifNoneMatch = don't overwrite the existing file
			PutObjectRequest putRequest = PutObjectRequest.builder()
	                .bucket(bucketName)
	                .key(s3Key)
	                .build();

	        s3Client.putObject(putRequest, filePath);
		}
	}
	
	public void uploadFilesToS3(Map<String, Path> downloadedFiles, S3Client s3Client) throws IOException {
		logger.debug("In uploadFilesToS3");
		for (Map.Entry<String, Path> entry : downloadedFiles.entrySet()) {
			Path filePath = entry.getValue();
			String s3Key = datausaFolder + formatS3Key(entry.getValue().getFileName().toString());
			// ifNoneMatch = don't overwrite the existing file
			PutObjectRequest putRequest = PutObjectRequest.builder()
		            .bucket(bucketName)
		            .key(s3Key)
		            .build();

		    // This is the correct v2 call
		    s3Client.putObject(putRequest, filePath);
		}
	}
	
	private static String formatS3Key(String s3key) {
		return s3key.substring(s3key.lastIndexOf('_') + 1) + ".json";
	}

	private static Map<String, String> parseQueryParams(String url) throws Exception {
		URI uri = new URI(url);
		String query = uri.getQuery(); // Gets the query string after '?'

		Map<String, String> params = new LinkedHashMap<>();
		for (String param : query.split("&")) {
			String[] pair = param.split("=", 2);
			String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
			String value = pair.length > 1 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
			params.put(key, value);
		}
		return params;
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
