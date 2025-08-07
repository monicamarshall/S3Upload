package com.rearc.quest.lambda.api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class AWSSecretsManager {
	private static final Logger logger = LoggerFactory.getLogger(AWSSecretsManager.class);

	private static String accessSecret = "dev/aws-access-secret";
	private static String accessKeyId = "dev/aws-access-key-id";
	private static AwsCredentials awsCredentials;

	static {
		try {
			// First try to get credentials from environment variables
			String envAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
			String envSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
			
			if (envAccessKey != null && !envAccessKey.isEmpty() && envSecretKey != null && !envSecretKey.isEmpty()) {
				logger.info("Using AWS credentials from environment variables");
				awsCredentials = AwsBasicCredentials.create(envAccessKey, envSecretKey);
			} else {
				// Try to get from Secrets Manager as a fallback
				try {
					Region region = Region.of("us-east-2");
					
					// Create a Secrets Manager client
					SecretsManagerClient client = SecretsManagerClient.builder().region(region).build();
					
					GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder().secretId(accessSecret)
							.build();
					GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
					accessSecret = getSecretValueResponse.secretString();
					getSecretValueRequest = GetSecretValueRequest.builder().secretId(accessKeyId).build();
					getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
					accessKeyId = getSecretValueResponse.secretString();
					awsCredentials = AwsBasicCredentials.create(extractObjectKey(accessKeyId), 
							extractObjectSecret(accessSecret));
					logger.info("AWS Credentials initialized from Secrets Manager");
				} catch (Exception ex) {
					logger.error("Error initializing AWS credentials from Secrets Manager: {}", ex.getMessage());
					// Default to dummy credentials for local development
					awsCredentials = AwsBasicCredentials.create("dummy-key", "dummy-secret");
					logger.warn("Using dummy credentials for local development");
				}
			}
		} catch (Exception ex) {
			logger.error("Error initializing AWS credentials: {}", ex.getMessage());
			// Default to dummy credentials for local development
			awsCredentials = AwsBasicCredentials.create("dummy-key", "dummy-secret");
			logger.warn("Using dummy credentials for local development");
		}
	}

	public static String extractObjectKey(String json) {
		Gson gson = new Gson();
		SecretValues values = gson.fromJson(json, SecretValues.class);
		return values.getObjectKey();
	}
	
	public static String extractObjectSecret(String json) {
		Gson gson = new Gson();
		SecretValues values = gson.fromJson(json, SecretValues.class);
		return values.getObjectSecret();
	}

	public static AwsCredentials getAwsCredentials() {
		return awsCredentials;
	}

	public static String getAccessSecret() {
		return accessSecret;
	}

	public static String getAccessKeyId() {
		return accessKeyId;
	}

	public static class SecretValues {
		private String aws_access_key_id;
		private String aws_access_secret;

		public String getObjectKey() {
			return aws_access_key_id;
		}

		public String getObjectSecret() {
			return aws_access_secret;
		}
	}
}
