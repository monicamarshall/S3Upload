package com.rearc.quest.lambda.api;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.rearc.quest.lambda.api.config.S3ClientConfig;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class ReadOrderFromS3PubToSNSLambda implements RequestHandler<S3Event, Void> {

	private static final String ENV_AWS_REGION = System.getenv().getOrDefault("AWS_REGION", "us-east-2");
	private static final Region REGION = Region.of(ENV_AWS_REGION);

	private static final String TOPIC_ARN = System.getenv("SNS_TOPIC_ARN");

	private static final S3Client s3 = S3ClientConfig.getS3Client();

	private static final SnsClient sns = SnsClient.builder().region(REGION).build();		

	static {
		if (TOPIC_ARN == null || TOPIC_ARN.isBlank()) {
			throw new IllegalStateException("SNS_TOPIC_ARN env var is not set — cannot start function.");
		}
	}

	@Override
	public Void handleRequest(S3Event s3event, Context context) {
		final LambdaLogger log = context.getLogger();
		log.log("Lambda coldstart check — region=" + REGION + ", topicArn=" + TOPIC_ARN + "\n");

		try {
			s3event.getRecords().forEach(record -> {
				final String bucket = record.getS3().getBucket().getName();
				final String key = URLDecoder.decode(record.getS3().getObject().getKey(), StandardCharsets.UTF_8);
				log.log("Processing S3 object s3://" + bucket + "/" + key + "\n");

				final GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key).build();

				try (ResponseInputStream<?> is = s3.getObject(req);
						BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

					StringBuilder body = new StringBuilder();
					String line;
					while ((line = reader.readLine()) != null) {
						body.append(line).append('\n');
						if (body.length() > 240_000) {
							body.append("\n[truncated]");
							break;
						}
					}

					final String subject = "New S3 object processed";
					final String message = "Bucket: " + bucket + "\nKey: " + key + "\n\n" + body;

					sns.publish(PublishRequest.builder().topicArn(TOPIC_ARN).subject(subject).message(message).build());

					log.log("Published to SNS topic " + TOPIC_ARN + " for s3://" + bucket + "/" + key + "\n");

				} catch (Exception e) {
					// Log full error, then rethrow so the invocation is marked as a failure (and S3
					// will retry)
					log.log("Error processing s3://" + bucket + "/" + key + " : " + e + "\n");
					throw new RuntimeException(e);
				}
			});
		} catch (RuntimeException e) {
			// Surface as a function error so it shows up clearly in CloudWatch + S3 retries
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
