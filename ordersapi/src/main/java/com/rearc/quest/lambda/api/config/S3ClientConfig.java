package com.rearc.quest.lambda.api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientConfig {
	
	private static final Logger logger = LoggerFactory.getLogger(S3ClientConfig.class);
    private static S3Client s3Client;
    
    public static S3Client getS3Client() {

        try {
            s3Client = S3Client.builder()
                    .region(Region.US_EAST_2) // or use Region.of(System.getenv("AWS_REGION"))
                    .build();
            logger.info("S3Client successfully built");
        } catch (Exception ex) {
            logger.error("Error initializing S3Client", ex);
        }
        return s3Client;
    }
}
