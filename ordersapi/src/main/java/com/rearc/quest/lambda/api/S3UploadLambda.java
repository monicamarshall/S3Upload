package com.rearc.quest.lambda.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.rearc.quest.lambda.api.service.DataUsaService;
import com.rearc.quest.lambda.api.service.S3Service;

public class S3UploadLambda {

	private static final Logger logger = LoggerFactory.getLogger(S3UploadLambda.class);

	public APIGatewayProxyResponseEvent uploadBlsGovPopulationFilesToS3(APIGatewayProxyRequestEvent request) 
			throws Exception {
		logger.info("In uploadBlsGovPopulationFilesToS3");
		S3Service.uploadFilesToS3(S3Service.downloadFilesToTempDirectory(S3Service.extractBlsGovFileLinks())); 
        logger.info("Completed upload of bls files");
        DataUsaService.uploadFilesToS3(DataUsaService.downloadPopulation());
        logger.info("Completed upload of datausa files");
		return new APIGatewayProxyResponseEvent().withStatusCode(200).withBody("bls-gov and datausa population files upload to s3 complete!");
    }
}
