package com.rearc.quest.lambda.api;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.rearc.quest.lambda.api.config.S3ClientConfig;
import com.rearc.quest.lambda.api.dto.Order;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.nio.charset.StandardCharsets;

/**
 * This class handles orders submitted via Post request to this
 * Lambda, reads the order, persists to DynamoDB and publishes it to S3
 */
public class OrderCreateHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    //Write Orders to S3
    private static final S3Client s3Client = S3ClientConfig.getS3Client();
    private final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(OrderCreateHandler.class);
	
	private static final String bucketName = System.getenv("BUCKET_NAME");
	private final DynamoDbClient ddb = DynamoDbClient.create();
	private final Gson gson = new Gson();
    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent apiGatewayProxyRequestEvent, Context context) {

        LambdaLogger logger = context.getLogger();
        System.out.println("Context logger " + logger);
        System.out.println("Start handleRequest");
        String proxyRequestEvent = gson.toJson(apiGatewayProxyRequestEvent);
        System.out.println("Received request : " + proxyRequestEvent);
        String httpMethod = apiGatewayProxyRequestEvent.getHttpMethod();
        String requestEventBody = apiGatewayProxyRequestEvent.getBody();
        
        return switch (httpMethod) {
            case "POST" -> {
                Order order = gson.fromJson(requestEventBody, Order.class);
                Map<String, AttributeValue> itemValues = createMap(order);        
                PutItemRequest request = PutItemRequest.builder()
                        .tableName(System.getenv("ORDERS_TABLE"))
                        .item(itemValues)
                        .build();    

                ddb.putItem(request);             
                //Write Order to S3
                try {
					writeOrderToS3(order);
				} catch (JsonProcessingException e) {
					System.out.println("Caught Exception " + e.getMessage());
				}
                try {
					writeOrderToS3(gson.fromJson(requestEventBody, Order.class));
				} catch (JsonProcessingException e) {
					System.out.println("Caught exception " + e.getMessage());
					System.exit(1);
				} catch (JsonSyntaxException e) {
					System.out.println("Caught exception " + e.getMessage());
					System.exit(1);
				}
                try {
                    PutItemResponse response = ddb.putItem(request);
                    System.out.println(System.getenv("ORDERS_TABLE") + " was successfully updated. The request id is "
                            + response.responseMetadata().requestId());

                } catch (ResourceNotFoundException e) {
                    System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", 
                    		System.getenv("ORDERS_TABLE"));
                    System.err.println("Be sure that it exists and that you've typed its name correctly!");
                    System.exit(1);
                } catch (DynamoDbException e) {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
                yield createAPIResponse(String.valueOf(order.getId()), 200);
            }
            default -> throw new Error("FileSizeHandler:: Unsupported Methods:::" + httpMethod);
        };

    }

    private void writeOrderToS3(Order order) throws JsonProcessingException{

    	
		String key = String.valueOf(order.getId());
		String orderMessage = null;
		try {
			orderMessage = mapper.writeValueAsString(order);
		} catch (JsonProcessingException e) {
			logger.error(e.getMessage());
			throw e;
		}
		PutObjectRequest putRequest = PutObjectRequest.builder().bucket(bucketName).key(key).build();

		// Convert the receipt content to bytes and upload to S3
		s3Client.putObject(putRequest, RequestBody.fromBytes(orderMessage.getBytes(StandardCharsets.UTF_8)));

		System.out.println("Order succcessfully written to S3 with key: " + key);

    }
   
    private Map<String, AttributeValue> createMap(Order order){
        // Create a HashMap to store the converted data
        Map<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();
        itemValues.put("id", AttributeValue.builder().n(String.valueOf(order.getId())).build());
        itemValues.put("itemName", AttributeValue.builder().s(order.getItemName()).build());
        itemValues.put("quantity", AttributeValue.builder().n(String.valueOf(order.getQuantity())).build());
        // Print the HashMap to verify
        System.out.println(itemValues);
        return itemValues;
    }

    private static APIGatewayProxyResponseEvent createAPIResponse(String body, int statusCode) {
        APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
        responseEvent.setBody(body);
        responseEvent.setHeaders(createHeaders());
        responseEvent.setStatusCode(statusCode);
        return responseEvent;
    }

    private static Map<String, String> createHeaders() {
        Map<String, String> headers = new HashMap<String,String>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Headers", "*");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Methods", "POST");
        return headers;
    }
}
