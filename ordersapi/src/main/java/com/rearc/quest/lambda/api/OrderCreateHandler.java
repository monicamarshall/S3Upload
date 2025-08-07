package com.rearc.quest.lambda.api;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.rearc.quest.lambda.api.dto.Order;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

public class OrderCreateHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

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
