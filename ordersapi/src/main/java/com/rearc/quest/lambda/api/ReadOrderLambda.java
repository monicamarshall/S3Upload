package com.rearc.quest.lambda.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.rearc.quest.lambda.api.dto.Order;

public class ReadOrderLambda {
	//private final ObjectMapper mapper = new ObjectMapper();
	private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();

	public APIGatewayProxyResponseEvent readOrder(APIGatewayProxyRequestEvent request) throws Exception {
		/**
		Order order = new Order(123, "Shoes", 88);
		String orderMessage = null;

		try {
			orderMessage = mapper.writeValueAsString(order);
		} catch (JsonMappingException e) {
			System.out.println("Caught JsonMappingException " + e.getMessage());
		} catch (JsonProcessingException e) {
			System.out.println("Caught JsonProcessingException " + e.getMessage());
		}
		*/
		ScanRequest scanRequest = new ScanRequest().withTableName(System.getenv("ORDERS_TABLE"));

		// Perform the scan operation
		ScanResult scanResult = dynamoDB.scan(scanRequest);
		List<Map<String, AttributeValue>> results = scanResult.getItems();
		Map<String, AttributeValue> lastKey = null;
		List<Order> orders = new ArrayList<Order>();
		do {
			results.forEach(r -> orders.add(createOrder(Integer.parseInt(r.get("id").getN()), r.get("itemName").getS(),
					Integer.parseInt(r.get("quantity").getN()))));
			lastKey = scanResult.getLastEvaluatedKey();
			scanRequest.setExclusiveStartKey(lastKey);

		} while (lastKey != null);
		System.out.println(orders);
		return new APIGatewayProxyResponseEvent().withStatusCode(200).withBody(orders.toString());
	}

	private Order createOrder(int id, String itemName, int quantity) {
		return new Order(id, itemName, quantity);
	}
}
