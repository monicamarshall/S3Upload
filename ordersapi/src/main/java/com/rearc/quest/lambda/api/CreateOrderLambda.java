package com.rearc.quest.lambda.api;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rearc.quest.lambda.api.dto.Order;

public class CreateOrderLambda {
	private final ObjectMapper mapper = new ObjectMapper();
	
	public APIGatewayProxyResponseEvent createOrder(APIGatewayProxyRequestEvent request)
	throws Exception {

		try {
			Order myorder = mapper.readValue(request.getBody(), Order.class);
			System.out.println("Created Order from RequestBody with id = " + myorder.getId());			
		} catch (JsonMappingException e) {
			System.out.println("Caught JsonMappingException " + e.getMessage());
		} catch (JsonProcessingException e) {
			System.out.println("Caught JsonProcessingException " + e.getMessage());
		}
		return new APIGatewayProxyResponseEvent().withStatusCode(200).withBody("Order Created!");
	}

}
