package com.rearc.quest.lambda.api.dto;

public class Order {

	private String itemName;
	private int quantity;
	private int id;
	
	public Order() {
		
	}
	public Order (int id, String itemName, int quantity) {
		this.id = id;
		this.itemName = itemName;
		this.quantity = quantity;
	}
	

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public int getQuantity() {
		return quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	
    @Override
    public String toString() {
    	return("Order [id: " + this.getId() + " quantity: " + this.getQuantity() + 
    			" itemName: " + this.getItemName() + "]");
    }
}
