package com.test;

import lombok.Data;

@Data
public class Sku implements java.io.Serializable {

	private static final long serialVersionUID = 8364394059562028701L;

	private Long id;

	private Long itemId;

	private String sku;

	private String detailName;

	private String barcode;

}