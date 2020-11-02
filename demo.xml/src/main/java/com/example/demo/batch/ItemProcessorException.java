package com.example.demo.batch;

public class ItemProcessorException extends Exception {
	private static final long serialVersionUID = 1L;

	public ItemProcessorException() {
	}

	public ItemProcessorException(String message) {
		super(message);
	}

	public ItemProcessorException(Throwable cause) {
		super(cause);
	}

	public ItemProcessorException(String message, Throwable cause) {
		super(message, cause);
	}

	public ItemProcessorException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
