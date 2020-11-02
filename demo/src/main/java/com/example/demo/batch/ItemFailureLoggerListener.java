package com.example.demo.batch;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.listener.ItemListenerSupport;

public class ItemFailureLoggerListener extends ItemListenerSupport {
	private static Log logger = LogFactory.getLog("ITEM ERROR");

	@Override
	public void onReadError(Exception ex) {
		logger.error("Read Error", ex);
	}

	@Override
	public void onProcessError(Object item, Exception ex) {
		logger.error(item);
	}

	@Override
	public void onWriteError(Exception ex, List item) {
		logger.error("Write Error", ex);
	}
}
