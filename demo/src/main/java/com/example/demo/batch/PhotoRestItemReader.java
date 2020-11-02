package com.example.demo.batch;

import java.util.Iterator;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.example.demo.model.PhotoDTO;
import com.example.demo.proxies.PhotoProxy;

@Component
public class PhotoRestItemReader implements ItemReader<PhotoDTO>, ItemStream {
	private Iterator<PhotoDTO> cache;
	@Autowired
	private PhotoProxy srv;	
	
	@Override
	public PhotoDTO read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return cache != null && cache.hasNext() ? cache.next() : null;
	}
	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		cache = srv.getAll().iterator();
	}
	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		// TODO Auto-generated method stub		
	}
	@Override
	public void close() throws ItemStreamException {
		cache = null;
	}
}
