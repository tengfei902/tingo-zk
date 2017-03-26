package com.tingo.zk.remote.impl;

import com.tingo.zk.remote.IHelloClient;
import org.springframework.stereotype.Component;

@Component
public class HelloClient implements IHelloClient{
	@Override
	public String hello(String username) {
		return null;
	}
}
