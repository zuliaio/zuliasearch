package io.zulia.util;

import com.google.protobuf.InvalidProtocolBufferException;

public class Test {

	public static void main(String[] args) throws InvalidProtocolBufferException {

		System.out.println(ZuliaVersion.getMajor());
		System.out.println(ZuliaVersion.getMinor());
	}

}
