package io.zulia.server.script;

import org.bson.Document;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.proxy.ProxyObject;

public class Parse {

	public static void main(String[] args) throws java.io.IOException {

		try (Context context = Context.newBuilder("js").allowHostAccess(HostAccess.ALL).build()) {
			Document doc = new Document().append("firstName", "Tom").append("lastName", "Smith");

			context.getBindings("js").putMember("d", ProxyObject.fromMap(doc));
			context.eval("js", "d['fullName'] = d['firstName'] + ' ' + d['lastName']");
			System.out.println(doc);
		}
	}
}
