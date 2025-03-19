package io.zulia.ui.rest.util;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.zulia.ui.rest.beans.UserEntity;

import java.util.Collections;

public class UserCreator {

	private static final EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	private static final HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());

	public static void main(String[] args) {
		UserEntity user = new UserEntity();
		user.setUsername("zulia");
		user.setPassword("password");
		HttpRequest<?> request = HttpRequest.POST("/zuliauirest/create-user", user); // <4>
		HttpResponse<BearerAccessRefreshToken> rsp = client.toBlocking().exchange(request, BearerAccessRefreshToken.class); // <5>
		embeddedServer.stop();
	}

}
