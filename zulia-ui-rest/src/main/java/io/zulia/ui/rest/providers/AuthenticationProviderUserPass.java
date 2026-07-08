package io.zulia.ui.rest.providers;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationFailureReason;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.provider.HttpRequestAuthenticationProvider;
import io.zulia.ui.rest.persistence.MongoUserPersistence;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class AuthenticationProviderUserPass<B> implements HttpRequestAuthenticationProvider<B> {

	@Inject
	MongoUserPersistence mongoUserPersistence;

	@Override
	public @NonNull AuthenticationResponse authenticate(@Nullable HttpRequest<B> requestContext, @NonNull AuthenticationRequest<String, String> authRequest) {

		String username = authRequest.getIdentity();
		boolean userVerified = mongoUserPersistence.verifyUser(username, authRequest.getSecret());
		return userVerified ?
				AuthenticationResponse.success(username, mongoUserPersistence.getRoles(username)) :
				AuthenticationResponse.failure(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH);
	}
}
