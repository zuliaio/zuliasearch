package io.zulia.ui.rest.persistence;

import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.errors.OauthErrorResponseException;
import io.micronaut.security.token.event.RefreshTokenGeneratedEvent;
import io.micronaut.security.token.refresh.RefreshTokenPersistence;
import io.zulia.ui.rest.beans.RefreshTokenEntity;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;
import java.util.Optional;

import static io.micronaut.security.errors.IssuingAnAccessTokenErrorCode.INVALID_GRANT;

@Singleton
public class MongoRefreshTokenPersistence implements RefreshTokenPersistence {

	private final RefreshTokenRepository refreshTokenRepository;

	public MongoRefreshTokenPersistence(RefreshTokenRepository refreshTokenRepository) {
		this.refreshTokenRepository = refreshTokenRepository;
	}

	@Override
	public void persistToken(RefreshTokenGeneratedEvent event) {
		if (event != null && event.getRefreshToken() != null && event.getAuthentication() != null && event.getAuthentication().getName() != null) {
			String payload = event.getRefreshToken();
			refreshTokenRepository.save(event.getAuthentication().getName(), payload, false);
		}
	}

	@Override
	public Publisher<Authentication> getAuthentication(String refreshToken) {
		return Flux.create(emitter -> {
			Optional<RefreshTokenEntity> tokenOpt = refreshTokenRepository.findByRefreshToken(refreshToken);
			if (tokenOpt.isPresent()) {
				RefreshTokenEntity token = tokenOpt.get();
				if (token.getRevoked()) {
					emitter.error(new OauthErrorResponseException(INVALID_GRANT, "refresh token revoked", null));
				}
				else {
					emitter.next(Authentication.build(token.getUsername(), List.of("ADMIN")));
					emitter.complete();
				}
			}
			else {
				emitter.error(new OauthErrorResponseException(INVALID_GRANT, "refresh token not found", null));
			}
		}, FluxSink.OverflowStrategy.ERROR);
	}
}
