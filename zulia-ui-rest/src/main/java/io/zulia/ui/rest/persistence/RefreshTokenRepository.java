package io.zulia.ui.rest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.mongodb.annotation.MongoRepository;
import io.micronaut.data.repository.CrudRepository;
import io.zulia.ui.rest.beans.RefreshTokenEntity;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@MongoRepository
public abstract class RefreshTokenRepository implements CrudRepository<RefreshTokenEntity, String> {

	// Unfortunately mongo doesn't handle creating indexes based on annotations, so we are creating a TTL index for dateCreated to clean up after a day
	private static final String DATABASE = "zuliaAuth";
	private static final String COLLECTION = "refreshTokens";

	@Inject
	MongoClient mongoClient;

	@PostConstruct
	public void createIndex() {
		mongoClient.getDatabase(DATABASE).getCollection(COLLECTION)
				.createIndex(Indexes.ascending("dateCreated"), new IndexOptions().background(true).expireAfter(24L, TimeUnit.HOURS));
	}

	public abstract RefreshTokenEntity save(@NonNull @NotBlank String username, @NonNull @NotBlank String refreshToken, @NonNull @NotNull Boolean revoked);

	public abstract Optional<RefreshTokenEntity> findByRefreshToken(@NonNull @NotBlank String refreshToken);

	public abstract long updateByUsername(@NonNull @NotBlank String username, boolean revoked);

	public abstract long delete(@NonNull @NotBlank String username);

}