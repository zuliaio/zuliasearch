package io.zulia.ui.rest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.mongodb.annotation.MongoRepository;
import io.micronaut.data.repository.CrudRepository;
import io.zulia.ui.rest.beans.RefreshTokenEntity;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

@MongoRepository(databaseName = "zuliaAuth")
public abstract class RefreshTokenRepository implements CrudRepository<RefreshTokenEntity, String> {

	private static final String DATABASE_NAME = "zuliaAuth";
	private static final String COLLECTION_NAME = "refreshTokens";

	@Inject
	MongoClient mongoClient;

	public abstract RefreshTokenEntity save(@NonNull @NotBlank String username, @NonNull @NotBlank String refreshToken, @NonNull @NotNull Boolean revoked);

	public abstract Optional<RefreshTokenEntity> findByRefreshToken(@NonNull @NotBlank String refreshToken);

	public abstract long updateByUsername(@NonNull @NotBlank String username, boolean revoked);

	public long countDocs() {
		return mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).countDocuments();
	}

	public void deleteAllByUsername(String username) {
		mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).deleteMany(Filters.eq("username", username));
	}

}