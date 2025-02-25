package io.zulia.ui.rest.persistence;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.mongodb.annotation.MongoRepository;
import io.micronaut.data.repository.CrudRepository;
import io.zulia.ui.rest.beans.RefreshTokenEntity;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

@MongoRepository
public abstract class RefreshTokenRepository implements CrudRepository<RefreshTokenEntity, String> {

	public abstract RefreshTokenEntity save(@NonNull @NotBlank String username, @NonNull @NotBlank String refreshToken, @NonNull @NotNull Boolean revoked);

	public abstract Optional<RefreshTokenEntity> findByRefreshToken(@NonNull @NotBlank String refreshToken);

	public abstract long updateByUsername(@NonNull @NotBlank String username, boolean revoked);

}