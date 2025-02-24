package io.zulia.ui.rest.beans;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.GeneratedValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

@MappedEntity(value = "refreshTokens")
public class RefreshTokenEntity {

	@Id
	@GeneratedValue
	@NonNull
	private String id;

	@NonNull
	@NotBlank
	private String username;

	@NonNull
	@NotBlank
	private String refreshToken;

	@NonNull
	@NotNull
	private Boolean revoked;

	@DateCreated
	@NonNull
	@NotNull
	private Instant dateCreated;

	public RefreshTokenEntity() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getRefreshToken() {
		return refreshToken;
	}

	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}

	public Boolean getRevoked() {
		return revoked;
	}

	public void setRevoked(Boolean revoked) {
		this.revoked = revoked;
	}

	public Instant getDateCreated() {
		return dateCreated;
	}

	public void setDateCreated(Instant dateCreated) {
		this.dateCreated = dateCreated;
	}
}
