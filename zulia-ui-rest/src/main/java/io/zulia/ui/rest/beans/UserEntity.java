package io.zulia.ui.rest.beans;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;

@Serdeable
@MappedEntity(value = "users")
public class UserEntity {

	@Id
	@NonNull
	private String username;

	@NonNull
	@NotNull
	private String password;

	@NonNull
	@NotNull
	private String hashedPassword;

	@DateCreated
	@NonNull
	@NotNull
	private Instant dateCreated;

	@Nullable
	private List<String> roles;

	public UserEntity() {
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHashedPassword() {
		return hashedPassword;
	}

	public void setHashedPassword(String hashedPassword) {
		this.hashedPassword = hashedPassword;
	}

	public Instant getDateCreated() {
		return dateCreated;
	}

	public void setDateCreated(Instant dateCreated) {
		this.dateCreated = dateCreated;
	}

	@Nullable
	public List<String> getRoles() {
		return roles;
	}

	public void setRoles(@Nullable List<String> roles) {
		this.roles = roles;
	}
}
