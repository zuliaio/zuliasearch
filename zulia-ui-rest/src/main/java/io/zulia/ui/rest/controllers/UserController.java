package io.zulia.ui.rest.controllers;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.zulia.ui.rest.beans.UserEntity;
import io.zulia.ui.rest.persistence.MongoUserPersistence;

// User provisioning requires an authenticated ADMIN. The first user must be bootstrapped
// offline via UserCreator; an anonymous create endpoint would let anyone self-provision admin.
@Secured("ADMIN")
@Controller
@Requires(beans = MongoUserPersistence.class)
public class UserController {

	private final MongoUserPersistence mongoUserPersistence;

	public UserController(MongoUserPersistence mongoUserPersistence) {
		this.mongoUserPersistence = mongoUserPersistence;
	}

	@Produces(MediaType.TEXT_PLAIN)
	@Post("/create-user")
	public void createUser(@Body UserEntity user) {
		mongoUserPersistence.createUser(user);
	}

}