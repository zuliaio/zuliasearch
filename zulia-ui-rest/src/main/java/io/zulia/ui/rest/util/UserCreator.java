package io.zulia.ui.rest.util;

import io.micronaut.context.ApplicationContext;
import io.zulia.ui.rest.beans.UserEntity;
import io.zulia.ui.rest.persistence.MongoUserPersistence;

import java.util.Arrays;
import java.util.List;

/**
 * Offline bootstrap for the first user. The /create-user HTTP endpoint now requires the ADMIN
 * role, so the initial account is provisioned directly through the persistence layer instead of
 * an anonymous HTTP call. Pass the username and password as arguments rather than hardcoding them.
 * The bootstrap account defaults to the ADMIN role so it can administer the service; pass explicit
 * roles to override.
 */
public class UserCreator {

	static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: UserCreator <username> <password> [role...]  (default role: ADMIN)");
			System.exit(1);
			return;
		}

		List<String> roles = args.length > 2 ? List.of(Arrays.copyOfRange(args, 2, args.length)) : List.of("ADMIN");

		try (ApplicationContext context = ApplicationContext.run()) {
			MongoUserPersistence userPersistence = context.getBean(MongoUserPersistence.class);
			UserEntity user = new UserEntity();
			user.setUsername(args[0]);
			user.setPassword(args[1]);
			user.setRoles(roles);
			userPersistence.createUser(user);
			System.out.println("Created user '" + args[0] + "' with roles " + roles);
		}
	}

}
