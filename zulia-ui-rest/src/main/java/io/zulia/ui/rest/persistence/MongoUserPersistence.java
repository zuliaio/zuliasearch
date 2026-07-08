package io.zulia.ui.rest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.password4j.BcryptFunction;
import com.password4j.Password;
import com.password4j.types.Bcrypt;
import io.zulia.ui.rest.beans.UserEntity;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
public class MongoUserPersistence implements UserService {

	// New users default to the least-privilege role. Admins are granted explicitly (see UserCreator).
	private static final List<String> DEFAULT_ROLES = List.of("USER");
	private static final BcryptFunction BCRYPT_FUNCTION = BcryptFunction.getInstance(Bcrypt.B, 12);
	private final MongoUserConfiguration mongoConf;
	private final MongoClient mongoClient;

	public MongoUserPersistence(MongoUserConfiguration mongoConf, MongoClient mongoClient) {
		this.mongoConf = mongoConf;
		this.mongoClient = mongoClient;
	}

	@Override
	public UserEntity getUser(String username) {
		return getCollection().find(Filters.eq(username)).first();
	}

	@Override
	public boolean verifyUser(String username, String password) {
		UserEntity user = getUser(username);
		return Password.check(password, user.getHashedPassword()).withBcrypt();
	}

	@Override
	public List<String> getRoles(String username) {
		return effectiveRoles(getUser(username));
	}

	@Override
	public void createUser(UserEntity user) {
		String rawPassword = user.getPassword();
		String hashedPassword = Password.hash(rawPassword).withBcrypt().getResult();
		user.setHashedPassword(hashedPassword);
		user.setPassword(null);
		if (user.getRoles() == null || user.getRoles().isEmpty()) {
			user.setRoles(DEFAULT_ROLES);
		}
		// insertOne relies on the unique _id (username) constraint, so an existing user is never
		// silently overwritten. A duplicate username fails fast with a write error instead of
		// clobbering the existing account's credentials.
		getCollection().insertOne(user);
	}

	// Fail closed to least privilege for unknown users or legacy records that predate the roles field.
	private static List<String> effectiveRoles(UserEntity user) {
		if (user == null || user.getRoles() == null || user.getRoles().isEmpty()) {
			return DEFAULT_ROLES;
		}
		return user.getRoles();
	}

	private MongoCollection<UserEntity> getCollection() {
		return mongoClient.getDatabase(mongoConf.getName()).getCollection(mongoConf.getCollection(), UserEntity.class);
	}
}
