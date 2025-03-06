package io.zulia.ui.rest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.password4j.BcryptFunction;
import com.password4j.Password;
import com.password4j.types.Bcrypt;
import io.zulia.ui.rest.beans.UserEntity;
import jakarta.inject.Singleton;

@Singleton
public class MongoUserPersistence implements UserService {

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
	public void createUser(UserEntity user) {
		String rawPassword = user.getPassword();
		String hashedPassword = Password.hash(rawPassword).addRandomSalt().withBcrypt().getResult();
		user.setHashedPassword(hashedPassword);
		user.setPassword(null);
		getCollection().replaceOne(Filters.eq(user.getUsername()), user, new ReplaceOptions().upsert(true));
	}

	private MongoCollection<UserEntity> getCollection() {
		return mongoClient.getDatabase(mongoConf.getName()).getCollection(mongoConf.getCollection(), UserEntity.class);
	}
}
