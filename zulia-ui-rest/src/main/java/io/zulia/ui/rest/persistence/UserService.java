package io.zulia.ui.rest.persistence;

import io.zulia.ui.rest.beans.UserEntity;

public interface UserService {

	UserEntity getUser(String username);

	boolean verifyUser(String username, String password);

	void createUser(UserEntity user);

}
