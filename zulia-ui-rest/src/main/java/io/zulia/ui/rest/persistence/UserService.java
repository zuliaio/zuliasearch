package io.zulia.ui.rest.persistence;

import io.zulia.ui.rest.beans.UserEntity;

import java.util.List;

public interface UserService {

	UserEntity getUser(String username);

	boolean verifyUser(String username, String password);

	List<String> getRoles(String username);

	void createUser(UserEntity user);

}
