package io.zulia.server.connection.server.validation;

/**
 * Created by Payam Meyer on 9/19/17.
 *
 * @author pmeyer
 */
public interface DefaultValidator<R> {

    R validateAndSetDefault(R request);

}
