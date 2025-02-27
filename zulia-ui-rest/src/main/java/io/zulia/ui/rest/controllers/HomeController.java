package io.zulia.ui.rest.controllers;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

import java.security.Principal;

@Secured(SecurityRule.IS_AUTHENTICATED)
@Controller
public class HomeController {

	@Produces(MediaType.TEXT_PLAIN)
	@Get
	public String home(Principal principal) {
		System.out.println("was able to hit secured method with any role: " + principal.getName());
		return principal.getName();
	}

	@Produces(MediaType.TEXT_PLAIN)
	@Get("/home-test")
	public String homeTest(Principal principal) {
		System.out.println("was able to hit secured method with any role: " + principal.getName());
		return principal.getName();
	}

	@Produces(MediaType.TEXT_PLAIN)
	@Get("/forbidden-test")
	@Secured("USER")
	public String forbiddenTest(Principal principal) {
		System.out.println("was able to hit secured method with USER role: " + principal.getName());
		return principal.getName();
	}

	@Produces(MediaType.TEXT_PLAIN)
	@Get("/home-test-admin")
	@Secured("ADMIN")
	public String homeTestAdmin(Principal principal) {
		System.out.println("was able to hit secured method with ADMIN role: " + principal.getName());
		return principal.getName();
	}
}