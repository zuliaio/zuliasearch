package io.zulia.server.rest.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.zulia.server.rest.ZuliaRESTService;
import io.zulia.util.ZuliaVersion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Graeme Rocher
 * @since 1.0
 */
@Controller
public class WelcomeController {

	@Get
	public HttpResponse<String> welcome() {

		try {
			StringBuilder html = new StringBuilder();
			Path path = Paths.get(ZuliaRESTService.class.getResource("/rest_index.html").getFile());
			Files.readAllLines(path).forEach(html::append);
			return HttpResponse.ok(html.toString()).header("Content-Type", "text/html");
		}
		catch (Exception e) {
			e.printStackTrace();
			return HttpResponse.ok("Welcome to Zulia's REST Service.\nVersion " + ZuliaVersion.getVersion());
		}

	}
}