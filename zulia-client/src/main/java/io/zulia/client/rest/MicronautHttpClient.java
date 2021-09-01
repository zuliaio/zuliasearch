package io.zulia.client.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.annotation.AnnotationMetadataResolver;
import io.micronaut.core.io.ResourceResolver;
import io.micronaut.http.HttpVersion;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.LoadBalancer;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.ssl.NettyClientSslBuilder;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.micronaut.jackson.ObjectMapperFactory;
import io.micronaut.jackson.codec.JsonMediaTypeCodec;
import io.micronaut.jackson.codec.JsonStreamMediaTypeCodec;
import io.micronaut.runtime.ApplicationConfiguration;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.logging.Logger;

import static java.time.temporal.ChronoUnit.SECONDS;

public class MicronautHttpClient extends DefaultHttpClient {

	private static final Logger LOG = Logger.getLogger(MicronautHttpClient.class.getName());

	public MicronautHttpClient(URL url, HttpClientConfiguration clientConfiguration) {
		super(LoadBalancer.fixed(url), clientConfiguration, null, new DefaultThreadFactory(MicronautHttpClient.class, true),
				new NettyClientSslBuilder(new ResourceResolver()), createDefaultMediaTypeRegistry(), AnnotationMetadataResolver.DEFAULT, null);

	}

	private static MediaTypeCodecRegistry createDefaultMediaTypeRegistry() {
		ObjectMapper objectMapper = new ObjectMapperFactory().objectMapper(null, null);
		ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
		return MediaTypeCodecRegistry.of(new JsonMediaTypeCodec(objectMapper, applicationConfiguration, null),
				new JsonStreamMediaTypeCodec(objectMapper, applicationConfiguration, null));
	}

	public static MicronautHttpClient createClient(String url) {
		URL uri;
		try {
			LOG.info("Creating REST client pool to " + url);
			uri = new URI(url).toURL();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		DefaultHttpClientConfiguration.DefaultConnectionPoolConfiguration defaultConnectionPoolConfiguration = new DefaultHttpClientConfiguration.DefaultConnectionPoolConfiguration();
		defaultConnectionPoolConfiguration.setEnabled(true);
		defaultConnectionPoolConfiguration.setAcquireTimeout(Duration.of(90, SECONDS));
		defaultConnectionPoolConfiguration.setMaxConnections(64);
		defaultConnectionPoolConfiguration.setMaxPendingAcquires(64);
		ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
		HttpClientConfiguration clientConfiguration = new DefaultHttpClientConfiguration(defaultConnectionPoolConfiguration, applicationConfiguration);
		clientConfiguration.setMaxContentLength(1024 * 1024 * 1024);
		clientConfiguration.setReadTimeout(Duration.ofSeconds(600));
		clientConfiguration.setHttpVersion(HttpVersion.HTTP_2_0);

		return new MicronautHttpClient(uri, clientConfiguration);
	}
}
