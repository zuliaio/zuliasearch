package io.zulia.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class HttpHelper {
	public static String createQuery(Map<String, Object> parameters) {

		StringBuilder sb = new StringBuilder();

		for (String key : parameters.keySet()) {

			Object value = parameters.get(key);
			if (value instanceof String) {
				if (!sb.isEmpty()) {
					sb.append('&');
				}

				sb.append(key);
				sb.append('=');

				sb.append(URLEncoder.encode((String) value, StandardCharsets.UTF_8));

			}
			else if (value instanceof List<?> stringList) {
				for (Object item : stringList) {

					if (!sb.isEmpty()) {
						sb.append('&');
					}

					sb.append(key);
					sb.append('=');

					sb.append(URLEncoder.encode(item.toString(), StandardCharsets.UTF_8));

				}
			}
		}
		return sb.toString();
	}


}
