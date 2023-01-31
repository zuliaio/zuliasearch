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
                if (sb.length() > 0) {
                    sb.append('&');
                }

                sb.append(key);
                sb.append('=');

                sb.append(URLEncoder.encode((String) value, StandardCharsets.UTF_8));

            } else if (value instanceof List) {
                List<String> stringList = (List<String>) value;
                for (String item : stringList) {

                    if (sb.length() > 0) {
                        sb.append('&');
                    }

                    sb.append(key);
                    sb.append('=');

                    sb.append(URLEncoder.encode(item, StandardCharsets.UTF_8));

                }
            }
        }
        return sb.toString();
    }

    public static String createRequestUrl(String server, int restPort, String url, Map<String, Object> parameters) {
        String fullUrl = ("http://" + server + ":" + restPort + url);
        if (parameters == null || parameters.isEmpty()) {
            return fullUrl;
        }

        return (fullUrl + "?" + createQuery(parameters));

    }
}
