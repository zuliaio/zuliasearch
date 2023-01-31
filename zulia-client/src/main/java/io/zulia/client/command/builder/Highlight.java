package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

public class Highlight implements HighlightBuilder {

    private ZuliaQuery.HighlightRequest.Builder highlightBuilder = ZuliaQuery.HighlightRequest.newBuilder();

    public Highlight(String field) {
        highlightBuilder.setField(field);
    }

    public int getNumberOfFragments() {
        return highlightBuilder.getNumberOfFragments();
    }

    public Highlight setNumberOfFragments(int numberOfFragments) {
        highlightBuilder.setNumberOfFragments(numberOfFragments);
        return this;
    }

    public String getPreTag() {
        return highlightBuilder.getPreTag();
    }

    public Highlight setPreTag(String preTag) {
        highlightBuilder.setPreTag(preTag);
        return this;
    }

    public String getPostTag() {
        return highlightBuilder.getPostTag();
    }

    public Highlight setPostTag(String postTag) {
        highlightBuilder.setPostTag(postTag);
        return this;
    }

    @Override
    public ZuliaQuery.HighlightRequest getHighlight() {
        return highlightBuilder.build();
    }
}
