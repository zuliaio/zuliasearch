package io.zulia.rest.dto;

import org.bson.Document;

import java.util.Date;

public record AssociatedMetadataDTO(String uniqueId, String filename, Date uploadDate, Document meta) {
}