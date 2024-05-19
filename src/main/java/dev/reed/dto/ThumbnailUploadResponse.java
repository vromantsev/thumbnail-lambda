package dev.reed.dto;

import lombok.Builder;

@Builder
public record ThumbnailUploadResponse(String fileName, String targetBucket, int status, String message) {

    public static final String DEFAULT_MESSAGE = "Thumbnail upload is finished.";
}
