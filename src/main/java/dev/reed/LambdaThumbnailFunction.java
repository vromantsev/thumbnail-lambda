package dev.reed;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import dev.reed.dto.ThumbnailUploadResponse;
import net.coobird.thumbnailator.Thumbnails;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;

public class LambdaThumbnailFunction {

    private static final String TMP_DIR = "/tmp/";
    private static final String THUMBNAIL_PREFIX = TMP_DIR + "thumbnail-";
    private static final String DESTINATION_DIR = "thumbnails/";
    private static final String TARGET_BUCKET_PROPERTY = "TARGET_BUCKET";

    private final S3Client s3Client;

    {
        s3Client = S3Client.builder()
                .region(Region.EU_NORTH_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    public ThumbnailUploadResponse handleRequest(final S3Event s3Event, final Context context) {
        ThumbnailUploadResponse.ThumbnailUploadResponseBuilder builder = ThumbnailUploadResponse.builder();
        String targetBucketName = System.getProperty(TARGET_BUCKET_PROPERTY);
        LambdaLogger logger = context.getLogger();
        s3Event.getRecords().forEach(record -> {
            try {
                S3EventNotification.S3Entity s3 = record.getS3();
                String sourceBucketName = s3.getBucket().getName();
                String sourceFileKey = s3.getObject().getKey();

                File file = new File(TMP_DIR + sourceFileKey);
                logger.log("Start downloading the object: '%s' from bucket: '%s'".formatted(sourceFileKey, sourceBucketName), LogLevel.INFO);
                s3Client.getObject(
                        GetObjectRequest.builder()
                                .bucket(sourceBucketName)
                                .key(sourceFileKey)
                                .build()
                );
                logger.log("Downloaded an object: '%s' from bucket: '%s'".formatted(sourceFileKey, sourceBucketName), LogLevel.INFO);

                File thumbnail = new File(THUMBNAIL_PREFIX + sourceFileKey);
                Thumbnails.of(file).size(100, 100).toFile(thumbnail);
                logger.log("Created a thumbnail: '%s' for file: '%s'".formatted(thumbnail.getPath(), sourceFileKey));

                logger.log("Putting a thumbnail: '%s' to the target bucket: '%s'".formatted(thumbnail.getName(), targetBucketName));
                PutObjectResponse putObjectResponse = s3Client.putObject(
                        PutObjectRequest.builder()
                                .bucket(targetBucketName)
                                .key(DESTINATION_DIR + sourceFileKey)
                                .build(),
                        RequestBody.fromFile(thumbnail)
                );
                logger.log("Thumbnail  '%s' was successfully sent to the target bucket: '%s'".formatted(thumbnail.getName(), targetBucketName));

                SdkHttpResponse sdkHttpResponse = putObjectResponse.sdkHttpResponse();
                builder.fileName(sourceFileKey)
                        .status(sdkHttpResponse.statusCode())
                        .message(sdkHttpResponse.statusText().orElse(ThumbnailUploadResponse.DEFAULT_MESSAGE))
                        .targetBucket(targetBucketName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return builder.build();
    }
}
