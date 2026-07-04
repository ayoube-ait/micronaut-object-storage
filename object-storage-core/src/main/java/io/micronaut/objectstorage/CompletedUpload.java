/*
 * Copyright 2017-2026 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.objectstorage;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Finalized information about a completed object upload.
 *
 * @param key The object key.
 * @param eTag The final entity tag.
 * @param contentLength The final content length.
 * @param contentType The content type associated with the stored object, or {@code null} when unknown. Depending on the
 * provider and request type, this can be explicitly declared, inferred from the key as a convenience, or returned by
 * the provider. It does not represent validation of the object bytes.
 * @param storageTimestamp The provider's storage timestamp. This is not an application or database timestamp.
 * @param metadata User metadata.
 * @param attributes Portable custom attributes.
 * @param nativeResponse The optional provider-native upload response.
 * @param <O> The provider-native upload response type.
 * @since 3.1.0
 */
public record CompletedUpload<O>(
    @NonNull String key,
    @NonNull String eTag,
    long contentLength,
    @Nullable String contentType,
    @NonNull Instant storageTimestamp,
    @NonNull Map<String, String> metadata,
    @NonNull Map<String, String> attributes,
    @NonNull Optional<O> nativeResponse
) {
    /**
     * Creates a completed upload and protects its metadata maps from subsequent mutation.
     */
    public CompletedUpload {
        key = Objects.requireNonNull(key, "key");
        eTag = Objects.requireNonNull(eTag, "eTag");
        storageTimestamp = Objects.requireNonNull(storageTimestamp, "storageTimestamp");
        metadata = Map.copyOf(Objects.requireNonNull(metadata, "metadata"));
        attributes = Map.copyOf(Objects.requireNonNull(attributes, "attributes"));
        nativeResponse = Objects.requireNonNull(nativeResponse, "nativeResponse");
    }
}
