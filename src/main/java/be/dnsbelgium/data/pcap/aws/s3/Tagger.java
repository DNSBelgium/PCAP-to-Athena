/*
 * This file is part of PCAP to Athena.
 *
 * Copyright (c) 2019 DNS Belgium.
 *
 * PCAP to Athena is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PCAP to Athena is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PCAP to Athena.  If not, see <https://www.gnu.org/licenses/>.
 */

package be.dnsbelgium.data.pcap.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class Tagger {

  private final AmazonS3 amazonS3;

  private static final Logger logger = getLogger(Tagger.class);

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  public Tagger(AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;
    logger.info("Current AWS region = {}", amazonS3.getRegionName());
    logger.info("Current AWS account = {}", amazonS3.getS3AccountOwner().getDisplayName());
  }

  public Map<String, String> getTags(S3PcapFile file) {
    return getTags(file.getObjectSummary().getBucketName(), file.getObjectSummary().getKey());
  }

  public Map<String, String> getTags(String bucket, String key) {
    logger.debug("gettings tags for {} in bucket {}", key, bucket);
    GetObjectTaggingRequest getRequest = new GetObjectTaggingRequest(bucket, key);
    GetObjectTaggingResult taggingResult = amazonS3.getObjectTagging(getRequest);

    return taggingResult.getTagSet().stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));
  }

  /**
   * Adds the specified tags to the specified S3 object. If the object already has some of these tags, their value will be overwritten.
   * Please note that you cannot remove tags with this method, but you can set the value of a tag to the empty string.
   *
   * @param bucket  the name of the S3 bucket
   * @param key     the key of the S3 object
   * @param newTags the tags to add
   * @return the new map of tags for the S3 object (including the one that were already present)
   */
  public Map<String, String> addTags(String bucket, String key, Map<String, String> newTags) {
    applyToTag(bucket, key, tags -> tags.putAll(newTags));
    return getTags(bucket, key);
  }

  public Map<String, String> addTags(S3PcapFile file, Map<String, String> newTags) {
    return addTags(file.getObjectSummary().getBucketName(), file.getObjectSummary().getKey(), newTags);
  }

  public void setTag(String bucket, String key, String tagName, String tagValue) {
    applyToTag(bucket, key, tags -> tags.put(tagName, tagValue));
    logger.info("adding tag {}:{} to {} in {}", tagName, tagValue, key, bucket);
  }

  public void removeTag(String bucket, String key, String tagName) {
    applyToTag(bucket, key, tags -> tags.remove(tagName));
    logger.info("Removing tag {} from {} in {}", tagName, key, bucket);
  }

  private void applyToTag(String bucket, String key, Consumer<Map<String, String>> tagsFunction) {
    Map<String, String> tags = getTags(bucket, key);
    tagsFunction.accept(tags);
    SetObjectTaggingRequest request = new SetObjectTaggingRequest(bucket, key, convert(tags));
    amazonS3.setObjectTagging(request);
  }

  private ObjectTagging convert(Map<String, String> map) {
    return new ObjectTagging(map.entrySet().stream().map(entry -> new Tag(entry.getKey(), entry.getValue())).collect(Collectors.toList()));
  }

}
