package be.dnsbelgium.data.pcap.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration(exclude = {
    org.springframework.cloud.aws.autoconfigure.context.ContextInstanceDataAutoConfiguration.class,
})
@SpringBootTest
@IfProfileValue(name="test-groups", value= "aws-integration-tests")
@TestPropertySource(locations = "classpath:test-application.properties")
public class TaggerTest {

  /*
   * This test will attempt to read and modify tags for some files from S3.
   * Make sure you have these files in the configure bucket (see pcap.bucket.name in test-application.properties)
   *
   * S3://<TestBucketName>/taggertest.dns.be/05-05-2018/7777_taggertest.dns.be.eth0.pcap001_DONE.gz
   */

  private static final Logger logger = getLogger(TaggerTest.class);

  private static final String OBJECT_KEY = "tagging.test.txt";
  private static final String PCAP_OBJECT_KEY = "taggertest.dns.be/05-05-2018/7777_taggertest.dns.be.eth0.pcap001_DONE.gz";

  @Autowired
  private Tagger tagger;

  @Autowired
  private Downloader downloader;

  @SuppressWarnings({"unused", "SpringJavaInjectionPointsAutowiringInspection"})
  @Autowired
  private AmazonS3 amazonS3;

  @Value("${pcap.bucket.name}")
  private String bucket_name;

  @Before
  public void before() {
    assertNotNull(tagger);
    assertNotNull(amazonS3);
    assertNotNull(downloader);
    amazonS3.putObject(bucket_name, OBJECT_KEY, "some content");
    amazonS3.putObject(bucket_name, PCAP_OBJECT_KEY, "some content");
    logger.debug("S3 Objects created with no tags");
  }

  @After
  public void after() {
    try {
      amazonS3.deleteObject(bucket_name, OBJECT_KEY);
      amazonS3.deleteObject(bucket_name, PCAP_OBJECT_KEY);
    } catch (Exception e) {
      logger.info("Failed to delete S3 object", e);
    }
  }

  @Test
  public void addTag() {
    Map<String, String> tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertTrue("initially object should have no tags", tags.isEmpty());
    tagger.setTag(bucket_name, OBJECT_KEY, "SOME_TAG", "SOME VALUE");
    tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertEquals(1, tags.size());
    assertEquals("SOME VALUE", tags.get("SOME_TAG"));
  }

  @Test
  public void removeTag() {
    Map<String, String> tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertTrue("initially object should have no tags", tags.isEmpty());

    tagger.setTag(bucket_name, OBJECT_KEY, "SOME_TAG", "SOME VALUE");
    tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertEquals(1, tags.size());
    assertEquals("SOME VALUE", tags.get("SOME_TAG"));

    tagger.removeTag(bucket_name, OBJECT_KEY, "SOME_TAG");
    tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertEquals(0, tags.size());
  }


  @Test
  public void addTags() {
    Map<String, String> tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertTrue("initially object should have no tags", tags.isEmpty());

    Map<String, String> tags1 = new HashMap<>();
    tags1.put("KEY1", "VALUE 1");
    tags1.put("KEY2", "VALUE 2");
    tags1.put("KEY3", "VALUE 3");
    tagger.addTags(bucket_name, OBJECT_KEY, tags1);

    tags = tagger.getTags(bucket_name, OBJECT_KEY);
    assertEquals(tags1, tags);

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("KEY2", "VALUE UPDATED");
    tags2.put("KEY3", "");
    tags2.put("KEY4", "VALUE 4");

    Map<String, String> newTags = tagger.addTags(bucket_name, OBJECT_KEY, tags2);

    assertEquals(4, newTags.size());
    assertEquals("untouched tag should be unchanged", "VALUE 1", newTags.get("KEY1"));
    assertEquals("updated tag should be updated", "VALUE UPDATED", newTags.get("KEY2"));
    assertEquals("emptied tag should be present", "", newTags.get("KEY3"));
    assertEquals("added tag should be present", "VALUE 4", newTags.get("KEY4"));
  }

  @Test
  public void addTagsOnPcapFile() {
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName(bucket_name);
    summary.setKey(PCAP_OBJECT_KEY);
    S3PcapFile pcapFile = S3PcapFile.parse(summary);
    assertNotNull(pcapFile);

    Map<String, String> tags = tagger.getTags(pcapFile);
    assertTrue("initially object should have no tags", tags.isEmpty());

    Map<String, String> tags1 = new HashMap<>();
    tags1.put("KEY1", "VALUE 1");
    tags1.put("KEY2", "VALUE 2");
    tags1.put("KEY3", "VALUE 3");
    tags = tagger.addTags(pcapFile, tags1);

    assertEquals(tags1, tags);

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("KEY2", "VALUE UPDATED");
    tags2.put("KEY3", "");
    tags2.put("KEY4", "VALUE 4");

    Map<String, String> newTags = tagger.addTags(pcapFile, tags2);

    assertEquals(4, newTags.size());
    assertEquals("untouched tag should be unchanged", "VALUE 1", newTags.get("KEY1"));
    assertEquals("updated tag should be updated", "VALUE UPDATED", newTags.get("KEY2"));
    assertEquals("emptied tag should be present", "", newTags.get("KEY3"));
    assertEquals("added tag should be present", "VALUE 4", newTags.get("KEY4"));

  }


}