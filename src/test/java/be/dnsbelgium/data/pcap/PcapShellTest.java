package be.dnsbelgium.data.pcap;

import be.dnsbelgium.data.pcap.configuration.ConversionServiceConfig;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.shell.Shell;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertNotNull;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {ConversionServiceConfig.class} )
@EnableAutoConfiguration(exclude = {
    org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextInstanceDataAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration.class,
    org.springframework.cloud.aws.autoconfigure.context.ContextResourceLoaderAutoConfiguration.class,

    org.springframework.boot.autoconfigure.mail.MailSenderValidatorAutoConfiguration.class,
    org.springframework.boot.autoconfigure.web.embedded.EmbeddedWebServerFactoryCustomizerAutoConfiguration.class,
    org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration.class,
    org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration.class,
    org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration.class,
    org.springframework.boot.autoconfigure.jdbc.JndiDataSourceAutoConfiguration.class,
    org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
    org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration.class,
    org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration.class,
    org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class,
    org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration.class,
    org.springframework.boot.autoconfigure.data.web.SpringDataWebAutoConfiguration.class,
    org.springframework.boot.autoconfigure.dao.PersistenceExceptionTranslationAutoConfiguration.class,
    org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration.class,
    org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration.class,
    org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration.class,
    org.springframework.boot.autoconfigure.info.ProjectInfoAutoConfiguration.class,
})
@TestPropertySource(locations = "classpath:test-application.properties")
public class PcapShellTest {

  @Autowired
  private Shell shell;
  @Autowired
  ApplicationContext context;
  private static final Logger logger = getLogger(PcapShellTest.class);

  @Test
  public void testHelp() {
    logger.info("shell = {}", shell);
    assertNotNull(shell);
    Object help = shell.evaluate(() -> "help");
    assertNotNull(help);
    logger.info("help = {}", help);
  }

  @Test
  public void listBeans() {
    logger.info("context = {}", context);
    for (String beanName : context.getBeanDefinitionNames()) {
      logger.info("beanName = {}", beanName);
    }
    logger.info("bean count = {}", context.getBeanDefinitionCount());
  }

  @Test
  @Ignore
  public void isRunningOnCloudEnvironment() {
    logger.info("Running on-cloud: {}",
        org.springframework.cloud.aws.context.support.env.AwsCloudEnvironmentCheckUtils.isRunningOnCloudEnvironment());
  }

}