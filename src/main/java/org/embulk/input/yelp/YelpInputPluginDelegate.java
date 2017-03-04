package org.embulk.input.yelp;

import java.util.List;
import java.util.Locale;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.collect.Ordering;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.jackson.JacksonJsonPointerValueLocator;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.request.RetryHelper;
import org.embulk.base.restclient.request.SingleRequester;
import org.embulk.base.restclient.request.StringResponseEntityReader;

import org.slf4j.Logger;

public class YelpInputPluginDelegate
        implements RestClientInputPluginDelegate<YelpInputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends RestClientInputTaskBase
    {
        @Config("access_token")
        public String getAccessToken();

        @Config("location")
        public String getLocation();

        @Config("maximum_retries")
        @ConfigDefault("7")
        public int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        public int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("60000")
        public int getMaximumRetryIntervalMillis();
    }

    private final StringJsonParser jsonParser = new StringJsonParser();

    @Override  // Overridden from |TaskValidatable|
    public void validateTask(PluginTask task)
    {
    }

    @Override  // Overridden from |ServiceResponseMapperBuildable|
    public JacksonServiceResponseMapper buildServiceResponseMapper(PluginTask task)
    {
        return JacksonServiceResponseMapper.builder()
            .add("rating", Types.LONG)
            .add("price", Types.STRING)
            .add("phone", Types.STRING)
            .add("id", Types.STRING)
            .add("is_closed", Types.BOOLEAN)
            .add("categories", Types.JSON)
            .add("review_count", Types.LONG)
            .add("name", Types.STRING)
            .add("url", Types.STRING)
            .add(new JacksonJsonPointerValueLocator("/coordinates/latitude"), "latitude", Types.STRING)
            .add(new JacksonJsonPointerValueLocator("/coordinates/longitude"), "longitude", Types.STRING)
            .add("image_url", Types.STRING)
            .add(new JacksonJsonPointerValueLocator("/location/city"), "location_city", Types.STRING)
            .add(new JacksonJsonPointerValueLocator("/location/country"), "location_country", Types.STRING)
            .build();
    }

    @Override  // Overridden from |ConfigDiffBuildable|
    public ConfigDiff buildConfigDiff(PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        return Exec.newConfigDiff();
    }

    @Override  // Overridden from |ServiceDataIngestable|
    public TaskReport ingestServiceData(final PluginTask task,
                                        RetryHelper retryHelper,
                                        RecordImporter recordImporter,
                                        int taskIndex,
                                        PageBuilder pageBuilder)
    {
        TaskReport report = Exec.newTaskReport();
        for (int offset = 0; offset < 1000; ) {
            final int limit = Exec.isPreview() ? 5 : (Ordering.natural().min(50, 1000 - offset));
            String content = fetchFromYelp(retryHelper,
                                           task.getAccessToken(),
                                           task.getLocation(),
                                           limit,
                                           offset);
            ArrayNode records = extractArrayField(content);

            for (JsonNode record : records) {
                if (!record.isObject()) {
                    logger.warn(
                        String.format(Locale.ENGLISH,
                                      "Record is not a JSON object: %s",
                                      record.toString()));
                    continue;
                }
                try {
                    recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) record),
                                                pageBuilder);
                }
                catch (Exception ex) {
                    logger.warn(
                        String.format(Locale.ENGLISH,
                                      "Record is skipped due to Exception: %s",
                                      record.toString()), ex);
                }
            }

            if (Exec.isPreview() || records.size() <= 0) {
                break;
            }
            offset += records.size();
        }

        return report;
    }

    private ArrayNode extractArrayField(String content)
    {
        ObjectNode object = jsonParser.parseJsonObject(content);
        JsonNode businesses = object.get("businesses");
        if (businesses.isArray()) {
            return (ArrayNode) businesses;
        }
        else {
            throw new DataException("Content is in an unexpected format: " + content);
        }
    }

    private String fetchFromYelp(RetryHelper retryHelper,
                                 final String bearerToken,
                                 final String location,
                                 final int limit,
                                 final int offset)
    {
        return retryHelper.requestWithRetry(
            new StringResponseEntityReader(),
            new SingleRequester() {
                @Override
                public Response requestOnce(javax.ws.rs.client.Client client)
                {
                    Response response = client
                        .target("https://api.yelp.com/v3/businesses/search")
                        .queryParam("location", location)
                        .queryParam("limit", String.valueOf(limit))
                        .queryParam("offset", String.valueOf(offset))
                        .request()
                        .header("Authorization", "Bearer " + bearerToken)
                        .get();
                    return response;
                }

                @Override
                public boolean isResponseStatusToRetry(javax.ws.rs.core.Response response)
                {
                    int status = response.getStatus();
                    if (status == 429) {
                        return true;  // Retry if 429.
                    }
                    return status / 100 != 4;  // Retry unless 4xx except for 429.
                }
            });
    }

    @Override  // Overridden from |ClientCreatable|
    public javax.ws.rs.client.Client createClient(PluginTask task)
    {
        // TODO(dmikurube): Configure org.glassfish.jersey.client.ClientProperties.CONNECT_TIMEOUT and READ_TIMEOUT.
        return javax.ws.rs.client.ClientBuilder.newBuilder().build();
    }

    @Override  // Overridden from |RetryConfigurable|
    public int configureMaximumRetries(PluginTask task)
    {
        return task.getMaximumRetries();
    }

    @Override  // Overridden from |RetryConfigurable|
    public int configureInitialRetryIntervalMillis(PluginTask task)
    {
        return task.getInitialRetryIntervalMillis();
    }

    @Override  // Overridden from |RetryConfigurable|
    public int configureMaximumRetryIntervalMillis(PluginTask task)
    {
        return task.getMaximumRetryIntervalMillis();
    }

    private final Logger logger = Exec.getLogger(YelpInputPluginDelegate.class);
}
