package org.embulk.input.yelp;

import java.util.List;
import java.util.Locale;

import javax.ws.rs.core.Response;
import javax.ws.rs.client.WebTarget;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import org.embulk.base.restclient.DefaultServiceDataSplitter;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.base.restclient.jackson.JacksonJsonPointerValueLocator;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.base.restclient.record.RecordImporter;

import org.embulk.util.retryhelper.jaxrs.JAXRSClientCreator;
import org.embulk.util.retryhelper.jaxrs.JAXRSRetryHelper;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.embulk.util.retryhelper.jaxrs.StringJAXRSResponseEntityReader;

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
        @ConfigDefault("null")
        public Optional<String> getLocation();

        @Config("latitude")
        @ConfigDefault("null")
        public Optional<String> getLatitude();

        @Config("longitude")
        @ConfigDefault("null")
        public Optional<String> getLongitude();

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

    @Override  // Overridden from |InputTaskValidatable|
    public void validateInputTask(PluginTask task)
    {
        if (!task.getLocation().isPresent() &&
            !(task.getLatitude().isPresent() && task.getLongitude().isPresent())) {
            throw new ConfigException("'location' or 'latitude'/'longitude' are required.");
        }
    }

    @Override  // Overridden from |InputTaskValidatable|
    public ServiceDataSplitter buildServiceDataSplitter(PluginTask task)
    {
        return new DefaultServiceDataSplitter();
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
                                        RecordImporter recordImporter,
                                        int taskIndex,
                                        PageBuilder pageBuilder)
    {
        TaskReport report = Exec.newTaskReport();
        for (int offset = 0; offset < 1000; ) {
            final int limit = Exec.isPreview() ? 5 : (Ordering.natural().min(50, 1000 - offset));
            final String content;
            try (JAXRSRetryHelper retryHelper = new JAXRSRetryHelper(
                     task.getMaximumRetries(),
                     task.getInitialRetryIntervalMillis(),
                     task.getMaximumRetryIntervalMillis(),
                     new JAXRSClientCreator() {
                         @Override
                         public javax.ws.rs.client.Client create() {
                             return javax.ws.rs.client.ClientBuilder.newBuilder().build();
                         }
                     })) {
                content = fetchFromYelp(retryHelper,
                                        task.getAccessToken(),
                                        task.getLocation(),
                                        task.getLatitude(),
                                        task.getLongitude(),
                                        limit,
                                        offset);
            }
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

    private String fetchFromYelp(JAXRSRetryHelper retryHelper,
                                 final String bearerToken,
                                 final Optional<String> location,
                                 final Optional<String> latitude,
                                 final Optional<String> longitude,
                                 final int limit,
                                 final int offset)
    {
        return retryHelper.requestWithRetry(
            new StringJAXRSResponseEntityReader(),
            new JAXRSSingleRequester() {
                @Override
                public Response requestOnce(javax.ws.rs.client.Client client)
                {
                    WebTarget webTarget = client
                        .target("https://api.yelp.com/v3/businesses/search")
                        .queryParam("limit", String.valueOf(limit))
                        .queryParam("offset", String.valueOf(offset));
                    if (location.isPresent()) {
                        webTarget = webTarget.queryParam("location", location.get());
                    } else if (latitude.isPresent() && longitude.isPresent()) {
                        webTarget = webTarget.queryParam("latitude", latitude.get())
                                             .queryParam("longitude", longitude.get());
                    } else {
                        throw new ConfigException(
                            "FATAL: 'location' or 'latitude'/'longitude' are required.");
                    }
                    Response response = webTarget
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

    private final Logger logger = Exec.getLogger(YelpInputPluginDelegate.class);
}
