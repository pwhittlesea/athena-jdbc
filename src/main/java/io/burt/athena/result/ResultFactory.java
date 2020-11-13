package io.burt.athena.result;

import java.time.Duration;

import io.burt.athena.configuration.ConnectionConfiguration;
import software.amazon.awssdk.services.athena.model.QueryExecution;

public class ResultFactory {

    private final ConnectionConfiguration configuration;

    public ResultFactory(ConnectionConfiguration configuration) {
        this.configuration = configuration;
    }

    public Result createResult(QueryExecution queryExecution) {
        switch (configuration.resultLoadingStrategy()) {
            case GET_EXECUTION_RESULTS:
                return new PreloadingStandardResult(configuration.athenaClient(), queryExecution, StandardResult.MAX_FETCH_SIZE, Duration.ofSeconds(10));
            case S3:
                return new S3Result(configuration.s3Client(), queryExecution, Duration.ofSeconds(10));
            default:
                throw new IllegalStateException(String.format("No such result loading strategy: %s", queryExecution));
        }
    }
}
