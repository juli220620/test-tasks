package by.juli220620;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CrptApi {
    private static final String REQUEST_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final RateLimiter rateLimiter;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        rateLimiter = new RateLimiter(timeUnit, requestLimit);
    }

    public static void main(String[] args) {
        CrptApi crptApi = new CrptApi(TimeUnit.MINUTES, 10);

        ExecutorService service = Executors.newFixedThreadPool(5);

        var product = new Product("string", LocalDate.of(2020, 1, 23),
                "string", "string", "string",
                LocalDate.of(2020, 1, 23),
                "string", "string", "string");

        var document = new Document(
                new Description("string"),
                "string", "string", true,
                "string", "string", "string",
                LocalDate.of(2020, 1, 23),
                "string", List.of(product),
                LocalDate.of(2020, 1, 23),
                "string"
        );

        for (int i = 0; i < 20; i++) {
            service.submit(() -> {
                crptApi.createDocument(document, "string");
                return "OK";
            });
        }

        service.shutdown();
    }

    public synchronized void createDocument(Document document, String signature) {
        long currentTime = System.currentTimeMillis();

        try {
            while (rateLimiter.doesExceedLimit(currentTime)) {
                //noinspection BusyWait
                Thread.sleep(rateLimiter.sleepAmountInMillis);
                currentTime = System.currentTimeMillis();
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(REQUEST_URL);
            post.setEntity(new StringEntity(convertToJsonString(document)));
            post.setHeader("Content-type", "application/json");
            attachSignatureToRequest(post, signature);

            client.execute(post);

            rateLimiter.lastPostTime = currentTime;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private String convertToJsonString(Document document) {
        try {
            return createObjectMapper().writeValueAsString(document);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private ObjectMapper createObjectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    private void attachSignatureToRequest(HttpPost post, String signature) {
        post.setHeader("signature", signature);
        // Я не смогла найти в задании ничего о подписи, но предполагаю,
        // что каким-то образом ее нужно передать вместе с запросом. Например,
        // положить ее в хедеры запроса.
    }

    public static class RateLimiter {

        private final double maxRateInMillis;
        private final long sleepAmountInMillis;

        private long lastPostTime;

        public RateLimiter(TimeUnit timeUnit, int requestLimit) {
            maxRateInMillis = (double) requestLimit / TimeUnit.MILLISECONDS.convert(1, timeUnit);
            sleepAmountInMillis = Math.round(
                    (float) TimeUnit.MILLISECONDS.convert(1, timeUnit) / requestLimit
            );
        }

        public boolean doesExceedLimit(long currentTime) {
            return calculateActualRate(currentTime, lastPostTime) > maxRateInMillis;
        }

        public double calculateActualRate(long currentTime, long lastPostTime) {
            return 1. / (currentTime - lastPostTime);
        }
    }

    @Getter
    @AllArgsConstructor
    public static class Document {
        private final Description description;
        @JsonProperty("doc_id")
        private final String docId;
        @JsonProperty("doc_status")
        private final String docStatus;
        @JsonProperty("doc_type")
        private final String docType = "LP_INTRODUCE_GOODS";
        private final boolean importRequest;
        @JsonProperty("owner_inn")
        private final String ownerInn;
        @JsonProperty("participant_inn")
        private final String participantInn;
        @JsonProperty("producer_inn")
        private final String producerInn;
        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private final LocalDate productionDate;
        @JsonProperty("production_type")
        private final String productionType;
        private final List<Product> products;
        @JsonProperty("reg_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private final LocalDate regDate;
        @JsonProperty("reg_number")
        private final String regNumber;
    }

    @Getter
    @AllArgsConstructor
    public static class Description {
        private String participantInn;
    }

    @Getter
    @AllArgsConstructor
    private static class Product {
        @JsonProperty("certificate_document")
        private final String certificateDocument;
        @JsonProperty("certificate_document_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private final LocalDate certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        private final String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        private final String ownerInn;
        @JsonProperty("producer_inn")
        private final String producerInn;
        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private final LocalDate productionDate;
        @JsonProperty("tnved_code")
        private final String tnvedCode;
        @JsonProperty("uit_code")
        private final String uitCode;
        @JsonProperty("uitu_code")
        private final String uituCode;
    }
}
