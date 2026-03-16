CREATE TABLE processed_events (
                                  event_id UUID PRIMARY KEY,
                                  consumer_name VARCHAR(255) NOT NULL,
                                  processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE idempotency_keys (
                                  id UUID PRIMARY KEY,
                                  idempotency_key VARCHAR(255) NOT NULL UNIQUE,
                                  request_hash VARCHAR(255),
                                  response_payload JSONB,
                                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);