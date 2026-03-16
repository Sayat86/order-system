CREATE TABLE orders (
                        id UUID PRIMARY KEY,
                        user_id UUID,
                        total_amount NUMERIC(19,2),
                        status VARCHAR(50),
                        created_at TIMESTAMP
);

CREATE TABLE order_items (
                             id UUID PRIMARY KEY,
                             order_id UUID,
                             product_id UUID,
                             quantity INT,
                             price NUMERIC(19,2)
);

CREATE TABLE outbox_events (
                               id UUID PRIMARY KEY,
                               aggregate_type VARCHAR(255),
                               aggregate_id UUID,
                               event_type VARCHAR(255),
                               payload JSONB,
                               created_at TIMESTAMP,
                               published BOOLEAN
);