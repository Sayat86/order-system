CREATE TABLE saga_state (
                            saga_id UUID PRIMARY KEY,
                            order_id UUID NOT NULL,
                            current_step VARCHAR(100),
                            status VARCHAR(50),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);