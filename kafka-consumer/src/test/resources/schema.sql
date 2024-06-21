CREATE TABLE IF NOT EXISTS clients
(
    id        BIGSERIAL NOT NULL PRIMARY KEY,
    client_id BIGINT UNIQUE,
    email     VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS transactions
(
    id         BIGSERIAL NOT NULL PRIMARY KEY,
    bank       VARCHAR(255),
    client_id  BIGINT,
    order_type VARCHAR(255),
    quantity   INTEGER,
    price      DOUBLE PRECISION,
    cost       DOUBLE PRECISION,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    constraint client_id_fk foreign key (client_id) REFERENCES clients (id)
);