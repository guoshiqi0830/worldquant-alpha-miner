create table alpha
(
    id integer PRIMARY KEY AUTOINCREMENT,
    alpha_id varchar(10),
    expression text,
    `type` varchar(20),
    instrument_type varchar(20),
    region varchar(10),
    universe varchar(10),
    delay integer,
    decay integer,
    neutralization varchar(20),
    truncation decimal,
    pasteurization varchar(10),
    unit_handling varchar(10),
    nan_handling varchar(10),
    max_trade varchar(10),
    `language` varchar(10),
    visualization boolean,
    status varchar(10),
    sharpe float,
    fitness float,
    turnover float,
    concentrated_weight float,
    sub_universe_sharpe float,
    self_correlation float,
    drawdown float,
    long_count float,
    short_count float,
    returns float,
    margin float,
    pnl float,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_alpha_timestamp
AFTER UPDATE ON alpha
FOR EACH ROW
BEGIN
    UPDATE alpha SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END;

create table data_field
(
    id integer PRIMARY KEY AUTOINCREMENT,
    field_name varchar(100),
    description text,
    dataset_id varchar(10),
    dataset_name varchar(100),
    category_id varchar(10),
    category_name varchar(100),
    subcategory_id varchar(10),
    subcategory_name varchar(100),
    region varchar(10),
    delay integer,
    universe varchar(20),
    type varchar(20),
    coverage decimal,
    user_count integer,
    alpha_count integer,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_data_field_timestamp
AFTER UPDATE ON data_field
FOR EACH ROW
BEGIN
    UPDATE data_field SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END;

create table alpha_queue
(
    id integer PRIMARY KEY AUTOINCREMENT,
    template_id integer,
    template text,
    params json,
    regular text,
    settings json,
    type varchar(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_alpha_queue_timestamp
AFTER UPDATE ON alpha_queue
FOR EACH ROW
BEGIN
    UPDATE alpha_queue SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END;

create table alpha_template
(
    id integer PRIMARY KEY AUTOINCREMENT,
    alpha_id varchar(10),
    template text,
    params json,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_alpha_template_timestamp
AFTER UPDATE ON alpha_template
FOR EACH ROW
BEGIN
    UPDATE alpha_template SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END;
