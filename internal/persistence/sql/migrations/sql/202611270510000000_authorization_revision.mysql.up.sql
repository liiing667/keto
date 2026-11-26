CREATE TABLE keto_authorization_revisions (
    nid        CHAR(36)  NOT NULL PRIMARY KEY,
    revision   BIGINT    NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
