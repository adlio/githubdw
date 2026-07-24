-- githubdw migration 002: local user/repo grouping tables + PR labels

-- PR label support (GitHub also allows labels on PRs).
CREATE TABLE pr_labels (
    pr_key   TEXT NOT NULL REFERENCES fact_pull_requests(pr_key) ON DELETE CASCADE,
    label_id TEXT NOT NULL REFERENCES labels(id),
    PRIMARY KEY (pr_key, label_id)
);
CREATE INDEX idx_pr_labels_label ON pr_labels(label_id);

CREATE TABLE user_group (
    name        TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE user_group_member (
    group_name TEXT NOT NULL REFERENCES user_group(name) ON DELETE CASCADE,
    login      TEXT NOT NULL,
    PRIMARY KEY (group_name, login)
);

CREATE TABLE repo_group (
    name        TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE repo_group_member (
    group_name TEXT NOT NULL REFERENCES repo_group(name) ON DELETE CASCADE,
    repo       TEXT NOT NULL,
    PRIMARY KEY (group_name, repo)
);
