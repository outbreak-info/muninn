DROP TABLE IF EXISTS annotations CASCADE;
DROP TABLE IF EXISTS papers CASCADE;
DROP TABLE IF EXISTS effects;
DROP TABLE IF EXISTS annotations_papers CASCADE;
CREATE TABLE papers (
    id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    publication_year BIGINT NOT NULL
);
CREATE TABLE effects (
    id BIGINT PRIMARY KEY,
    detail TEXT NOT NULL,
    species TEXT
);
CREATE TABLE annotations (
    id BIGINT PRIMARY KEY,
    amino_acid_substitution_id BIGINT NOT NULL REFERENCES amino_acid_substitutions(id),
    paper_id BIGINT NOT NULL REFERENCES papers(id),
    effect_id BIGINT NOT NULL REFERENCES effects(id),
    confidence FLOAT NOT NULL
);
CREATE TABLE annotations_papers (
    id BIGINT PRIMARY KEY,
    annotation_id BIGINT NOT NULL REFERENCES annotations(id),
    paper_id BIGINT NOT NULL REFERENCES papers(id)
);
