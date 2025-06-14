DROP TABLE IF EXISTS annotations CASCADE;
DROP TABLE IF EXISTS papers CASCADE;
DROP TABLE IF EXISTS effects;
DROP TABLE IF EXISTS annotations_papers CASCADE;
CREATE TABLE papers (
    id BIGSERIAL PRIMARY KEY,
    author TEXT NOT NULL,
    publication_year BIGINT NOT NULL
);
CREATE TABLE effects (
    id BIGSERIAL PRIMARY KEY,
    detail TEXT NOT NULL,
    species TEXT
);
CREATE TABLE annotations (
    id BIGSERIAL PRIMARY KEY,
    effect_id BIGINT NOT NULL REFERENCES effects(id)
);
CREATE TABLE annotations_papers (
    id BIGSERIAL,
    annotation_id BIGINT NOT NULL REFERENCES annotations(id),
    paper_id BIGINT NOT NULL REFERENCES papers(id)
);
CREATE TABLE substitutions_annotations (
    id BIGSERIAL,
    annotation_id BIGINT NOT NULL REFERENCES annotations(id),
    amino_acid_substitution_id BIGINT NOT NULL REFERENCES amino_acid_substitutions(id)
);
