SELECT mutation_name,
    papers.*,
    effect_name
FROM markers_mutations
INNER JOIN markers_effects ON markers_effects.marker_id = markers_mutations.marker_id
LEFT JOIN papers ON papers.id = markers_effects.paper_id
;
