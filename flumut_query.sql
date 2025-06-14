SELECT mutation_name,
    mm.marker_id
FROM markers_mutations mm
INNER JOIN markers_effects ON markers_effects.marker_id = mm.marker_id
LEFT JOIN papers ON papers.id = markers_effects.paper_id
WHERE subtype = 'H5N1'
GROUP BY mutation_name, mm.marker_id
ORDER BY mm.marker_id
LIMIT 10
;
