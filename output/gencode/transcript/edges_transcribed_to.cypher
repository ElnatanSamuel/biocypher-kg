
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:////mnt/d/2024/icogbiocy/biocypher-kg/output/gencode/transcript/edges_transcribed_to.csv' AS row FIELDTERMINATOR '|' RETURN row",
    "MATCH (source:row.source_type {id: row.source_id})
    MATCH (target:row.target_type {id: row.target_id})
    MERGE (source)-[r:transcribed_to]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {batchSize:1000}
)
YIELD batches, total
RETURN batches, total;
                