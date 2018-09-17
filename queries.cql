USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:/authordetails.csv" AS row
CREATE (:Author {id: row.id, name: row.full_name});

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:/articles.csv" AS row
CREATE (:Article {id: toInteger(row.abs_id), published: date(row.published), cited_by: toInteger(row.cited_by), keywords: split(row.keywords, ' | ')});

CREATE C ON :Author(id);
CREATE INDEX ON :Article(id);

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:/network.csv" AS row
CREATE (:Article {id: toInteger(row.abs_id), published: date(row.published), cited_by: toInteger(row.cited_by), keywords: split(row.keywords, ' | ')});

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:/network2.csv" AS row FIELDTERMINATOR ';'
MATCH (from_author:Author {id: row.Source})
MATCH (to_author:Author {id: row.Target})
CREATE (from_author)-[:COAUTHORED { article: row.article, year: row.year, citations: row.citations }]->(to_author);


MATCH (r1:Author)-[:COAUTHORED]->(:Author)<-[:COAUTHORED]-(:Author)<-[:COAUTHORED]-(r2:Author)
WITH r1, r2, COUNT(*) AS count
CREATE (r2)-[r:COAUTHORED]->(r1)
SET r.count = count

MATCH (r1:Author)-[:COAUTHORED]->(r2:Author)
WITH r1, r2, COUNT(*) AS count
CREATE (r2)-[r:COAUTHOR]->(r1)
SET r.count = count

MATCH p=(seed:Author)-[:AUTHORED]->(ar:Article)<-[:AUTHORED]->(coauthors)
WHERE seed.id=7006052153
RETURN count(coauthors)

MATCH (seed:Author {id:$id})-[:AUTHORED]->(ar:Article)<-[:AUTHORED]-(co)
MERGE (seed)-[r:COAUTHOR]->(co)
RETURN id(r)