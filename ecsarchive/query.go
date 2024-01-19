package ecsarchive

const QUERY = `
WITH annotationPredicate AS (
        SELECT
                content.uuid uuid,
                STRING_AGG(concept.pref_label, ',') authors
        FROM concept
                JOIN annotation ON concept.uuid=annotation.concept_uuid
                JOIN content ON annotation.content_uuid=content.uuid
        WHERE annotation.predicate = 'http://www.ft.com/ontology/annotation/hasAuthor' OR annotation.predicate = 'http://www.ft.com/ontology/hasContributor'
        GROUP BY
                content.uuid
)

SELECT
        article.uuid,
        article.body_xml,
        article.canonical_web_url,
        content.first_published_date,
        content.last_modified,
        COALESCE(annotationPredicate.authors, article.byline, '') authors
FROM article
        JOIN content ON article.uuid=content.uuid
        LEFT JOIN annotationPredicate ON annotationPredicate.uuid=content.uuid
WHERE (
                content.first_published_date<$1::date
                AND content.first_published_date>$2::date
        ) AND NOT (
                content.can_be_distributed = 'no'
        )
GROUP BY
        article.uuid,
        content.first_published_date,
        content.last_modified,
        annotationPredicate.authors;`
