package postgres

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/lib/pq"
)

// CCC planning depends only on each request's own consistency payload, so it
// runs in the caller's goroutine before the request is queued. The flush
// worker merges plans into integer arrays and prebuilt SQL; the database
// function receives no criteria to parse while holding the writer lock.

// compiledCriterion is one AND criterion rendered for both persisted lookups
// and in-batch matching. Its canonical JSON identifies it across requests.
type compiledCriterion struct {
	canonical  string
	storedSQL  string
	matchSQL   string // non-equality criteria only; evaluated over candidate documents
	shape      string // equality criteria only; canonical JSON array of sorted keys
	isEquality bool
}

type compiledCheck struct {
	criteria      []compiledCriterion
	transactionID int64
	globalID      int64
}

func compileConsistencyChecks(checks []eventstore.ConsistencyCheck) ([]compiledCheck, error) {
	compiled := make([]compiledCheck, len(checks))
	for i, check := range checks {
		c := &compiled[i]
		c.transactionID = check.Position.CommitPosition
		c.globalID = check.Position.PreparePosition
		c.criteria = make([]compiledCriterion, len(check.Criteria))
		for j, criterion := range eventstore.EncodeReadCriteria(check.Criteria) {
			var err error
			if c.criteria[j], err = compileCriterion(criterion); err != nil {
				return nil, err
			}
		}
	}
	return compiled, nil
}

func compileCriterion(fields map[string]any) (compiledCriterion, error) {
	if len(fields) == 0 {
		return compiledCriterion{}, fmt.Errorf("consistency criterion has no tags")
	}
	canonical, err := json.Marshal(fields)
	if err != nil {
		return compiledCriterion{}, err
	}
	compiled := compiledCriterion{canonical: string(canonical), isEquality: true}
	keys := make([]string, 0, len(fields))
	for key, value := range fields {
		keys = append(keys, key)
		if _, ok := value.(string); !ok {
			compiled.isEquality = false
		}
	}
	sort.Strings(keys)
	if compiled.storedSQL, err = storedRow("stored.").criterionSQL(fields); err != nil {
		return compiledCriterion{}, err
	}
	if compiled.isEquality {
		shape, err := json.Marshal(keys)
		if err != nil {
			return compiledCriterion{}, err
		}
		compiled.shape = string(shape)
	} else if compiled.matchSQL, err = candidateDocument("candidate.document").criterionSQL(fields); err != nil {
		return compiledCriterion{}, err
	}
	return compiled, nil
}

// postgresFlushPlan is the batch-wide CCC plan. Offsets are 1-based and each
// trailing sentinel closes the preceding request or check range.
type postgresFlushPlan struct {
	criterionCount        int
	requestCheckOffsets   []int32
	checkCriterionOffsets []int32
	checkCriterionIDs     []int32
	expectedTransactions  []int64
	expectedGlobals       []int64
	latestQuery           *string
	equalityCriteria      []byte // [[id, criterion], ...]; the database canonicalizes each criterion
	criterionShapes       []byte // [[key, ...], ...]
	predicateQuery        *string
}

func planPostgresFlush(table string, requests [][]compiledCheck) postgresFlushPlan {
	plan := postgresFlushPlan{requestCheckOffsets: make([]int32, 0, len(requests)+1)}
	ids := make(map[string]int32)
	var latest, predicates, equality []string
	shapes := make(map[string]bool)
	var shapeList []string
	for _, checks := range requests {
		plan.requestCheckOffsets = append(plan.requestCheckOffsets, int32(len(plan.expectedTransactions)+1))
		for _, check := range checks {
			plan.checkCriterionOffsets = append(plan.checkCriterionOffsets, int32(len(plan.checkCriterionIDs)+1))
			plan.expectedTransactions = append(plan.expectedTransactions, check.transactionID)
			plan.expectedGlobals = append(plan.expectedGlobals, check.globalID)
			for _, criterion := range check.criteria {
				id, ok := ids[criterion.canonical]
				if !ok {
					id = int32(len(ids) + 1)
					ids[criterion.canonical] = id
					idText := strconv.Itoa(int(id))
					latest = append(latest, "(SELECT "+idText+"::INT AS criterion_id, stored.transaction_id, stored.global_id FROM "+table+
						" stored WHERE "+criterion.storedSQL+" ORDER BY stored.transaction_id DESC, stored.global_id DESC LIMIT 1)")
					if criterion.isEquality {
						equality = append(equality, "["+idText+","+criterion.canonical+"]")
						if !shapes[criterion.shape] {
							shapes[criterion.shape] = true
							shapeList = append(shapeList, criterion.shape)
						}
					} else {
						predicates = append(predicates, "CASE WHEN "+criterion.matchSQL+" THEN "+idText+" END")
					}
				}
				plan.checkCriterionIDs = append(plan.checkCriterionIDs, id)
			}
		}
	}
	plan.requestCheckOffsets = append(plan.requestCheckOffsets, int32(len(plan.expectedTransactions)+1))
	plan.checkCriterionOffsets = append(plan.checkCriterionOffsets, int32(len(plan.checkCriterionIDs)+1))
	plan.criterionCount = len(ids)
	plan.equalityCriteria = []byte("[" + strings.Join(equality, ",") + "]")
	plan.criterionShapes = []byte("[" + strings.Join(shapeList, ",") + "]")
	if len(latest) > 0 {
		query := strings.Join(latest, " UNION ALL ")
		plan.latestQuery = &query
	}
	if len(predicates) > 0 {
		// Plan operator matching once per accepted request, rather than once
		// per event. Every event participates: a criterion may match an
		// intermediate event but not the last event in the request.
		query := `SELECT matched.id, MAX(candidate.global_id) AS global_id
FROM unnest($1::JSONB[], $2::BIGINT[]) AS candidate(document, global_id)
CROSS JOIN LATERAL unnest(ARRAY[` + strings.Join(predicates, ",") + `]::INT[]) AS matched(id)
WHERE matched.id IS NOT NULL
GROUP BY matched.id`
		plan.predicateQuery = &query
	}
	return plan
}

func postgresEventTable(schema, boundary string) string {
	return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_es_event")
}
