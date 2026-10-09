package sqlite

import (
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"reflect"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// The latest position of an OR query is the maximum of its AND criteria's
// latest positions. Separate ordered lookups let every criterion use its own
// (possibly partial) index and stop after one match, without sorting history.
// All lookups run on the writer transaction, including earlier accepted saves.
func latestCriteriaPosition(conn *sqlite.Conn, criteria []orisun.ReadCriterion) (int64, int64, error) {
	queries, err := latestCriteriaQueries(criteria)
	if err != nil {
		return 0, 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid consistency criteria: %v", err)
	}
	latestTx, latestGid := int64(-1), int64(-1)
	for _, query := range queries {
		if err := sqlitex.ExecuteTransient(conn, query, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				tx, gid := stmt.ColumnInt64(0), stmt.ColumnInt64(1)
				if tx > latestTx || (tx == latestTx && gid > latestGid) {
					latestTx, latestGid = tx, gid
				}
				return nil
			},
		}); err != nil {
			return 0, 0, statuscode.Errorf(statuscode.Internal, "ccc check: %v", err)
		}
	}
	return latestTx, latestGid, nil
}

type criterionLookup struct {
	criterion map[string]any
	where     string
}

func latestCriteriaQueries(criteria []orisun.ReadCriterion) ([]string, error) {
	var lookups []criterionLookup
	for _, criterion := range readCriteriaAsList(criteria) {
		// Empty AND objects have always been ignored among nonempty criteria.
		if len(criterion) == 0 {
			continue
		}
		// Render and validate every original criterion before simplification.
		// A redundant clause must not hide an invalid request.
		where, err := buildCriteriaSQL([]map[string]any{criterion})
		if err != nil {
			return nil, err
		}
		lookups = append(lookups, criterionLookup{criterion: criterion, where: where})
	}
	var queries []string
	for _, lookup := range minimalCriteria(lookups) {
		queries = append(queries, "SELECT transaction_id, global_id FROM orisun_es_event WHERE "+lookup.where+" ORDER BY transaction_id DESC, global_id DESC LIMIT 1")
	}
	if len(queries) == 0 {
		queries = append(queries, "SELECT transaction_id, global_id FROM orisun_es_event ORDER BY transaction_id DESC, global_id DESC LIMIT 1")
	}
	return queries, nil
}

// Remove redundant AND criteria from a disjunction: A OR (A AND B) is A.
// This also removes duplicate criteria. It avoids proving that a narrower,
// potentially unindexed context has no newer match after its superset was read.
func minimalCriteria(criteria []criterionLookup) []criterionLookup {
	var minimal []criterionLookup
	for _, candidate := range criteria {
		redundant := false
		for _, existing := range minimal {
			if criterionContains(candidate.criterion, existing.criterion) {
				redundant = true
				break
			}
		}
		if redundant {
			continue
		}
		kept := minimal[:0]
		for _, existing := range minimal {
			if !criterionContains(existing.criterion, candidate.criterion) {
				kept = append(kept, existing)
			}
		}
		minimal = append(kept, candidate)
	}
	return minimal
}

func criterionContains(criterion, subset map[string]any) bool {
	if len(subset) > len(criterion) {
		return false
	}
	for key, value := range subset {
		if got, ok := criterion[key]; !ok || !reflect.DeepEqual(got, value) {
			return false
		}
	}
	return true
}
