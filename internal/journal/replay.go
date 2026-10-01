package journal

import (
	"sort"

	"github.com/wyw14/cry-97/internal/model"
)

// OrderByCommit returns a copy of the events in commit order (line id, then
// strictly increasing sequence). Recovery folds events in this order so the
// resulting projection matches the committed history.
func OrderByCommit(events []model.Event) []model.Event {
	ordered := append([]model.Event(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].LineID == ordered[j].LineID {
			return ordered[i].Sequence < ordered[j].Sequence
		}
		return ordered[i].LineID < ordered[j].LineID
	})
	return ordered
}
