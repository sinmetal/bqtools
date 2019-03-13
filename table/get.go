package table

import (
	"github.com/morikuni/failure"
	"google.golang.org/api/bigquery/v2"
)

func (s *Service) GetAll(projectID string, dataset string, search *SearchOption) ([]*bigquery.TableListTables, error) {
	const pageTokenNull = "@@NULL_PAGE_TOKEN@@"

	var rtl []*bigquery.TableListTables
	nextPageToken := pageTokenNull
	for {
		tlreq := s.bq.Tables.List(projectID, dataset).MaxResults(365)
		if nextPageToken != pageTokenNull {
			tlreq.PageToken(nextPageToken)
		}
		tl, err := tlreq.Do()
		if err != nil {
			return nil, failure.Wrap(err)
		}
		for _, t := range tl.Tables {
			if search != nil {
				ok, err := search.Check(t.TableReference.TableId)
				if err != nil {
					return nil, failure.Wrap(err)
				}
				if !ok {
					continue
				}
			}
			rtl = append(rtl, t)
		}

		if tl.NextPageToken == "" {
			break
		}
		nextPageToken = tl.NextPageToken
	}

	return rtl, nil
}
