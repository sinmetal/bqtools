package table

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/morikuni/failure"
	"google.golang.org/api/bigquery/v2"
)

// DeleteAll is 指定したDatasetの条件に一致するTableを削除する
// start, end で指定した範囲に収まってるYYYYMMDDのTableをコピーする。
func (s *Service) DeleteAll(dataset Dataset, search *SearchOption) error {
	const pageTokenNull = "@@NULL_PAGE_TOKEN@@"

	nextPageToken := pageTokenNull
	for {
		tlreq := s.bq.Tables.List(dataset.Project, dataset.DatasetID).MaxResults(400)
		if nextPageToken != pageTokenNull {
			tlreq.PageToken(nextPageToken)
		}
		tl, err := tlreq.Do()
		if err != nil {
			return nil, err
		}

		if err := s.processDelete(tl, search); err != nil {
			return err
		}

		fmt.Printf("TotalItems:%d, NextPageToken:%s\n", len(tl.Tables), tl.NextPageToken)
		if tl.NextPageToken == "" {
			break
		}
		nextPageToken = tl.NextPageToken
	}

	return nil
}

func (s *Service) processDelete(tl *bigquery.TableList, search *SearchOption) error {
	for _, t := range tl.Tables {
		if search != nil {
			ok, err := search.Check(t.TableReference.TableId)
			if err != nil {
				return failure.Wrap(err)
			}
			if !ok {
				continue
			}
		}

		fmt.Println(t.TableReference.TableId)

		if err := s.Delete(&Dataset{t.TableReference.ProjectId, t.TableReference.DatasetId}, t.TableReference.TableId); err != nil {
			fmt.Printf("%s : failed : %s\n", t.TableReference.TableId, err.Error())
			continue
		}
		time.Sleep(80*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond)
	}

	return nil
}

func (s *Service) Delete(dataset *Dataset, tableID string) (rerr error) {
	const maxRetryCount = 3

	for i := 0; i < maxRetryCount; i++ {
		if err := s.bq.Tables.Delete(dataset.Project, dataset.DatasetID, tableID).Do(); err != nil {
			fmt.Printf("failed Table Delete : err = %+v", err)
			return err
		}
	}

	return
}
