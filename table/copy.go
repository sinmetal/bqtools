package table

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

type TableService struct {
	bq *bigquery.Service
}

func NewTableService(ctx context.Context) (*TableService, error) {
	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return nil, err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}
	return &TableService{
		bq: bq,
	}, nil
}

type Dataset struct {
	Project   string
	DatasetID string
}

// Copy is srcDatasetからdstDatasetにTableをコピーする
// start, end で指定した範囲に収まってるYYYYMMDDのTableをコピーする。
func (s *TableService) CopyAll(jobInsertProjectID string, srcDataset Dataset, dstDataset Dataset, start, end string) ([]string, error) {
	const pageTokenNull = "@@NULL_PAGE_TOKEN@@"

	jobIDs := []string{}
	nextPageToken := pageTokenNull
	for {
		tlreq := s.bq.Tables.List(srcDataset.Project, srcDataset.DatasetID).MaxResults(400)
		if nextPageToken != pageTokenNull {
			tlreq.PageToken(nextPageToken)
		}
		tl, err := tlreq.Do()
		if err != nil {
			return nil, err
		}

		js, err := s.process(jobInsertProjectID, tl, dstDataset, start, end)
		if err != nil {
			return nil, err
		}
		jobIDs = append(jobIDs, js...)

		fmt.Printf("TotalItems:%d, NextPageToken:%s\n", len(tl.Tables), tl.NextPageToken)
		if tl.NextPageToken == "" {
			break
		}
		nextPageToken = tl.NextPageToken
	}

	return jobIDs, nil
}

func (s *TableService) process(jobInsertProjectID string, tl *bigquery.TableList, dstDataset Dataset, start string, end string) ([]string, error) {
	jobIDs := []string{}
	for _, t := range tl.Tables {
		isDate, err := IsYYYYMMDD(t.TableReference.TableId, start, end)
		if err != nil {
			fmt.Printf("%s is Failed in range specification of %s-%s\n", t.TableReference.TableId, start, end)
			continue
		}
		if !isDate {
			fmt.Printf("%s isSince it is not in the range of %s-%s\n", t.TableReference.TableId, start, end)
			continue
		}
		fmt.Println(t.TableReference.TableId)

		jobID, err := s.Copy(jobInsertProjectID, Dataset{t.TableReference.ProjectId, t.TableReference.DatasetId}, dstDataset, t.TableReference.TableId)
		if err != nil {
			fmt.Printf("%s : failed : %s\n", t.TableReference.TableId, err.Error())
			continue
		}
		fmt.Println(jobID)
		jobIDs = append(jobIDs, jobID)
		time.Sleep(80*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond)
	}

	return jobIDs, nil
}

func (s *TableService) Copy(jobInsertProjectID string, srcDataset Dataset, dstDataset Dataset, tableID string) (jobID string, rerr error) {
	for i := 0; i < 3; i++ {
		job, err := s.bq.Jobs.Insert(jobInsertProjectID, &bigquery.Job{
			Configuration: &bigquery.JobConfiguration{
				Copy: &bigquery.JobConfigurationTableCopy{

					WriteDisposition: "WRITE_TRUNCATE",
					SourceTable: &bigquery.TableReference{
						ProjectId: srcDataset.Project,
						DatasetId: srcDataset.DatasetID,
						TableId:   tableID,
					},
					DestinationTable: &bigquery.TableReference{
						ProjectId: dstDataset.Project,
						DatasetId: dstDataset.DatasetID,
						TableId:   tableID,
					},
				},
			},
		}).Do()
		if err != nil {
			fmt.Printf("failed Table Copy : err = %+v", err)
			if strings.Contains(err.Error(), "CREATEIFNEEDED") {
				fmt.Printf("retry %s:%s\n", srcDataset, tableID)
				rerr = err
				time.Sleep(time.Duration(300)*time.Millisecond + time.Duration(500)*time.Duration(i)*time.Millisecond)
				continue
			}
			return "", err
		}
		return job.JobReference.JobId, nil
	}

	return
}
