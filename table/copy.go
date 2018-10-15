package table

import (
	"context"
	"fmt"

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

func (s *TableService) Copy(jobInsertProjectID string, srcDataset Dataset, dstDataset Dataset) ([]string, error) {
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

		js, err := s.process(jobInsertProjectID, tl, dstDataset)
		if err != nil {

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

func (s *TableService) process(jobInsertProjectID string, tl *bigquery.TableList, dstDataset Dataset) ([]string, error) {
	jobIDs := []string{}
	for _, t := range tl.Tables {
		fmt.Println(t.TableReference.TableId)

		jobID, err := s.copy(jobInsertProjectID, Dataset{t.TableReference.ProjectId, t.TableReference.DatasetId}, dstDataset, t.TableReference.TableId)
		if err != nil {
			fmt.Printf("%s : failed : %s\n", t.TableReference.TableId, err.Error())
			continue
		}
		fmt.Println(jobID)
		jobIDs = append(jobIDs, jobID)
	}

	return jobIDs, nil
}

func (s *TableService) copy(jobInsertProjectID string, srcDataset Dataset, dstDataset Dataset, tableID string) (string, error) {
	job, err := s.bq.Jobs.Insert(jobInsertProjectID, &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Copy: &bigquery.JobConfigurationTableCopy{

				CreateDisposition: "CreateIfNeeded",
				WriteDisposition:  "WRITE_TRUNCATE",
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

	return job.JobReference.JobId, err
}
