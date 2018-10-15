package job

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

type JobService struct {
	bq *bigquery.Service
}

func NewJobService(ctx context.Context) (*JobService, error) {
	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return nil, err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}
	return &JobService{
		bq: bq,
	}, nil
}

type FailedJob struct {
	JobID     string
	Query     *bigquery.JobConfigurationQuery
	Load      *bigquery.JobConfigurationLoad
	Extract   *bigquery.JobConfigurationExtract
	TableCopy *bigquery.JobConfigurationTableCopy
	Message   string
	StartTime time.Time
	EndTime   time.Time
}

func (s *JobService) ListFailedJob(projectID string, jobType string, limit int64) ([]*FailedJob, error) {
	var fjobs []*FailedJob

	jl, err := s.ListJob(projectID, jobType, "done", limit)
	if err != nil {
		return nil, err
	}
	for _, j := range jl {
		if j.ErrorResult == nil {
			continue
		}

		fjobs = append(fjobs, &FailedJob{
			JobID:     j.Id,
			Query:     j.Configuration.Query,
			Load:      j.Configuration.Load,
			Extract:   j.Configuration.Extract,
			TableCopy: j.Configuration.Copy,
			Message:   j.ErrorResult.Message,
			StartTime: time.Unix(j.Statistics.StartTime*1000, 0),
			EndTime:   time.Unix(j.Statistics.EndTime*1000, 0),
		})
	}

	return fjobs, nil
}

// ListJob
// jobType: Can be QUERY, LOAD, EXTRACT, COPY or UNKNOWN.
// State: Can be done, pending, running
func (s *JobService) ListJob(projectID string, jobType string, state string, limit int64) ([]*bigquery.JobListJobs, error) {
	const pageTokenNull = "@@NULL_PAGE_TOKEN@@"

	var jobs []*bigquery.JobListJobs

	var maxResults int64 = 100
	if maxResults > limit {
		maxResults = limit
	}
	var count int64
	nextPageToken := pageTokenNull
	for {
		jlreq := s.bq.Jobs.List(projectID).Projection("full").MaxResults(maxResults)
		if state != "" {
			jlreq.StateFilter(state)
		}
		if nextPageToken != pageTokenNull {
			jlreq.PageToken(nextPageToken)
		}
		jl, err := jlreq.Do()
		if err != nil {
			return nil, err
		}

		jljl := s.process(jl, jobType)
		jobs = append(jobs, jljl...)

		fmt.Printf("TotalItems:%d, NextPageToken:%s\n", len(jl.Jobs), jl.NextPageToken)
		if jl.NextPageToken == "" {
			break
		}
		nextPageToken = jl.NextPageToken
		count += int64(len(jl.Jobs))
		if count > limit {
			break
		}
	}

	return jobs, nil
}

func (s *JobService) process(jl *bigquery.JobList, jobType string) []*bigquery.JobListJobs {
	fjl := []*bigquery.JobListJobs{}
	for _, j := range jl.Jobs {
		if jobType != "" && j.Configuration.JobType != jobType {
			continue
		}

		fjl = append(fjl, j)
	}

	return fjl
}

func (s *JobService) Cancel(projectID string, jobID string) error {
	res, err := s.bq.Jobs.Cancel(projectID, jobID).Do()
	if err != nil {
		return err
	}
	fmt.Printf("%s : %d\n", res.Job.Id, res.ServerResponse.HTTPStatusCode)
	return nil
}
