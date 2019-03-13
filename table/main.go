package table

import (
	"context"

	"github.com/morikuni/failure"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

const (
	InvalidArgument failure.StringCode = "InvalidArgument"
)

type Service struct {
	bq *bigquery.Service
}

func NewService(ctx context.Context) (*Service, error) {
	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return nil, err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}
	return &Service{
		bq: bq,
	}, nil
}

type Dataset struct {
	Project   string
	DatasetID string
}
