package iam

import (
	"context"
	"fmt"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

// AddRoleToAllDataset is 指定したProjectの全Datasetに対して、指定したRoleを追加する。
// 除外したいDatasetがある場合は excludeDatasets に DatasetIDを入れておく。KVは同じ値でかまわない。 example hoge:hoge
func AddRoleToAllDataset(projectID string, roles []*bigquery.DatasetAccess, excludeDatasets map[string]string) error {
	ctx := context.Background()

	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return err
	}
	dl, err := bq.Datasets.List(projectID).MaxResults(1000).Do()
	if err != nil {
		return err
	}
	for _, d := range dl.Datasets {
		did := d.DatasetReference.DatasetId

		_, ok := excludeDatasets[did]
		if ok {
			fmt.Printf("%s is not process.!!!!!!!!!!!!!!!!!!!!!!\n", did)
			continue
		}

		if err := patch(bq, projectID, did, roles); err != nil {
			return err
		}
	}

	return nil
}

func patch(bq *bigquery.Service, projectID, datasetID string, roles []*bigquery.DatasetAccess) error {
	d, err := bq.Datasets.Get(projectID, datasetID).Do()
	if err != nil {
		return err
	}
	for _, role := range roles {
		d.Access = append(d.Access, role)
	}
	d, err = bq.Datasets.Patch(projectID, d.DatasetReference.DatasetId, d).Do()
	if err != nil {
		return err
	}
	fmt.Printf("%d:%s\n", d.HTTPStatusCode, d.DatasetReference.DatasetId)
	return nil
}
