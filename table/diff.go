package table

import (
	"fmt"
	"strings"

	"google.golang.org/api/bigquery/v2"
)

func (s *TableService) Diff(baseDataset Dataset, targetDataset Dataset) error {
	const pageTokenNull = "@@NULL_PAGE_TOKEN@@"

	var tltl []*bigquery.TableListTables
	nextPageToken := pageTokenNull
	for {
		tlreq := s.bq.Tables.List(baseDataset.Project, baseDataset.DatasetID).MaxResults(400)
		if nextPageToken != pageTokenNull {
			tlreq.PageToken(nextPageToken)
		}
		tl, err := tlreq.Do()
		if err != nil {
			return err
		}
		tltl = append(tltl, tl.Tables...)

		fmt.Printf("TotalItems:%d, NextPageToken:%+v\n", len(tl.Tables), tl.NextPageToken)
		fmt.Printf("TokenLength=%d\n", len(tl.NextPageToken))
		if len(tl.NextPageToken) < 1 || tl.NextPageToken == "" {
			break
		}
		nextPageToken = tl.NextPageToken
	}

	fmt.Println("Start Diff...")
	for _, tl := range tltl {
		t1, err := s.getTable(tl.TableReference.ProjectId, tl.TableReference.DatasetId, tl.TableReference.TableId)
		if err != nil {
			return err
		}
		t2, err := s.getTable(targetDataset.Project, targetDataset.DatasetID, tl.TableReference.TableId)
		if err != nil {
			if !strings.Contains(err.Error(), "404: Not found: Table") {
				return err
			}
		}
		td := s.diff(t1, t2)
		if td.T1NumRows != 0 || td.T2NumRows != 0 || len(td.SchemaDiff) > 0 {
			fmt.Printf("%+v\n", td)
		}
	}

	return nil
}

func (s *TableService) getTable(projectID string, datasetID string, tableID string) (*bigquery.Table, error) {
	t, err := s.bq.Tables.Get(projectID, datasetID, tableID).Do()
	if err != nil {
		return nil, err
	}
	return t, nil
}

type TableDiff struct {
	TableID    string
	T1NumRows  uint64
	T2NumRows  uint64
	SchemaDiff []string
}

func (s *TableService) diff(t1 *bigquery.Table, t2 *bigquery.Table) *TableDiff {
	td := &TableDiff{
		TableID: t1.TableReference.TableId,
	}
	if t1.NumRows == 0 {
		// t1がZero行の時はスキップ
		return td
	}
	if t2 == nil {
		td.SchemaDiff = append(td.SchemaDiff, fmt.Sprintf("%s table is not found", t1.TableReference.TableId))
		return td
	}
	if t1.NumRows != t2.NumRows {
		td.T1NumRows = t1.NumRows
		td.T2NumRows = t2.NumRows
	}
	t1sm := s.tableSchemasToTableSchemaMap(t1.Schema.Fields)
	t2sm := s.tableSchemasToTableSchemaMap(t2.Schema.Fields)
	for _, t1s := range t1sm {
		t2s, ok := t2sm[t1s.Name]
		if !ok {
			td.SchemaDiff = append(td.SchemaDiff, fmt.Sprintf("%s column is not found", t1s.Name))
			continue
		}
		if e, g := t1s.Type, t2s.Type; e != g {
			td.SchemaDiff = append(td.SchemaDiff, fmt.Sprintf("%s column is expected %s; got %s", t1s.Name, e, g))
		}
		// Query -> Destination Tableを実行すると、NULLABLEになるので、Modeはひとまず確認しない
		//if e, g := t1s.Mode, t2s.Mode; e != g {
		//	td.SchemaDiff = append(td.SchemaDiff, fmt.Sprintf("%s is expected %s; got %s", t1s.Name, e, g))
		//}
	}
	return td
}

func (s *TableService) tableSchemasToTableSchemaMap(tss []*bigquery.TableFieldSchema) map[string]*bigquery.TableFieldSchema {
	tm := map[string]*bigquery.TableFieldSchema{}
	for _, ts := range tss {
		tm[ts.Name] = ts
	}

	return tm
}
