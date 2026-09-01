package cli

import (
	"os"
	"sort"

	"github.com/urfave/cli/v2"

	"github.com/uber/cadence/tools/common/commoncli"
)

type (
	SearchAttributesRow struct {
		Key       string `header:"Key"`
		ValueType string `header:"Value type"`
	}
	SearchAttributesTable []SearchAttributesRow
)

func (s SearchAttributesTable) Len() int {
	return len(s)
}
func (s SearchAttributesTable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s SearchAttributesTable) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

// GetSearchAttributes get valid search attributes
func GetSearchAttributes(c *cli.Context) error {
	wfClient, err := getWorkflowClient(c)
	if err != nil {
		return err
	}
	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error in creating context:", err)
	}

	resp, err := wfClient.GetSearchAttributes(ctx)
	if err != nil {
		return commoncli.Problem("Failed to get search attributes.", err)
	}

	table := SearchAttributesTable{}
	for k, v := range resp.Keys {
		table = append(table, SearchAttributesRow{Key: k, ValueType: v.String()})
	}
	sort.Sort(table)
	return RenderTable(os.Stdout, table, RenderOptions{Color: true, Border: true})
}
