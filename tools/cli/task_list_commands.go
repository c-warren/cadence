package cli

import (
	"io"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/tools/common/commoncli"
)

type (
	TaskListPollerRow struct {
		ActivityIdentity string    `header:"Activity Poller Identity"`
		DecisionIdentity string    `header:"Decision Poller Identity"`
		LastAccessTime   time.Time `header:"Last Access Time"`
	}
	TaskListPartitionRow struct {
		ActivityPartition string `header:"Activity Task List Partition"`
		DecisionPartition string `header:"Decision Task List Partition"`
		Host              string `header:"Host"`
	}
)

// DescribeTaskList show pollers info of a given tasklist
func DescribeTaskList(c *cli.Context) error {
	wfClient, err := getWorkflowClient(c)
	if err != nil {
		return err
	}
	domain, err := getRequiredOption(c, FlagDomain)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}
	taskList, err := getRequiredOption(c, FlagTaskList)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}
	taskListType := strToTaskListType(c.String(FlagTaskListType)) // default type is decision

	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error in creating context:", err)
	}
	request := &types.DescribeTaskListRequest{
		Domain: domain,
		TaskList: &types.TaskList{
			Name: taskList,
		},
		TaskListType: &taskListType,
	}
	response, err := wfClient.DescribeTaskList(ctx, request)
	if err != nil {
		return commoncli.Problem("Operation DescribeTaskList failed.", err)
	}

	pollers := response.Pollers
	if len(pollers) == 0 {
		return commoncli.Problem(colorMagenta("No poller for tasklist: "+taskList), nil)
	}

	return printTaskListPollers(getDeps(c).Output(), pollers, taskListType)
}

// ListTaskListPartitions gets all the tasklist partition and host information.
func ListTaskListPartitions(c *cli.Context) error {
	frontendClient, err := getDeps(c).ServerFrontendClient(c)
	if err != nil {
		return err
	}
	domain, err := getRequiredOption(c, FlagDomain)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}
	taskList, err := getRequiredOption(c, FlagTaskList)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}
	taskListType := strToTaskListType(c.String(FlagTaskListType)) // default type is decision

	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error in creating context:", err)
	}
	request := &types.ListTaskListPartitionsRequest{
		Domain:   domain,
		TaskList: &types.TaskList{Name: taskList},
	}

	response, err := frontendClient.ListTaskListPartitions(ctx, request)
	if err != nil {
		return commoncli.Problem("Operation ListTaskListPartitions failed.", err)
	}

	switch taskListType {
	case types.TaskListTypeActivity:
		return printTaskListPartitions(types.TaskListTypeActivity, response.ActivityTaskListPartitions)
	case types.TaskListTypeDecision:
		return printTaskListPartitions(types.TaskListTypeDecision, response.DecisionTaskListPartitions)
	default:
		// should never happen
		return nil
	}
}

func printTaskListPollers(w io.Writer, pollers []*types.PollerInfo, taskListType types.TaskListType) error {
	table := []TaskListPollerRow{}
	for _, poller := range pollers {
		table = append(table, TaskListPollerRow{
			ActivityIdentity: poller.GetIdentity(),
			DecisionIdentity: poller.GetIdentity(),
			LastAccessTime:   time.Unix(0, poller.GetLastAccessTime())})
	}
	return RenderTable(w, table, RenderOptions{Color: true, PrintDateTime: true, OptionalColumns: map[string]bool{
		"Activity Poller Identity": taskListType == types.TaskListTypeActivity,
		"Decision Poller Identity": taskListType == types.TaskListTypeDecision,
	}})
}

func printTaskListPartitions(taskListType types.TaskListType, partitions []*types.TaskListPartitionMetadata) error {
	table := []TaskListPartitionRow{}
	for _, partition := range partitions {
		table = append(table, TaskListPartitionRow{
			ActivityPartition: partition.GetKey(),
			DecisionPartition: partition.GetKey(),
			Host:              partition.GetOwnerHostName(),
		})
	}
	return RenderTable(os.Stdout, table, RenderOptions{Color: true, OptionalColumns: map[string]bool{
		"Activity Task List Partition": taskListType == types.TaskListTypeActivity,
		"Decision Task List Partition": taskListType == types.TaskListTypeDecision,
	}})
}
