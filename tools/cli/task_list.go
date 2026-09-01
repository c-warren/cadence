package cli

import "github.com/urfave/cli/v2"

func newTaskListCommands() []*cli.Command {
	return []*cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe pollers info of tasklist",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagTaskList,
					Aliases: []string{"tl"},
					Usage:   "TaskList description",
				},
				&cli.StringFlag{
					Name:    FlagTaskListType,
					Aliases: []string{"tlt"},
					Value:   "decision",
					Usage:   "Optional TaskList type [decision|activity]",
				},
			},
			Action: DescribeTaskList,
		},
		{
			Name:    "list-partition",
			Aliases: []string{"lp"},
			Usage:   "List all the tasklist partitions and the hostname for partitions.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagTaskList,
					Aliases: []string{"tl"},
					Usage:   "TaskList description",
				},
				&cli.StringFlag{
					Name:    FlagTaskListType,
					Aliases: []string{"tlt"},
					Value:   "decision",
					Usage:   "Optional TaskList type [decision|activity]",
				},
			},
			Action: ListTaskListPartitions,
		},
	}
}
