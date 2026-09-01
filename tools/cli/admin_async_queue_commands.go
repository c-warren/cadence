package cli

import (
	"encoding/json"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/tools/common/commoncli"
)

func AdminGetAsyncWFConfig(c *cli.Context) error {
	adminClient, err := getDeps(c).ServerAdminClient(c)
	if err != nil {
		return err
	}

	domainName, err := getRequiredOption(c, FlagDomain)
	if err != nil {
		return commoncli.Problem("Required flag not present:", err)
	}
	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error in creating context: ", err)
	}

	req := &types.GetDomainAsyncWorkflowConfiguratonRequest{
		Domain: domainName,
	}

	resp, err := adminClient.GetDomainAsyncWorkflowConfiguraton(ctx, req)
	if err != nil {
		return commoncli.Problem("Failed to get async wf queue config", err)
	}

	if resp == nil || resp.Configuration == nil {
		fmt.Printf("Async workflow queue config not found for domain %s\n", domainName)
		return nil
	}

	fmt.Printf("Async workflow queue config for domain %s:\n", domainName)
	prettyPrintJSONObject(getDeps(c).Output(), resp.Configuration)
	return nil
}

func AdminUpdateAsyncWFConfig(c *cli.Context) error {
	adminClient, err := getDeps(c).ServerAdminClient(c)
	if err != nil {
		return err
	}

	domainName, err := getRequiredOption(c, FlagDomain)
	if err != nil {
		return commoncli.Problem("Required flag not present:", err)
	}
	asyncWFCfgJSON, err := getRequiredOption(c, FlagJSON)
	if err != nil {
		return commoncli.Problem("Required flag not present:", err)
	}

	var cfg types.AsyncWorkflowConfiguration
	err = json.Unmarshal([]byte(asyncWFCfgJSON), &cfg)
	if err != nil {
		return commoncli.Problem("Failed to parse async workflow config", err)
	}

	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error in creating context: ", err)
	}

	req := &types.UpdateDomainAsyncWorkflowConfiguratonRequest{
		Domain:        domainName,
		Configuration: &cfg,
	}

	_, err = adminClient.UpdateDomainAsyncWorkflowConfiguraton(ctx, req)
	if err != nil {
		return commoncli.Problem("Failed to update async workflow queue config", err)
	}

	fmt.Printf("Successfully updated async workflow queue config for domain %s\n", domainName)
	return nil
}
