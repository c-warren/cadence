package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime/debug"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {

	// Create MCP server
	s := server.NewMCPServer(
		"Cadence MCP",
		"0.0.1",
		server.WithLogging(),
	)

	// Add tool handlers
	s.AddTool(mcp.NewTool("domain_rr",
		mcp.WithDescription("Check if a cadence domain is resilient to regional outages"),
		mcp.WithString("domain",
			mcp.Required(),
			mcp.Description("Name of the cadence domain to check"),
		),
		mcp.WithString("grpc_endpoint",
			mcp.DefaultString("localhost:7833"),
			mcp.Description("gRPC endpoint of the cadence domain"),
		),
	), domainRRHandler)

	s.AddTool(mcp.NewTool("payload_decoder",
		mcp.WithDescription("Decode a payload that is encoded by hex or base64. The payload is from Cadence database."),
		mcp.WithString("payload",
			mcp.Required(),
			mcp.Description("The payload to decode"),
		),
	), payloadDecoderHandler)

	s.AddTool(mcp.NewTool("command_generator",
		mcp.WithDescription("Convert natural language to Cadence CLI commands. Use this tool when you need to generate Cadence CLI commands from natural language descriptions like 'list failed workflows from past 7 days' or 'start workflow with search attributes'."),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("Natural language description of Cadence CLI command to be generated (e.g., 'list failed workflows from past 7 days', 'start workflow with search attributes')"),
		),
		mcp.WithString("domain",
			mcp.Required(),
			mcp.Description("Target domain name for Cadence command"),
		),
		mcp.WithString("address",
			mcp.DefaultString("localhost:7833"),
			mcp.Description("gRPC endpoint of cadence domain"),
		),
	), cadenceCommandGeneratorHandler)

	debugLog("Cadence MCP started")

	// Start the stdio server
	if err := server.ServeStdio(s); err != nil {
		debugLog("Server error: %v\n", err)
	}

	debugLog("Cadence MCP stopped")
}

func domainRRHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	defer func() {
		// recover from panic
		if r := recover(); r != nil {
			// include the stack trace
			debugLog("Panic: %v\n", r)
			debugLog("Stack trace: %s\n", string(debug.Stack()))
		}
	}()

	domain, ok := request.Params.Arguments["domain"].(string)
	if !ok {
		return nil, errors.New("domain must be a string")
	}

	endpoint, ok := request.Params.Arguments["grpc_endpoint"].(string)
	if !ok {
		endpoint = "localhost:7833"
	}

	// run cadence CLI to check if it's a global domain or not
	cmd := exec.Command("docker", "run", "-t", "--rm", "--network", "host", "ubercadence/cli:master",
		"--transport", "grpc",
		"--address", endpoint,
		"--domain", domain,
		"domain", "describe")
	// run the cmd and capture both stdout and stderr
	output, err := cmd.CombinedOutput()
	if err != nil {
		debugLog("Error checking domain resilience: %v, %s\n", err, string(output))
		return mcp.NewToolResultError("Error checking domain resilience: " + err.Error() + "\n" + string(output)), nil
	}

	// parse the output of the cadence CLI
	// if it contains "IsGlobal(XDC)Domain: true" then it's a global domain
	// otherwise it's not
	if strings.Contains(string(output), "IsGlobal(XDC)Domain: true") {
		return mcp.NewToolResultText("Yes, this domain is resilient to regional outages"), nil
	}

	return mcp.NewToolResultText("No, this domain is not resilient to regional outages. Consider making it a global domain."), nil
}

func payloadDecoderHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	payload, ok := request.Params.Arguments["payload"].(string)
	if !ok {
		return nil, errors.New("payload must be a string")
	}

	// check if the payload is encoded by hex or base64
	enc := "base64"
	if isHexEncoded(payload) {
		enc = "hex"
	}

	debugLog("Decoding payload with %s encoding\n", enc)

	// invoke cadence CLI to decode the payload
	cmd := exec.Command("docker", "run", "-t", "--rm", "--network", "host", "ubercadence/cli:master",
		"admin", "db", "decode_thrift",
		"--input", payload,
		"--encoding", enc)

	// run the cmd and capture both stdout and stderr
	output, err := cmd.CombinedOutput()
	if err != nil {
		debugLog("Error decoding payload: %v, %s\n", err, string(output))
		return mcp.NewToolResultError("Error decoding payload: " + err.Error() + "\n" + string(output)), nil
	}

	return mcp.NewToolResultText(string(output)), nil
}

func isHexEncoded(payload string) bool {
	_, err := hex.DecodeString(strings.TrimPrefix(payload, "0x"))
	return err == nil
}

func debugLog(format string, args ...interface{}) {
	// get the path of the binary
	binaryPath, err := os.Executable()
	if err != nil {
		fmt.Println("Failed to get executable path:", err)
		return
	}
	logFile, err := os.OpenFile(path.Join(path.Dir(binaryPath), "cadence_mcp.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		return
	}
	defer logFile.Close()

	logFile.WriteString(fmt.Sprintf(format, args...))
	logFile.WriteString("\n")
}

func cadenceCommandGeneratorHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	defer func() {
		if r := recover(); r != nil {
			debugLog("Panic in Cadence Command Generator: %v\n", r)
			debugLog("Stack trace: %s\n", string(debug.Stack()))
		}
	}()

	query, ok := request.Params.Arguments["query"].(string)
	if !ok {
		return nil, errors.New("query must be a string")
	}

	domain, ok := request.Params.Arguments["domain"].(string)
	if !ok {
		return nil, errors.New("domain must be a string")
	}

	address, ok := request.Params.Arguments["address"].(string)
	if !ok {
		address = "localhost:7833"
	}

	command, err := generateCadenceCommand(query, domain, address)
	if err != nil {
		return nil, errors.New("error generating cadence command: " + err.Error())
	}

	// format the command to be displayed to user
	formattedCommand := fmt.Sprintf("Command Generated: %s", command)
	return mcp.NewToolResultText(formattedCommand), nil
}
