package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nebula/nebula/internal/cli"
)

func printUsage() {
	fmt.Println(`Nebula CLI — Cloud Control Plane Tool

Usage:
  nebula <command> [arguments]

Available Commands:
  login     Authenticate with the Nebula Control Plane API
  init      Initialize and register a new project
  deploy    Deploy an application image or source code to the cluster
  status    Show status of a project and its active deployment
  logs      Stream or print project and deployment logs
  version   Display CLI version

Use "nebula <command> --help" for more information about a command.`)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	ctx := context.Background()

	// Global version flag
	if cmd == "version" || cmd == "--version" || cmd == "-v" {
		fmt.Printf("nebula CLI version %s\n", cli.CurrentClientVersion)
		return
	}

	// Resolve local config if exists
	home, _ := os.UserHomeDir()
	configPath := filepath.Join(home, cli.DefaultConfigFile)
	cfg, _ := cli.LoadConfig(configPath)
	if cfg == nil {
		cfg = &cli.Config{
			Endpoint: "http://127.0.0.1:8080",
		}
	}

	switch cmd {
	case "login":
		fs := flag.NewFlagSet("login", flag.ContinueOnError)
		endpoint := fs.String("endpoint", cfg.Endpoint, "Control Plane API URL")
		token := fs.String("token", "", "Authentication token")
		if err := fs.Parse(os.Args[2:]); err != nil {
			os.Exit(1)
		}

		if *token == "" {
			fmt.Fprintln(os.Stderr, "Error: --token is required")
			os.Exit(1)
		}

		client := cli.NewClient(*endpoint, *token)
		if err := client.Login(ctx, *token); err != nil {
			fmt.Fprintf(os.Stderr, "Login failed: %v\n", err)
			os.Exit(1)
		}

		cfg.Endpoint = *endpoint
		cfg.Token = *token
		if err := cli.SaveConfig(configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to save config to %s: %v\n", configPath, err)
		}

		fmt.Printf("✅ Successfully authenticated to %s\n", *endpoint)

	case "init":
		fs := flag.NewFlagSet("init", flag.ContinueOnError)
		endpoint := fs.String("endpoint", cfg.Endpoint, "Control Plane API URL")
		branch := fs.String("branch", "main", "Default git branch")
		rootDir := fs.String("root", ".", "Root directory for builds")
		if err := fs.Parse(os.Args[2:]); err != nil {
			os.Exit(1)
		}

		args := fs.Args()
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "Error: project name is required: nebula init <project-name>")
			os.Exit(1)
		}
		projectName := args[0]

		client := cli.NewClient(*endpoint, cfg.Token)
		project, err := client.InitProject(ctx, projectName, *branch, *rootDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Init failed: %v\n", err)
			os.Exit(1)
		}

		cfg.DefaultProject = project.ID
		_ = cli.SaveConfig(configPath, cfg)

		fmt.Printf("✅ Project initialized successfully!\n")
		fmt.Printf("  Project ID: %s\n", project.ID)
		fmt.Printf("  Name:       %s\n", project.Name)

	case "deploy":
		fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
		endpoint := fs.String("endpoint", cfg.Endpoint, "Control Plane API URL")
		projectID := fs.String("project", cfg.DefaultProject, "Target project ID")
		image := fs.String("image", "", "Container image to deploy")
		sourcePath := fs.String("dir", "", "Path to source directory to build and deploy")
		replicas := fs.Int("replicas", 1, "Desired number of instances")
		if err := fs.Parse(os.Args[2:]); err != nil {
			os.Exit(1)
		}

		if *projectID == "" {
			fmt.Fprintln(os.Stderr, "Error: project ID is required via --project flag or prior 'nebula init'")
			os.Exit(1)
		}
		if *image == "" && *sourcePath == "" {
			*sourcePath = "."
		}

		client := cli.NewClient(*endpoint, cfg.Token)
		fmt.Printf("🚀 Initiating deployment for project %s...\n", *projectID)

		dep, err := client.Deploy(ctx, cli.DeployParams{
			ProjectID:     *projectID,
			Image:         *image,
			SourcePath:    *sourcePath,
			InstanceCount: *replicas,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Deployment failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✅ Deployment successful! (Status: %s, ID: %s)\n", dep.Status, dep.ID)
		if dep.ImageDigest != "" {
			fmt.Printf("  Image Digest: %s\n", dep.ImageDigest)
		}

	case "status":
		fs := flag.NewFlagSet("status", flag.ContinueOnError)
		endpoint := fs.String("endpoint", cfg.Endpoint, "Control Plane API URL")
		projectID := fs.String("project", cfg.DefaultProject, "Project ID to query")
		if err := fs.Parse(os.Args[2:]); err != nil {
			os.Exit(1)
		}

		if *projectID == "" {
			fmt.Fprintln(os.Stderr, "Error: project ID is required: nebula status --project=<id>")
			os.Exit(1)
		}

		client := cli.NewClient(*endpoint, cfg.Token)
		status, err := client.Status(ctx, *projectID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Status check failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Project: %s (%s)\n", status.Project.Name, status.Project.ID)
		fmt.Printf("Service URL: %s\n", status.ServiceURL)
		if status.LatestDeployment != nil {
			d := status.LatestDeployment
			fmt.Printf("Latest Deployment: %s\n", d.ID)
			fmt.Printf("  Status:    %s (Stage: %s)\n", d.Status, d.Stage)
			fmt.Printf("  Instances: %d / %d running\n", d.RunningCount, d.InstanceCount)
			fmt.Printf("  Image:     %s\n", d.Image)
		} else {
			fmt.Println("No deployments recorded for this project.")
		}

	case "logs":
		fs := flag.NewFlagSet("logs", flag.ContinueOnError)
		endpoint := fs.String("endpoint", cfg.Endpoint, "Control Plane API URL")
		projectID := fs.String("project", cfg.DefaultProject, "Project ID")
		deploymentID := fs.String("deployment", "", "Specific deployment ID")
		follow := fs.Bool("follow", false, "Stream live logs")
		fs.BoolVar(follow, "f", false, "Stream live logs (shorthand)")
		if err := fs.Parse(os.Args[2:]); err != nil {
			os.Exit(1)
		}

		if *projectID == "" && *deploymentID == "" {
			fmt.Fprintln(os.Stderr, "Error: either --project or --deployment must be specified")
			os.Exit(1)
		}

		client := cli.NewClient(*endpoint, cfg.Token)
		if err := client.Logs(ctx, *projectID, *deploymentID, *follow, os.Stdout); err != nil {
			if !strings.Contains(err.Error(), "context canceled") {
				fmt.Fprintf(os.Stderr, "Logs error: %v\n", err)
				os.Exit(1)
			}
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q. Run 'nebula --help' for usage.\n", cmd)
		os.Exit(1)
	}
}
