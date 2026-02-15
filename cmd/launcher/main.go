package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"tolerex/internal/storage"
)

// ===================================================================================
// INPUT HELPERS
// ===================================================================================

func askInt(prompt string, min int) int {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(prompt)
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "" {
			return min
		}
		value, err := strconv.Atoi(text)
		if err == nil && value >= min {
			return value
		}
		fmt.Printf("Invalid input. Please enter a number >= %d (or leave empty for default %d).\n", min, min)
	}
}

func askChoice(prompt string, allowed ...string) string {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(prompt)
		input := strings.TrimSpace(readLine(reader))
		for _, option := range allowed {
			if strings.EqualFold(input, option) {
				return option
			}
		}
		fmt.Println("Invalid choice. Allowed values:", strings.Join(allowed, ", "))
	}
}

func readLine(r *bufio.Reader) string {
	line, _ := r.ReadString('\n')
	return strings.TrimSpace(line)
}

func waitEnter(message string) {
	fmt.Print(message)
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func readTolerance(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	line := strings.TrimSpace(string(data))
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return 0, errors.New("invalid format, expected TOLERANCE=<positive integer>")
	}

	if strings.TrimSpace(parts[0]) != "TOLERANCE" {
		return 0, errors.New("invalid key, expected TOLERANCE")
	}

	tolerance, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, errors.New("tolerance value must be an integer")
	}
	if tolerance <= 0 {
		return 0, errors.New("tolerance must be greater than 0")
	}

	return tolerance, nil
}

// ===================================================================================
// TERMINAL PROCESS SPAWNER (WINDOWS)
// ===================================================================================

func spawnTerminal(title, command string) {
	psCommand := fmt.Sprintf(
		"$host.UI.RawUI.WindowTitle='%s'; %s",
		title,
		command,
	)

	cmd := exec.Command(
		"cmd",
		"/c",
		"start",
		"powershell",
		"-NoExit",
		"-Command",
		psCommand,
	)
	_ = cmd.Start()
}

// ===================================================================================
// MAIN
// ===================================================================================

func main() {
	fmt.Println("========================================")
	fmt.Println("     TOLEREX CLUSTER DEPLOY LAUNCHER    ")
	fmt.Println("========================================")

	memberCount := askInt("Number of CLUSTER MEMBERS [Default 6]: ", 1)
	clientCount := askInt("Number of STRESS CLIENTS  [Default 4]: ", 0)

	memberIOMode := askChoice(
		"Member Disk IO Mode [buffered/unbuffered]: ",
		storage.ModeBuffered,
		storage.ModeUnbuffered,
	)

	stressMins := 0
	flushRate := 1000
	measureMode := "no"
	csvMode := "no"
	if clientCount > 0 {
		fmt.Println("\n--- Client Configuration ---")
		stressMins = askInt("Stress Test Duration (minutes, 0 for interactive): ", 0)
		flushRate = askInt("Client Flush Rate (1=unbuffered, 1000=default): ", 1)
		measureMode = askChoice("Enable RTT measurement? [yes/no]: ", "yes", "no")
		if measureMode == "yes" {
			csvMode = askChoice("Enable CSV output for detailed metrics? [yes/no]: ", "yes", "no")
		}
	}

	baseDir := os.Getenv("TOLEREX_BASE_DIR")
	if baseDir == "" {
		wd, err := os.Getwd()
		if err == nil {
			baseDir = wd
		} else {
			baseDir = "."
		}
	}

	tolerancePath := os.Getenv("TOLERANCE_CONF")
	if tolerancePath == "" {
		tolerancePath = filepath.Join(baseDir, "config", "tolerance.conf")
	} else if !filepath.IsAbs(tolerancePath) {
		tolerancePath = filepath.Join(baseDir, tolerancePath)
	}

	tolerance, err := readTolerance(tolerancePath)
	if err != nil {
		fmt.Printf("ERROR: tolerance config okunamadi (%s): %v\n", tolerancePath, err)
		os.Exit(1)
	}
	if memberCount < tolerance {
		fmt.Printf("ERROR: Yetersiz uye sayisi. TOLERANCE=%d ancak MEMBER=%d.\n", tolerance, memberCount)
		fmt.Println("Lutfen member sayisini tolerance degerine esitleyin veya tolerance.conf degerini dusurun.")
		os.Exit(1)
	}

	const (
		startGrpcPort   = 5556
		startMetricPort = 9092
		leaderTCPAddr   = "127.0.0.1:6666" // The port leader listens for clients
	)

	measuredDir := ""
	if clientCount > 0 && csvMode == "yes" {
		measuredDir = filepath.Join(baseDir, "measured")
		_ = os.MkdirAll(measuredDir, 0755)
	}

	fmt.Println("\n--- Final Deployment Plan ---")
	fmt.Printf("Members      : %d\n", memberCount)
	fmt.Printf("Clients      : %d\n", clientCount)
	fmt.Printf("Member IO    : %s\n", memberIOMode)
	if clientCount > 0 {
		fmt.Printf("Duration     : %d minutes\n", stressMins)
		fmt.Printf("Flush Rate   : %d\n", flushRate)
		fmt.Printf("Measure RTT  : %s\n", measureMode)
		fmt.Printf("CSV Logging  : %s\n", csvMode)
	} else {
		fmt.Println("Stress       : disabled (no clients requested)")
	}
	fmt.Println("-----------------------------")

	waitEnter("Press ENTER to launch the cluster...")

	// 1. Start Leader
	fmt.Println("\n--- Leader Configuration ---")
	balancerStrat := askChoice("Load Balancer Strategy [least_loaded / p2c]: ", "least_loaded", "p2c")

	fmt.Println("\n[1/3] Launching Leader node...")
	spawnTerminal(
		"TOLEREX-LEADER [PORT:6666]",
		fmt.Sprintf(
			"cd \"%s\"; "+
				"$env:LEADER_GRPC_PORT='5555'; "+
				"$env:LEADER_METRICS_PORT='9090'; "+
				"$env:BALANCER_STRATEGY='%s'; "+
				"go run ./cmd/leader/main.go",
			baseDir,
			balancerStrat,
		),
	)

	time.Sleep(2 * time.Second)
	fmt.Println("Leader process started.")

	// 2. Start Members
	fmt.Printf("[2/3] Launching %d Members...\n", memberCount)
	for i := 0; i < memberCount; i++ {
		grpcPort := startGrpcPort + i
		metricsPort := startMetricPort + i

		spawnTerminal(
			fmt.Sprintf("MEMBER-%d [GRPC:%d]", i+1, grpcPort),
			fmt.Sprintf(
				"cd \"%s\"; "+
					"$env:LEADER_ADDR='localhost:5555'; "+
					"$env:MEMBER_ADDR='localhost:%d'; "+
					"go run ./cmd/member/main.go -port=%d -metrics=%d -io=%s",
				baseDir,
				grpcPort,
				grpcPort,
				metricsPort,
				memberIOMode,
			),
		)
		time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)
	fmt.Println("Cluster membership formed.")

	// 3. Start Clients (optional)
	if clientCount > 0 {
		fmt.Printf("[3/3] Launching %d Clients...\n", clientCount)
		for i := 0; i < clientCount; i++ {
			clientID := fmt.Sprintf("client-%d", i+1)
			offsetVal := i * 100_000

			title := fmt.Sprintf("CLIENT-%d", i+1)

			// Build command flags
			flags := fmt.Sprintf("-addr %s -id %s -offset %d -flush %d", leaderTCPAddr, clientID, offsetVal, flushRate)

			if stressMins > 0 {
				flags += fmt.Sprintf(" -stress %d", stressMins)
				title += " [STRESS]"
			}
			if measureMode == "yes" {
				flags += " -measure"
				title += " [MEASURE]"
			}
			if csvMode == "yes" {
				flags += " -csv"
				title += " [CSV]"
				csvFile := fmt.Sprintf(".\\measured\\%s.csv", clientID)
				flags += fmt.Sprintf(" > %s", csvFile)
			}

			spawnTerminal(
				title,
				fmt.Sprintf("cd \"%s\"; go run ./cmd/client/main.go %s", baseDir, flags),
			)
			time.Sleep(300 * time.Millisecond)
		}
	} else {
		fmt.Println("[3/3] Client launch skipped (0 clients requested).")
	}

	fmt.Println("\nDeployment successful! Use the opened terminals to monitor the cluster.")
	if measuredDir != "" {
		fmt.Printf("Real-time telemetry saved to: %s\n", measuredDir)
	}
	fmt.Println("========================================")
	waitEnter("Press ENTER to exit launcher (this will NOT kill the nodes)...")
}
