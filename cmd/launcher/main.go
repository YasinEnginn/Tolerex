package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"tolerex/internal/storage"
)

var inputReader = bufio.NewReader(os.Stdin)

func askInt(prompt string, min, defaultValue int) int {
	for {
		fmt.Print(prompt)
		text, _ := inputReader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "" {
			return defaultValue
		}
		value, err := strconv.Atoi(text)
		if err == nil && value >= min {
			return value
		}
		fmt.Printf("Invalid input. Please enter a number >= %d (or leave empty for default %d).\n", min, defaultValue)
	}
}

func askChoice(prompt string, allowed ...string) string {
	for {
		fmt.Print(prompt)
		input := strings.TrimSpace(readLine(inputReader))
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
	_, _ = inputReader.ReadString('\n')
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

func spawnTerminal(title, command string) error {
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
	return cmd.Start()
}

func waitForTCP(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s did not become ready within %s", address, timeout)
}

func waitForCluster(address string, required int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			reader := bufio.NewReader(conn)
			_, readyErr := reader.ReadString('\n')
			if readyErr == nil {
				_, err = fmt.Fprint(conn, "STATUS\r\n")
			}
			response := ""
			if err == nil && readyErr == nil {
				response, err = reader.ReadString('\n')
			}
			_ = conn.Close()
			if err == nil {
				var members, alive, tolerance, messages int
				if _, scanErr := fmt.Sscanf(strings.TrimSpace(response), "STATUS members=%d alive=%d tolerance=%d messages=%d", &members, &alive, &tolerance, &messages); scanErr == nil && alive >= required {
					return nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("cluster did not register %d live members within %s", required, timeout)
}

func main() {
	fmt.Println("========================================")
	fmt.Println("     TOLEREX CLUSTER DEPLOY LAUNCHER    ")
	fmt.Println("========================================")

	memberCount := askInt("Number of CLUSTER MEMBERS [Default 6]: ", 1, 6)
	clientCount := askInt("Number of STRESS CLIENTS  [Default 4]: ", 0, 4)

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
		stressMins = askInt("Stress Test Duration (minutes, 0 for interactive): ", 0, 0)
		flushRate = askInt("Client Flush Rate (1=unbuffered, 1000=default): ", 1, 1000)
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

	fmt.Println("\n--- Leader Configuration ---")
	balancerStrat := askChoice("Load Balancer Strategy [least_loaded / p2c]: ", "least_loaded", "p2c")

	fmt.Println("\n[1/3] Launching Leader node...")
	if err := spawnTerminal(
		"TOLEREX-LEADER [PORT:6666]",
		fmt.Sprintf(
			"cd \"%s\"; "+
				"$env:LEADER_GRPC_PORT='5555'; "+
				"$env:LEADER_METRICS_PORT='9090'; "+
				"$env:BALANCER_STRATEGY='%s'; "+
				"go run ./cmd/leader",
			baseDir,
			balancerStrat,
		),
	); err != nil {
		fmt.Printf("ERROR: leader could not be started: %v\n", err)
		return
	}
	if err := waitForTCP("127.0.0.1:5555", 20*time.Second); err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	fmt.Println("Leader process started.")

	fmt.Printf("[2/3] Launching %d Members...\n", memberCount)
	for i := 0; i < memberCount; i++ {
		grpcPort := startGrpcPort + i
		metricsPort := startMetricPort + i

		if err := spawnTerminal(
			fmt.Sprintf("MEMBER-%d [GRPC:%d]", i+1, grpcPort),
			fmt.Sprintf(
				"cd \"%s\"; "+
					"$env:LEADER_ADDR='localhost:5555'; "+
					"$env:MEMBER_ADDR='localhost:%d'; "+
					"go run ./cmd/member -port=%d -metrics=%d -io=%s",
				baseDir,
				grpcPort,
				grpcPort,
				metricsPort,
				memberIOMode,
			),
		); err != nil {
			fmt.Printf("ERROR: member-%d could not be started: %v\n", i+1, err)
			return
		}
	}
	for i := 0; i < memberCount; i++ {
		if err := waitForTCP(fmt.Sprintf("127.0.0.1:%d", startGrpcPort+i), 20*time.Second); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
	}
	if err := waitForCluster(leaderTCPAddr, memberCount, 20*time.Second); err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	fmt.Println("Cluster membership formed.")

	if clientCount > 0 {
		fmt.Printf("[3/3] Launching %d Clients...\n", clientCount)
		for i := 0; i < clientCount; i++ {
			clientID := fmt.Sprintf("client-%d", i+1)
			offsetVal := i * 100_000

			title := fmt.Sprintf("CLIENT-%d", i+1)

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

			if err := spawnTerminal(
				title,
				fmt.Sprintf("cd \"%s\"; go run ./cmd/client %s", baseDir, flags),
			); err != nil {
				fmt.Printf("ERROR: client-%d could not be started: %v\n", i+1, err)
				return
			}
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
