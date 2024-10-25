package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Config holds the application configuration
type Config struct {
	NodeURL         string        `json:"nodeUrl"`
	ContractAddress string        `json:"contractAddress"`
	AbiFilePath     string        `json:"abiFilePath"`
	PollInterval    time.Duration `json:"pollInterval"`
}

// DataPacket represents a structured data packet
type DataPacket struct {
	DeviceID  string            `json:"deviceId"`
	SensorID  string            `json:"sensorId"`
	JSONData  []string          `json:"jsonData"`
	Timestamp time.Time         `json:"timestamp"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Monitor handles the blockchain monitoring logic
type Monitor struct {
	client       *ethclient.Client
	config       *Config
	abi          abi.ABI
	contractAddr common.Address
	lastBlockNum uint64
	mutex        sync.RWMutex
	errorHandler func(error)
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewMonitor creates a new blockchain monitor
func NewMonitor(config *Config) (*Monitor, error) {
	client, err := ethclient.Dial(config.NodeURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum client: %w", err)
	}

	parsedABI, err := loadABI(config.AbiFilePath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Monitor{
		client:       client,
		config:       config,
		abi:          parsedABI,
		contractAddr: common.HexToAddress(config.ContractAddress),
		ctx:          ctx,
		cancel:       cancel,
		errorHandler: func(err error) {
			log.Printf("Error: %v", err)
		},
	}, nil
}

// Start begins monitoring the blockchain
func (m *Monitor) Start() error {
	log.Printf("Starting monitoring of contract: %s", m.config.ContractAddress)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	ticker := time.NewTicker(m.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.processNewBlocks(); err != nil {
				m.errorHandler(fmt.Errorf("block processing error: %w", err))
			}
		case <-sigChan:
			log.Println("Received interrupt signal, shutting down...")
			m.Stop()
			return nil
		case <-m.ctx.Done():
			return nil
		}
	}
}

// Stop gracefully shuts down the monitor
func (m *Monitor) Stop() {
	m.cancel()
}

// processNewBlocks handles new block processing
func (m *Monitor) processNewBlocks() error {
	header, err := m.client.HeaderByNumber(m.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get latest block header: %w", err)
	}

	currentBlock := header.Number.Uint64()
	m.mutex.RLock()
	lastBlock := m.lastBlockNum
	m.mutex.RUnlock()

	if currentBlock <= lastBlock {
		return nil
	}

	log.Printf("Processing block: %d", currentBlock)

	if err := m.processBlock(currentBlock); err != nil {
		return fmt.Errorf("failed to process block %d: %w", currentBlock, err)
	}

	m.mutex.Lock()
	m.lastBlockNum = currentBlock
	m.mutex.Unlock()

	return nil
}

// processBlock handles individual block processing
func (m *Monitor) processBlock(blockNum uint64) error {
	block, err := m.client.BlockByNumber(m.ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	for _, tx := range block.Transactions() {
		if err := m.processTransaction(tx); err != nil {
			log.Printf("Error processing transaction %s: %v", tx.Hash().Hex(), err)
			continue
		}
	}

	fmt.Println(strings.Repeat("#", 40))
	return nil
}

// processTransaction handles individual transaction processing
func (m *Monitor) processTransaction(tx *types.Transaction) error {
	receipt, err := m.client.TransactionReceipt(m.ctx, tx.Hash())
	if err != nil {
		return fmt.Errorf("failed to get transaction receipt: %w", err)
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil
	}

	txData := tx.Data()
	if len(txData) <= 4 {
		return nil
	}

	method, err := m.abi.MethodById(txData[:4])
	if err != nil {
		return nil // Not a method we're interested in
	}

	inputs := make(map[string]interface{})
	if err := method.Inputs.UnpackIntoMap(inputs, txData[4:]); err != nil {
		return fmt.Errorf("failed to unpack inputs: %w", err)
	}

	return m.printDataPacket(inputs)
}

// printDataPacket formats and prints the data packet
func (m *Monitor) printDataPacket(inputs map[string]interface{}) error {
	deviceID, ok1 := inputs["deviceId"].(string)
	sensorID, ok2 := inputs["sensorId"].(string)
	jsonData, ok3 := inputs["jsonData"].([]string)

	if !ok1 || !ok2 || !ok3 {
		return fmt.Errorf("invalid input format")
	}

	fmt.Printf("\nDevice ID: %s, Sensor ID: %s\n", deviceID, sensorID)
	fmt.Println(strings.Repeat("-", 50))

	for _, packet := range jsonData {
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(packet), &data); err != nil {
			fmt.Printf("Invalid JSON packet: %s\n", packet)
			continue
		}

		prettyPrint(data)
	}

	return nil
}

// prettyPrint formats and prints JSON data
func prettyPrint(data map[string]interface{}) {
	var fields []string
	for k, v := range data {
		fields = append(fields, fmt.Sprintf("%s: %v", k, v))
	}
	fmt.Printf("{ %s }\n", strings.Join(fields, ", "))
}

// loadABI reads and parses the ABI file
func loadABI(filePath string) (abi.ABI, error) {
	abiFile, err := os.ReadFile(filePath)
	if err != nil {
		return abi.ABI{}, fmt.Errorf("failed to read ABI file: %w", err)
	}

	parsedABI, err := abi.JSON(strings.NewReader(string(abiFile)))
	if err != nil {
		return abi.ABI{}, fmt.Errorf("failed to parse contract ABI: %w", err)
	}

	return parsedABI, nil
}

func main() {
	// Default configuration
	config := &Config{
		NodeURL:         "http://10.110.11.137:8541",
		ContractAddress: "0xDAf3bB8F1bC06DB3d2fe443686f6c13D4c76d085",
		AbiFilePath:     "abi.json",
		PollInterval:    2 * time.Second,
	}

	monitor, err := NewMonitor(config)
	if err != nil {
		log.Fatalf("Failed to create monitor: %v", err)
	}

	if err := monitor.Start(); err != nil {
		log.Fatalf("Monitor error: %v", err)
	}
}
