package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ermes-thesis/ermes-common/caches"
	"gopkg.in/yaml.v3" //go get gopkg.in/yaml.v3
)

// config data structures
type Config struct {
	Node                    NodeConfig               `yaml:"node"`
	Hierarchy               HierarchyConfig          `yaml:"hierarchy"`
	MigrationManager        MigrationManagerConfig   `yaml:"migrationManager"`
	Redis                   RedisConfig              `yaml:"redis"`
	Lru                     LruConfig                `yaml:"lru"`
	FunctionsCache          FunctionsCacheConfig     `yaml:"functionsCache"`
	Scheduler               SchedulerConfig          `yaml:"scheduler"`
	Collections             []CollectionSettingsInfo `yaml:"collections"`
	GroupTokenPublicKeyPath string                   `yaml:"groupTokenPublicKeyPath"`
}

type MigrationManagerConfig struct {
	LeaderThreshold   int     `yaml:"leaderThreshold"`
	FollowerThreshold int     `yaml:"followerThreshold"`
	ViewSize          int     `yaml:"viewSize"`
	IdleTimeoutMs     int     `yaml:"idleTimeoutMs"`
	GravityConstant   float64 `yaml:"gravityConstant"`
}

type LruConfig struct {
	Size      uint    `yaml:"size"`
	SafeLevel float32 `yaml:"safeLevel"`
}

type RedisConfig struct {
	Address  string `yaml:"address"`
	Port     string `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	Protocol int    `yaml:"protocol"`
}

type FunctionsCacheConfig struct {
	BinaryCache      caches.MetadataBinaryCacheConfig `yaml:"binaryCache"`
	TemplateCache    TemplatesCacheConfig             `yaml:"templateCache"`
	FunctionRegistry FunctionRegistryConfig           `yaml:"functionRegistry"`
	LatestCache      caches.LatestCacheConfig         `yaml:"latestCache"`
}

type TemplatesCacheConfig struct {
	Size int           `yaml:"size"`
	Ttl  time.Duration `yaml:"ttl"`
}

type NodeConfig struct {
	Name         string `yaml:"name"`
	Domain       string `yaml:"domain"`
	Address      string `yaml:"address"`
	Port         string `yaml:"port"`
	Secret       string `yaml:"secret"`
	ClientSecret string `yaml:"clientSecret"`
}

type FunctionRegistryConfig struct {
	Address string `yaml:"address"`
	Port    string `yaml:"port"`
}
type SchedulerConfig struct {
	GroupTokenProviderAddress string            `yaml:"groupTokenProviderAddress"`
	GroupTokenProviderPort    string            `yaml:"groupTokenProviderPort"`
	WasmRuntime               ChosenWasmRuntime `yaml:"wasmRuntime"`
}

type HierarchyConfig struct {
	Father   *HierarchyNodeConfig  `yaml:"father,omitempty"`
	Cloud    *HierarchyNodeConfig  `yaml:"cloud,omitempty"`
	Children []HierarchyNodeConfig `yaml:"children"`
}

type HierarchyNodeConfig struct {
	Address   string `yaml:"address"`
	Domain    string `yaml:"domain"`
	LatencyUp int    `yaml:"latency_up"`
}

func LoadConfig(filename string) Config {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Errore nella lettura del file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("Errore nel parsing YAML: %v", err)
	}

	return config
}

type ChosenWasmRuntime int

const (
	Wasmedge ChosenWasmRuntime = iota
	Wasmtime
	Wazero
)

func (m *ChosenWasmRuntime) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToLower(s) {
	case "wasmedge":
		*m = Wasmedge
	case "wasmtime":
		*m = Wasmtime
	case "wazero":
		*m = Wazero
	default:
		return fmt.Errorf("unknown chosen wasm runtime: %s", s)
	}

	return nil
}

func (m ChosenWasmRuntime) String() string {
	switch m {
	case Wasmedge:
		return "wasmedge"
	case Wasmtime:
		return "wasmtime"
	case Wazero:
		return "wazero"
	default:
		return "unknown"
	}
}

type CollectionSettingsInfo struct {
	CollectionID   string   `yaml:"collection_id"`       // mandatory
	IsStrong       bool     `yaml:"is_strong"`           // mandatory
	CollectionSize *uint64  `yaml:"collection_size"`     // mandatory for local collections
	IsLeader       *bool    `yaml:"is_leader,omitempty"` // optional
	IsRemote       bool     `yaml:"is_local"`            // mandatory
	HostsID        []string `yaml:"hosts_id,omitempty"`  // optional
	LeaderID       *string  `yaml:"leader_id,omitempty"` // optional
}
