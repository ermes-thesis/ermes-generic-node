package config

import (
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3" //go get gopkg.in/yaml.v3
)

// config data structures
type Config struct {
	Node             NodeConfig             `yaml:"node"`
	Hierarchy        HierarchyConfig        `yaml:"hierarchy"`
	MigrationManager MigrationManagerConfig `yaml:"migrationManager"`
	Redis            RedisConfig            `yaml:"redis"`
	Lru              LruConfig              `yaml:"lru"`
	FunctionsCache   FunctionsCacheConfig   `yaml:"functionsCache"`
	FunctionRegistry FunctionRegistryConfig `yaml:"functionRegistry"`
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
	BinaryCache   BinaryCacheConfig    `yaml:"binaryCache"`
	TemplateCache TemplatesCacheConfig `yaml:"templateCache"`
}

type BinaryCacheConfig struct {
	Size   int           `yaml:"size"`
	Ttl    time.Duration `yaml:"ttl"`
	BinDir string        `yaml:"binDir"`
}

type TemplatesCacheConfig struct {
	Size int           `yaml:"size"`
	Ttl  time.Duration `yaml:"ttl"`
}

type NodeConfig struct {
	Name   string `yaml:"name"`
	Domain string `yaml:"domain"`
	Port   string `yaml:"port"`
	Secret string `yaml:"secret"`
}

type FunctionRegistryConfig struct {
	Address string `yaml:"address"`
	Port    string `yaml:"port"`
}

// The father should look something like:
//
//	father:
//		fatherName
//		fatherDomain
//		fatherLatency
//
// Children will be:
//
//	children:
//		child1Name
//		child1Domain
//		child1Latency
//		child2Name
//		child2Domain
//		child2Latency
//		...
//
// In case of no children or no father:
//
// children:
//
//	nil
type HierarchyConfig struct {
	Father   []string `yaml:"father"`
	Cloud    []string `yaml:"cloud"`
	Children []string `yaml:"children"`
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
	// Stampa i valori
}
