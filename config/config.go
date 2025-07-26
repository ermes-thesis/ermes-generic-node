package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3" //go get gopkg.in/yaml.v3
)

// config data structures
type Config struct {
	MigrationManager MigrationManagerConfig `yaml:"migrationManager"`
	Redis            RedisConfig            `yaml:"redis"`
}

type MigrationManagerConfig struct {
	LeaderThreshold   int     `yaml:"leaderThreshold"`
	FollowerThreshold int     `yaml:"followerThreshold"`
	ViewSize          int     `yaml:"viewSize"`
	IdleTimeoutMs     int     `yaml:"idleTimeoutMs"`
	GravityConstant   float64 `yaml:"gravityConstant"`
}

type RedisConfig struct {
	Address  string `yaml:"address"`
	Port     string `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	Protocol int    `yaml:"protocol"`
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
