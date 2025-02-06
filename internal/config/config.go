package config

import (
	"flag"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Singleton instance for Viper
var (
	instance *viper.Viper
	once     sync.Once
	mu       sync.RWMutex // Protects access to config values
)

// RequiredFields ensures mandatory fields exist in the config
var RequiredFields = []string{
	"project_id",
	"dataset",
	"table",
	"snowflake_dsn",
}

// LoadConfig initializes and loads configuration from a YAML file with hot-reloading.
func LoadConfig() (*viper.Viper, error) {
	once.Do(func() {
		instance = viper.New()

		// Read optional config path from CLI
		configPath := flag.String("config", "config.yaml", "Path to the configuration file")
		flag.Parse()

		instance.SetConfigFile(*configPath)
		instance.SetConfigType("yaml")

		// Allow environment variable overrides
		instance.AutomaticEnv()
		instance.SetEnvPrefix("SYNC") // Example: SYNC_PROJECT_ID

		// Read the config file
		if err := instance.ReadInConfig(); err != nil {
			zap.L().Fatal("Failed to read config file", zap.Error(err))
		}

		// Validate required fields
		if err := ValidateConfig(instance); err != nil {
			zap.L().Fatal("Configuration validation failed", zap.Error(err))
		}

		// Enable hot-reloading
		instance.WatchConfig()
		instance.OnConfigChange(func(e fsnotify.Event) {
			zap.L().Info("Config file changed, reloading...", zap.String("file", e.Name))
			mu.Lock()
			defer mu.Unlock()
			if err := instance.ReadInConfig(); err != nil {
				zap.L().Error("Failed to reload config", zap.Error(err))
			} else {
				zap.L().Info("Configuration successfully reloaded.")
			}
		})
	})

	return instance, nil
}

// ValidateConfig ensures required fields are present
func ValidateConfig(v *viper.Viper) error {
	for _, key := range RequiredFields {
		if !v.IsSet(key) {
			return fmt.Errorf("missing required config key: %s", key)
		}
	}
	return nil
}

// GetConfig retrieves the singleton Viper instance.
func GetConfig() (*viper.Viper, error) {
	if instance == nil {
		return LoadConfig()
	}
	return instance, nil
}

// GetConfigValue retrieves a string value from the config.
func GetConfigValue(key string) (string, error) {
	mu.RLock()
	defer mu.RUnlock()

	config, err := GetConfig()
	if err != nil {
		return "", fmt.Errorf("failed to load config: %w", err)
	}
	if !config.IsSet(key) {
		return "", fmt.Errorf("missing config key: %s", key)
	}
	return config.GetString(key), nil
}

// GetConfigValueInt retrieves an integer value from the config.
func GetConfigValueInt(key string) (int, error) {
	mu.RLock()
	defer mu.RUnlock()

	config, err := GetConfig()
	if err != nil {
		return 0, fmt.Errorf("failed to load config: %w", err)
	}
	if !config.IsSet(key) {
		return 0, fmt.Errorf("missing config key: %s", key)
	}
	return config.GetInt(key), nil
}

// GetConfigValueBool retrieves a boolean value from the config.
func GetConfigValueBool(key string) (bool, error) {
	mu.RLock()
	defer mu.RUnlock()

	config, err := GetConfig()
	if err != nil {
		return false, fmt.Errorf("failed to load config: %w", err)
	}
	if !config.IsSet(key) {
		return false, fmt.Errorf("missing config key: %s", key)
	}
	return config.GetBool(key), nil
}

// Config Struct Example (Optional)
type Config struct {
	ProjectID    string `mapstructure:"project_id"`
	Dataset      string `mapstructure:"dataset"`
	Table        string `mapstructure:"table"`
	SnowflakeDSN string `mapstructure:"snowflake_dsn"`
}

// LoadConfigStruct loads configuration into a structured Config struct.
func LoadConfigStruct() (*Config, error) {
	mu.RLock()
	defer mu.RUnlock()

	config, err := GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	var cfg Config
	if err := config.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}
