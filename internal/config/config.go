package config

import (
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	instance *viper.Viper
	once     sync.Once
	mu       sync.RWMutex // Protects concurrent access to config.
)

// RequiredFields lists the mandatory configuration keys.
var RequiredFields = []string{
	"project_id",
	"dataset",
	"table",
	"snowflake_dsn",
	// You can add more required fields (e.g., "service_account", "snowflake_stage") as needed.
}

// LoadConfig initializes and loads configuration from a YAML file.
// The provided configPath (if empty, defaults to "config.yaml") is used.
func LoadConfig(configPath string) (*viper.Viper, error) {
	var err error
	once.Do(func() {
		instance = viper.New()
		if configPath == "" {
			configPath = "config.yaml"
		}
		instance.SetConfigFile(configPath)
		instance.SetConfigType("yaml")

		// Allow environment variable overrides.
		instance.AutomaticEnv()
		instance.SetEnvPrefix("SYNC") // e.g. SYNC_PROJECT_ID

		// Read the configuration file.
		if err = instance.ReadInConfig(); err != nil {
			zap.L().Fatal("Failed to read config file", zap.Error(err))
		}

		// Validate required fields.
		if err = ValidateConfig(instance); err != nil {
			zap.L().Fatal("Configuration validation failed", zap.Error(err))
		}

		// Enable hot-reloading.
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
	return instance, err
}

// ValidateConfig ensures that all required configuration keys are set.
func ValidateConfig(v *viper.Viper) error {
	for _, key := range RequiredFields {
		if !v.IsSet(key) {
			return fmt.Errorf("missing required config key: %s", key)
		}
	}
	return nil
}
