package util

import "os"

// Getenv returns the environment variable value if set, otherwise returns defaultValue.
func Getenv(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
