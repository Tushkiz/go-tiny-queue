package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	ctx := context.Background()
	dsn := getenv("DB_DSN", "app:app@tcp(127.0.0.1:3306)/tiny-queue?parseTime=true&charset=utf8mb4&loc=UTC")

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("failed to get sql db: %v", err)
	}
	defer sqlDB.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		log.Fatalf("database ping failed: %v", err)
	}

	fmt.Println("go-tiny-queue: DB connection OK")

}

func getenv(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
