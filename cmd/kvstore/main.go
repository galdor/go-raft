package main

import (
	"github.com/galdor/go-service/pkg/service"
)

func main() {
	service.Run("kvstore", "a simple key-value storage server", NewService())
}
