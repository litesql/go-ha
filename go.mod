module github.com/litesql/go-ha

go 1.25

replace github.com/mattn/go-sqlite3 => github.com/litesql/go-sqlite3 v1.14.33

require (
	github.com/antlr4-go/antlr/v4 v4.13.1
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/mattn/go-sqlite3 v1.14.32
)

require golang.org/x/exp v0.0.0-20250911091902-df9299821621 // indirect
