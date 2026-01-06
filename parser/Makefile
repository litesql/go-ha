sqlite_parser.go: SQLiteLexer.g4 SQLiteParser.g4 antlr-4.13.1-complete.jar
	java -jar antlr-4.13.1-complete.jar -Dlanguage=Go -no-visitor -package parser SQLiteLexer.g4 SQLiteParser.g4

antlr-4.13.1-complete.jar:
	curl -O https://www.antlr.org/download/antlr-4.13.1-complete.jar

download:
	curl -O https://raw.githubusercontent.com/antlr/grammars-v4/refs/heads/master/sql/sqlite/SQLiteLexer.g4
	sed -i "s/BIND_PARAMETER: '?' DIGIT\\* | \\[:@\\$$\\] IDENTIFIER;/BIND_PARAMETER: '?' DIGIT\\* | '$$' DIGIT+ | \\[:@\\$$\\] IDENTIFIER;/g" SQLiteLexer.g4
	curl -O https://raw.githubusercontent.com/antlr/grammars-v4/refs/heads/master/sql/sqlite/SQLiteParser.g4
