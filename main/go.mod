module main

go 1.24.4

replace server => ../server

replace dataManager => ../dataManager

require (
	dataManager v0.0.0-00010101000000-000000000000
	server v0.0.0-00010101000000-000000000000
)

require gopkg.in/yaml.v3 v3.0.1 // indirect
