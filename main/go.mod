module main

go 1.24.4

replace server => ../server
replace migrationManager => ../migrationManager

require (
	migrationManager v0.0.0-00010101000000-000000000000
	server v0.0.0-00010101000000-000000000000
)
